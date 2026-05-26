package connections

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/bszjozwiak/myWhatsApp/server/messages"
	"github.com/gorilla/websocket"
)

const maxConsecutiveBadFrames = 5

// MessageIngester is the cross-domain contract Service needs to
// persist inbound message envelopes. It is satisfied by
// *messages.Service.
type MessageIngester interface {
	Ingest(ctx context.Context, sender string, in messages.Inbound) (messages.Message, error)
}

// Service owns the WebSocket entry point and the inter-replica
// publish step. Its dependencies are expressed only through
// MessageIngester (a peer-domain service) and DAO (the local
// pub/sub data access).
type Service struct {
	messages MessageIngester
	dao      DAO
}

// NewService wires the connections Service.
func NewService(m MessageIngester, dao DAO) *Service {
	return &Service{messages: m, dao: dao}
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// HandleWS is the HTTP handler that upgrades to a WebSocket and runs
// the per-connection ingest loop (spec §4.1, §4.2, §6.2).
func (s *Service) HandleWS(w http.ResponseWriter, r *http.Request) {
	clientID := r.URL.Query().Get("client_id")
	if clientID == "" {
		http.Error(w, "missing client_id", http.StatusBadRequest)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("ws upgrade failed", "client_id", clientID, "err", err)
		return
	}
	defer conn.Close()

	slog.Info("ws connected", "client_id", clientID)

	badFrames := 0
	for {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				slog.Info("ws read ended", "client_id", clientID, "err", err)
			}
			return
		}

		var in messages.Inbound
		if err := json.Unmarshal(payload, &in); err != nil {
			badFrames++
			slog.Warn("malformed frame", "client_id", clientID, "bad_frames", badFrames, "err", err)
			if badFrames >= maxConsecutiveBadFrames {
				writeClose(conn, websocket.CloseUnsupportedData, "too many bad frames")
				return
			}
			continue
		}
		badFrames = 0

		m, err := s.messages.Ingest(r.Context(), clientID, in)
		if err != nil {
			slog.Error("insert message failed", "client_id", clientID, "err", err)
			_ = conn.WriteJSON(map[string]string{"error": "persist_failed"})
			writeClose(conn, websocket.CloseInternalServerErr, "persist failed")
			return
		}
		slog.Info("message ingested", "client_id", clientID, "message_id", m.ID, "to", m.Recipient)

		s.publish(r.Context(), m)
	}
}

func (s *Service) publish(ctx context.Context, m messages.Message) {
	payload, err := json.Marshal(m.AsOutbound())
	if err != nil {
		slog.Error("publish payload marshal failed", "message_id", m.ID, "err", err)
		return
	}
	channel := messageChannel(m.Recipient)
	if err := s.dao.Publish(ctx, channel, payload); err != nil {
		slog.Error("redis publish failed", "message_id", m.ID, "channel", channel, "err", err)
		return
	}
	slog.Info("message published", "message_id", m.ID, "channel", channel)
}

func writeClose(conn *websocket.Conn, code int, reason string) {
	_ = conn.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(code, reason),
		time.Now().Add(time.Second),
	)
}
