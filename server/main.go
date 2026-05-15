package main

import (
	"context"
	crand "crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/oklog/ulid/v2"
	"github.com/redis/go-redis/v9"
)

const maxConsecutiveBadFrames = 5

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

const schemaSQL = `
CREATE TABLE IF NOT EXISTS messages (
    id           TEXT        PRIMARY KEY,
    sender       TEXT        NOT NULL,
    recipient    TEXT        NOT NULL,
    body         TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    delivered_at TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS idx_messages_pending
    ON messages (recipient, created_at)
    WHERE delivered_at IS NULL;
`

type message struct {
	ID        string
	Sender    string
	Recipient string
	Body      string
	CreatedAt time.Time
}

type inboundMsg struct {
	To          string `json:"to"`
	Body        string `json:"body"`
	Traceparent string `json:"traceparent"`
}

type outboundPayload struct {
	ID        string    `json:"id"`
	From      string    `json:"from"`
	To        string    `json:"to"`
	Body      string    `json:"body"`
	CreatedAt time.Time `json:"created_at"`
}

type messageStore interface {
	insertMessage(ctx context.Context, m message) error
}

type messagePublisher interface {
	publish(ctx context.Context, channel string, payload []byte) error
}

type pgStore struct {
	db *sql.DB
}

func (s *pgStore) insertMessage(ctx context.Context, m message) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO messages (id, sender, recipient, body, created_at) VALUES ($1, $2, $3, $4, $5)`,
		m.ID, m.Sender, m.Recipient, m.Body, m.CreatedAt)
	return err
}

type redisPublisher struct {
	client *redis.Client
}

func (p *redisPublisher) publish(ctx context.Context, channel string, payload []byte) error {
	return p.client.Publish(ctx, channel, payload).Err()
}

var (
	ulidMu      sync.Mutex
	ulidEntropy = ulid.Monotonic(crand.Reader, 0)
)

func newULID() string {
	ulidMu.Lock()
	defer ulidMu.Unlock()
	return ulid.MustNew(ulid.Timestamp(time.Now()), ulidEntropy).String()
}

func openDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func bootstrapSchema(db *sql.DB) error {
	_, err := db.Exec(schemaSQL)
	return err
}

func openRedis(addr string) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, err
	}
	return client, nil
}

func wsHandler(store messageStore, pub messagePublisher) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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

			var in inboundMsg
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

			m := message{
				ID:        newULID(),
				Sender:    clientID,
				Recipient: in.To,
				Body:      in.Body,
				CreatedAt: time.Now().UTC(),
			}
			if err := store.insertMessage(r.Context(), m); err != nil {
				slog.Error("insert message failed", "client_id", clientID, "err", err)
				_ = conn.WriteJSON(map[string]string{"error": "persist_failed"})
				writeClose(conn, websocket.CloseInternalServerErr, "persist failed")
				return
			}
			slog.Info("message ingested", "client_id", clientID, "message_id", m.ID, "to", m.Recipient)

			publishMessage(r.Context(), pub, m)
		}
	}
}

func publishMessage(ctx context.Context, pub messagePublisher, m message) {
	payload, err := json.Marshal(outboundPayload{
		ID:        m.ID,
		From:      m.Sender,
		To:        m.Recipient,
		Body:      m.Body,
		CreatedAt: m.CreatedAt,
	})
	if err != nil {
		slog.Error("publish payload marshal failed", "message_id", m.ID, "err", err)
		return
	}
	channel := "client:" + m.Recipient
	if err := pub.publish(ctx, channel, payload); err != nil {
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

func main() {
	port := os.Getenv("SERVER_PORT")
	if port == "" {
		port = "8080"
	}

	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		slog.Error("POSTGRES_DSN is required")
		os.Exit(1)
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		slog.Error("REDIS_ADDR is required")
		os.Exit(1)
	}

	db, err := openDB(dsn)
	if err != nil {
		slog.Error("postgres connect failed", "err", err)
		os.Exit(1)
	}
	defer db.Close()

	if err := bootstrapSchema(db); err != nil {
		slog.Error("schema bootstrap failed", "err", err)
		os.Exit(1)
	}
	slog.Info("postgres connected; schema ready")

	rdb, err := openRedis(redisAddr)
	if err != nil {
		slog.Error("redis connect failed", "err", err)
		os.Exit(1)
	}
	defer rdb.Close()
	slog.Info("redis connected", "addr", redisAddr)

	store := &pgStore{db: db}
	pub := &redisPublisher{client: rdb}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", wsHandler(store, pub))

	addr := ":" + port
	slog.Info("server listening", "addr", addr)
	if err := http.ListenAndServe(addr, mux); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("server exited", "err", err)
		os.Exit(1)
	}
}
