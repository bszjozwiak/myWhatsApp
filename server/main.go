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
}

type inboundMsg struct {
	To          string `json:"to"`
	Body        string `json:"body"`
	Traceparent string `json:"traceparent"`
}

type messageStore interface {
	insertMessage(ctx context.Context, m message) error
}

type pgStore struct {
	db *sql.DB
}

func (s *pgStore) insertMessage(ctx context.Context, m message) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO messages (id, sender, recipient, body) VALUES ($1, $2, $3, $4)`,
		m.ID, m.Sender, m.Recipient, m.Body)
	return err
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

func wsHandler(store messageStore) http.HandlerFunc {
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
			}
			if err := store.insertMessage(r.Context(), m); err != nil {
				slog.Error("insert message failed", "client_id", clientID, "err", err)
				_ = conn.WriteJSON(map[string]string{"error": "persist_failed"})
				writeClose(conn, websocket.CloseInternalServerErr, "persist failed")
				return
			}
			slog.Info("message ingested", "client_id", clientID, "message_id", m.ID, "to", m.Recipient)
		}
	}
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

	store := &pgStore{db: db}

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", wsHandler(store))

	addr := ":" + port
	slog.Info("server listening", "addr", addr)
	if err := http.ListenAndServe(addr, mux); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("server exited", "err", err)
		os.Exit(1)
	}
}
