package main

import (
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"os"

	"github.com/gorilla/websocket"
	_ "github.com/jackc/pgx/v5/stdlib"
)

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

func handleWS(w http.ResponseWriter, r *http.Request) {
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

	for {
		msgType, payload, err := conn.ReadMessage()
		if err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				slog.Info("ws read ended", "client_id", clientID, "err", err)
			}
			return
		}
		if err := conn.WriteMessage(msgType, payload); err != nil {
			slog.Info("ws write failed", "client_id", clientID, "err", err)
			return
		}
	}
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

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", handleWS)

	addr := ":" + port
	slog.Info("server listening", "addr", addr)
	if err := http.ListenAndServe(addr, mux); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("server exited", "err", err)
		os.Exit(1)
	}
}
