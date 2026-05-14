package main

import (
	"errors"
	"log/slog"
	"net/http"
	"os"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
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

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", handleWS)

	addr := ":" + port
	slog.Info("server listening", "addr", addr)
	if err := http.ListenAndServe(addr, mux); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("server exited", "err", err)
		os.Exit(1)
	}
}
