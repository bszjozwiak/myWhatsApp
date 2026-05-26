package main

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/bszjozwiak/myWhatsApp/server/connections"
	"github.com/bszjozwiak/myWhatsApp/server/messages"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
)

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

	msgDAO := messages.NewPostgresDAO(db)
	if err := msgDAO.Bootstrap(context.Background()); err != nil {
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

	msgService := messages.NewService(msgDAO)
	connDAO := connections.NewRedisDAO(rdb)
	connService := connections.NewService(msgService, connDAO)

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", connService.HandleWS)

	addr := ":" + port
	slog.Info("server listening", "addr", addr)
	if err := http.ListenAndServe(addr, mux); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("server exited", "err", err)
		os.Exit(1)
	}
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
