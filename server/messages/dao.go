package messages

import (
	"context"
	"database/sql"
)

// DAO is the data-access contract the messages Service depends on.
// Implementations live alongside this interface so that other
// packages depend only on the Service.
type DAO interface {
	Insert(ctx context.Context, m Message) error
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

// PostgresDAO is the PostgreSQL implementation of DAO.
type PostgresDAO struct {
	db *sql.DB
}

// NewPostgresDAO wraps an open *sql.DB as a messages DAO.
func NewPostgresDAO(db *sql.DB) *PostgresDAO {
	return &PostgresDAO{db: db}
}

// Insert persists a message into the `messages` table.
func (p *PostgresDAO) Insert(ctx context.Context, m Message) error {
	_, err := p.db.ExecContext(ctx,
		`INSERT INTO messages (id, sender, recipient, body, created_at) VALUES ($1, $2, $3, $4, $5)`,
		m.ID, m.Sender, m.Recipient, m.Body, m.CreatedAt)
	return err
}

// Bootstrap creates the messages table and pending-queue index if
// they do not already exist (spec §3.3 D9, §5). Safe to call from
// every server replica on every startup.
func (p *PostgresDAO) Bootstrap(ctx context.Context) error {
	_, err := p.db.ExecContext(ctx, schemaSQL)
	return err
}
