package messages

import (
	"context"
	crand "crypto/rand"
	"io"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

// Service owns the lifecycle of stored messages. Its dependencies are
// expressed only through the DAO interface declared in dao.go.
type Service struct {
	dao DAO

	ulidMu      sync.Mutex
	ulidEntropy io.Reader
	now         func() time.Time
}

// NewService constructs a Service backed by the given DAO. The ULID
// entropy source is seeded from crypto/rand and made monotonic so IDs
// generated within the same millisecond preserve order.
func NewService(dao DAO) *Service {
	return &Service{
		dao:         dao,
		ulidEntropy: ulid.Monotonic(crand.Reader, 0),
		now:         func() time.Time { return time.Now().UTC() },
	}
}

// Ingest assigns a server-side ID and timestamp to an inbound message
// envelope, persists the resulting row, and returns it.
func (s *Service) Ingest(ctx context.Context, sender string, in Inbound) (Message, error) {
	m := Message{
		ID:        s.newULID(),
		Sender:    sender,
		Recipient: in.To,
		Body:      in.Body,
		CreatedAt: s.now(),
	}
	if err := s.dao.Insert(ctx, m); err != nil {
		return Message{}, err
	}
	return m, nil
}

func (s *Service) newULID() string {
	s.ulidMu.Lock()
	defer s.ulidMu.Unlock()
	return ulid.MustNew(ulid.Timestamp(time.Now()), s.ulidEntropy).String()
}
