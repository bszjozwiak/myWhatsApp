package messages

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeDAO struct {
	mu       sync.Mutex
	inserted []Message
	err      error
}

func (f *fakeDAO) Insert(_ context.Context, m Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.inserted = append(f.inserted, m)
	return nil
}

func (f *fakeDAO) snapshot() []Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Message, len(f.inserted))
	copy(out, f.inserted)
	return out
}

func TestService_Ingest_PersistsAndReturnsMessage(t *testing.T) {
	dao := &fakeDAO{}
	svc := NewService(dao)

	got, err := svc.Ingest(context.Background(), "alice", Inbound{
		To:   "bob",
		Body: "hi",
	})
	if err != nil {
		t.Fatalf("Ingest returned err: %v", err)
	}

	if got.Sender != "alice" || got.Recipient != "bob" || got.Body != "hi" {
		t.Fatalf("returned message = %+v, want sender=alice recipient=bob body=hi", got)
	}
	if len(got.ID) != 26 {
		t.Fatalf("ULID len = %d, want 26 (id=%q)", len(got.ID), got.ID)
	}
	if got.CreatedAt.IsZero() {
		t.Fatalf("CreatedAt is zero")
	}

	inserted := dao.snapshot()
	if len(inserted) != 1 {
		t.Fatalf("DAO inserts = %d, want 1", len(inserted))
	}
	if inserted[0] != got {
		t.Fatalf("inserted row = %+v, returned = %+v (must match)", inserted[0], got)
	}
}

func TestService_Ingest_GeneratesUniqueULIDsAcrossCalls(t *testing.T) {
	dao := &fakeDAO{}
	svc := NewService(dao)

	const n = 50
	seen := make(map[string]struct{}, n)
	for i := 0; i < n; i++ {
		m, err := svc.Ingest(context.Background(), "alice", Inbound{To: "bob", Body: "x"})
		if err != nil {
			t.Fatalf("Ingest[%d] err: %v", i, err)
		}
		if _, dup := seen[m.ID]; dup {
			t.Fatalf("duplicate ULID %q on iteration %d", m.ID, i)
		}
		seen[m.ID] = struct{}{}
	}
}

func TestService_Ingest_PropagatesDAOError(t *testing.T) {
	want := errors.New("simulated pg failure")
	svc := NewService(&fakeDAO{err: want})

	_, err := svc.Ingest(context.Background(), "alice", Inbound{To: "bob", Body: "hi"})
	if !errors.Is(err, want) {
		t.Fatalf("Ingest err = %v, want %v", err, want)
	}
}

func TestMessage_AsOutbound_MapsFields(t *testing.T) {
	t0 := time.Date(2026, 5, 14, 12, 34, 56, 0, time.UTC)
	m := Message{ID: "abc", Sender: "a", Recipient: "b", Body: "hi", CreatedAt: t0}
	out := m.AsOutbound()
	if out.ID != "abc" || out.From != "a" || out.To != "b" || out.Body != "hi" || !out.CreatedAt.Equal(t0) {
		t.Fatalf("AsOutbound = %+v", out)
	}
}
