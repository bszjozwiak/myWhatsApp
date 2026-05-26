package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type fakeStore struct {
	mu       sync.Mutex
	inserted []message
	failWith error
}

func (s *fakeStore) insertMessage(_ context.Context, m message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failWith != nil {
		return s.failWith
	}
	s.inserted = append(s.inserted, m)
	return nil
}

func (s *fakeStore) snapshot() []message {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]message, len(s.inserted))
	copy(out, s.inserted)
	return out
}

type publishedFrame struct {
	channel string
	payload []byte
}

type fakePublisher struct {
	mu       sync.Mutex
	frames   []publishedFrame
	failWith error
}

func (p *fakePublisher) publish(_ context.Context, channel string, payload []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failWith != nil {
		return p.failWith
	}
	cp := make([]byte, len(payload))
	copy(cp, payload)
	p.frames = append(p.frames, publishedFrame{channel: channel, payload: cp})
	return nil
}

func (p *fakePublisher) snapshot() []publishedFrame {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]publishedFrame, len(p.frames))
	copy(out, p.frames)
	return out
}

func newTestServer(t *testing.T, store messageStore, pub messagePublisher) *httptest.Server {
	t.Helper()
	if pub == nil {
		pub = &fakePublisher{}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", wsHandler(store, pub))
	return httptest.NewServer(mux)
}

func wsURL(httpURL, path string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http") + path
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestWS_MissingClientIDReturns400(t *testing.T) {
	srv := newTestServer(t, &fakeStore{}, nil)
	defer srv.Close()

	_, resp, err := websocket.DefaultDialer.Dial(wsURL(srv.URL, "/ws"), nil)
	if err == nil {
		t.Fatal("expected dial to fail without client_id")
	}
	if resp == nil {
		t.Fatal("expected HTTP response, got nil")
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

func TestWS_EmptyClientIDReturns400(t *testing.T) {
	srv := newTestServer(t, &fakeStore{}, nil)
	defer srv.Close()

	_, resp, err := websocket.DefaultDialer.Dial(wsURL(srv.URL, "/ws?client_id="), nil)
	if err == nil {
		t.Fatal("expected dial to fail with empty client_id")
	}
	if resp == nil || resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %v, want 400", resp)
	}
}

func TestWS_ValidMessagePersisted(t *testing.T) {
	store := &fakeStore{}
	srv := newTestServer(t, store, nil)
	defer srv.Close()

	conn, _, err := websocket.DefaultDialer.Dial(wsURL(srv.URL, "/ws?client_id=alice"), nil)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	payload := `{"to":"bob","body":"hi","traceparent":"00-tp-sp-01"}`
	if err := conn.WriteMessage(websocket.TextMessage, []byte(payload)); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	waitFor(t, func() bool { return len(store.snapshot()) == 1 }, 2*time.Second)
	got := store.snapshot()
	if len(got) != 1 {
		t.Fatalf("inserted = %d, want 1", len(got))
	}
	m := got[0]
	if m.Sender != "alice" || m.Recipient != "bob" || m.Body != "hi" {
		t.Fatalf("row = %+v, want sender=alice recipient=bob body=hi", m)
	}
	if len(m.ID) != 26 {
		t.Fatalf("ULID len = %d, want 26 (id=%q)", len(m.ID), m.ID)
	}
	if m.CreatedAt.IsZero() {
		t.Fatalf("CreatedAt is zero, want non-zero timestamp")
	}
}

func TestWS_FiveMalformedFramesCloseWith1003(t *testing.T) {
	srv := newTestServer(t, &fakeStore{}, nil)
	defer srv.Close()

	conn, _, err := websocket.DefaultDialer.Dial(wsURL(srv.URL, "/ws?client_id=alice"), nil)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	for i := 0; i < 5; i++ {
		if err := conn.WriteMessage(websocket.TextMessage, []byte("not-json")); err != nil {
			t.Fatalf("write %d failed: %v", i, err)
		}
	}

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _, err = conn.ReadMessage()
	var ce *websocket.CloseError
	if !errors.As(err, &ce) {
		t.Fatalf("expected CloseError, got %v", err)
	}
	if ce.Code != websocket.CloseUnsupportedData {
		t.Fatalf("close code = %d, want %d (1003)", ce.Code, websocket.CloseUnsupportedData)
	}
}

func TestWS_BadFrameCounterResetsOnValidFrame(t *testing.T) {
	store := &fakeStore{}
	srv := newTestServer(t, store, nil)
	defer srv.Close()

	conn, _, err := websocket.DefaultDialer.Dial(wsURL(srv.URL, "/ws?client_id=alice"), nil)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	// 4 bad, 1 good, 4 bad — counter must reset on the good frame.
	for i := 0; i < 4; i++ {
		if err := conn.WriteMessage(websocket.TextMessage, []byte("nope")); err != nil {
			t.Fatalf("bad write %d: %v", i, err)
		}
	}
	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"to":"bob","body":"hi"}`)); err != nil {
		t.Fatalf("good write: %v", err)
	}
	for i := 0; i < 4; i++ {
		if err := conn.WriteMessage(websocket.TextMessage, []byte("nope")); err != nil {
			t.Fatalf("bad write %d: %v", i, err)
		}
	}

	// Connection must still be alive — push one more good frame and confirm it lands.
	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"to":"bob","body":"hi2"}`)); err != nil {
		t.Fatalf("trailing good write: %v", err)
	}

	waitFor(t, func() bool { return len(store.snapshot()) == 2 }, 2*time.Second)
	if got := len(store.snapshot()); got != 2 {
		t.Fatalf("inserted = %d, want 2 (connection should not have been closed)", got)
	}
}

func TestWS_InsertFailureSendsErrorAndClosesWith1011(t *testing.T) {
	store := &fakeStore{failWith: errors.New("simulated pg failure")}
	srv := newTestServer(t, store, nil)
	defer srv.Close()

	conn, _, err := websocket.DefaultDialer.Dial(wsURL(srv.URL, "/ws?client_id=alice"), nil)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"to":"bob","body":"hi"}`)); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	mt, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("expected error JSON frame before close, got err: %v", err)
	}
	if mt != websocket.TextMessage || !strings.Contains(string(data), "persist_failed") {
		t.Fatalf("error frame = (type=%d, body=%q), want text with persist_failed", mt, string(data))
	}

	_, _, err = conn.ReadMessage()
	var ce *websocket.CloseError
	if !errors.As(err, &ce) {
		t.Fatalf("expected CloseError, got %v", err)
	}
	if ce.Code != websocket.CloseInternalServerErr {
		t.Fatalf("close code = %d, want %d (1011)", ce.Code, websocket.CloseInternalServerErr)
	}
}

func TestWS_PublishesAfterInsert(t *testing.T) {
	store := &fakeStore{}
	pub := &fakePublisher{}
	srv := newTestServer(t, store, pub)
	defer srv.Close()

	conn, _, err := websocket.DefaultDialer.Dial(wsURL(srv.URL, "/ws?client_id=alice"), nil)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"to":"bob","body":"hello bob"}`)); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	waitFor(t, func() bool { return len(pub.snapshot()) == 1 }, 2*time.Second)
	frames := pub.snapshot()
	if len(frames) != 1 {
		t.Fatalf("published = %d, want 1", len(frames))
	}
	f := frames[0]
	if f.channel != "client:bob" {
		t.Fatalf("channel = %q, want client:bob", f.channel)
	}

	var got outboundPayload
	if err := json.Unmarshal(f.payload, &got); err != nil {
		t.Fatalf("unmarshal payload failed: %v (raw=%s)", err, f.payload)
	}
	stored := store.snapshot()
	if len(stored) != 1 {
		t.Fatalf("stored = %d, want 1", len(stored))
	}
	want := stored[0]
	if got.ID != want.ID || got.From != "alice" || got.To != "bob" || got.Body != "hello bob" {
		t.Fatalf("payload = %+v, want id=%s from=alice to=bob body=\"hello bob\"", got, want.ID)
	}
	if !got.CreatedAt.Equal(want.CreatedAt) {
		t.Fatalf("payload CreatedAt = %v, stored CreatedAt = %v", got.CreatedAt, want.CreatedAt)
	}
}

func TestWS_PublishFailureDoesNotCloseConnection(t *testing.T) {
	store := &fakeStore{}
	pub := &fakePublisher{failWith: errors.New("simulated redis failure")}
	srv := newTestServer(t, store, pub)
	defer srv.Close()

	conn, _, err := websocket.DefaultDialer.Dial(wsURL(srv.URL, "/ws?client_id=alice"), nil)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	// First frame: publish will fail, but DB insert succeeds and connection stays open.
	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"to":"bob","body":"one"}`)); err != nil {
		t.Fatalf("write 1 failed: %v", err)
	}
	waitFor(t, func() bool { return len(store.snapshot()) == 1 }, 2*time.Second)
	if n := len(store.snapshot()); n != 1 {
		t.Fatalf("after publish failure, stored = %d, want 1 (insert must not be rolled back)", n)
	}

	// Second frame: connection should still be usable.
	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"to":"bob","body":"two"}`)); err != nil {
		t.Fatalf("write 2 failed (connection was closed): %v", err)
	}
	waitFor(t, func() bool { return len(store.snapshot()) == 2 }, 2*time.Second)
	if n := len(store.snapshot()); n != 2 {
		t.Fatalf("stored = %d, want 2", n)
	}
}

func TestWS_PublishNotCalledOnInsertFailure(t *testing.T) {
	store := &fakeStore{failWith: errors.New("simulated pg failure")}
	pub := &fakePublisher{}
	srv := newTestServer(t, store, pub)
	defer srv.Close()

	conn, _, err := websocket.DefaultDialer.Dial(wsURL(srv.URL, "/ws?client_id=alice"), nil)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"to":"bob","body":"hi"}`)); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	// Drain the error + close frames so we know the handler finished its work.
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			break
		}
	}

	if frames := pub.snapshot(); len(frames) != 0 {
		t.Fatalf("publish called %d times on insert failure, want 0", len(frames))
	}
}
