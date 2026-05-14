package main

import (
	"context"
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

func newTestServer(t *testing.T, store messageStore) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", wsHandler(store))
	return httptest.NewServer(mux)
}

func wsURL(httpURL, path string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http") + path
}

func TestWS_MissingClientIDReturns400(t *testing.T) {
	srv := newTestServer(t, &fakeStore{})
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
	srv := newTestServer(t, &fakeStore{})
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
	srv := newTestServer(t, store)
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

	deadline := time.Now().Add(2 * time.Second)
	var got []message
	for time.Now().Before(deadline) {
		got = store.snapshot()
		if len(got) == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
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
}

func TestWS_FiveMalformedFramesCloseWith1003(t *testing.T) {
	srv := newTestServer(t, &fakeStore{})
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
	srv := newTestServer(t, store)
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

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(store.snapshot()) == 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := len(store.snapshot()); got != 2 {
		t.Fatalf("inserted = %d, want 2 (connection should not have been closed)", got)
	}
}

func TestWS_InsertFailureSendsErrorAndClosesWith1011(t *testing.T) {
	store := &fakeStore{failWith: errors.New("simulated pg failure")}
	srv := newTestServer(t, store)
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
