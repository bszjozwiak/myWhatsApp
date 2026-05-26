package connections

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bszjozwiak/myWhatsApp/server/messages"
	"github.com/gorilla/websocket"
)

type ingestCall struct {
	Sender string
	In     messages.Inbound
}

type fakeIngester struct {
	mu      sync.Mutex
	calls   []ingestCall
	returns []messages.Message
	err     error
}

func (f *fakeIngester) Ingest(_ context.Context, sender string, in messages.Inbound) (messages.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return messages.Message{}, f.err
	}
	m := messages.Message{
		ID:        fmt.Sprintf("test-id-%d", len(f.calls)+1),
		Sender:    sender,
		Recipient: in.To,
		Body:      in.Body,
		CreatedAt: time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC).Add(time.Duration(len(f.calls)) * time.Second),
	}
	f.calls = append(f.calls, ingestCall{Sender: sender, In: in})
	f.returns = append(f.returns, m)
	return m, nil
}

func (f *fakeIngester) snapshotCalls() []ingestCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]ingestCall, len(f.calls))
	copy(out, f.calls)
	return out
}

func (f *fakeIngester) snapshotReturns() []messages.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]messages.Message, len(f.returns))
	copy(out, f.returns)
	return out
}

type publishedFrame struct {
	Channel string
	Payload []byte
}

type fakeDAO struct {
	mu     sync.Mutex
	frames []publishedFrame
	err    error
}

func (p *fakeDAO) Publish(_ context.Context, channel string, payload []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	cp := make([]byte, len(payload))
	copy(cp, payload)
	p.frames = append(p.frames, publishedFrame{Channel: channel, Payload: cp})
	return nil
}

func (p *fakeDAO) snapshot() []publishedFrame {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]publishedFrame, len(p.frames))
	copy(out, p.frames)
	return out
}

func newTestServer(t *testing.T, ing MessageIngester, dao DAO) *httptest.Server {
	t.Helper()
	if dao == nil {
		dao = &fakeDAO{}
	}
	svc := NewService(ing, dao)
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", svc.HandleWS)
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

func TestHandleWS_MissingClientIDReturns400(t *testing.T) {
	srv := newTestServer(t, &fakeIngester{}, nil)
	defer srv.Close()

	_, resp, err := websocket.DefaultDialer.Dial(wsURL(srv.URL, "/ws"), nil)
	if err == nil {
		t.Fatal("expected dial to fail without client_id")
	}
	if resp == nil || resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %v, want 400", resp)
	}
}

func TestHandleWS_EmptyClientIDReturns400(t *testing.T) {
	srv := newTestServer(t, &fakeIngester{}, nil)
	defer srv.Close()

	_, resp, err := websocket.DefaultDialer.Dial(wsURL(srv.URL, "/ws?client_id="), nil)
	if err == nil {
		t.Fatal("expected dial to fail with empty client_id")
	}
	if resp == nil || resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %v, want 400", resp)
	}
}

func TestHandleWS_ValidFrameDispatchedToIngester(t *testing.T) {
	ing := &fakeIngester{}
	srv := newTestServer(t, ing, nil)
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

	waitFor(t, func() bool { return len(ing.snapshotCalls()) == 1 }, 2*time.Second)
	calls := ing.snapshotCalls()
	if len(calls) != 1 {
		t.Fatalf("ingest calls = %d, want 1", len(calls))
	}
	c := calls[0]
	if c.Sender != "alice" {
		t.Fatalf("sender = %q, want alice", c.Sender)
	}
	want := messages.Inbound{To: "bob", Body: "hi", Traceparent: "00-tp-sp-01"}
	if c.In != want {
		t.Fatalf("inbound = %+v, want %+v", c.In, want)
	}
}

func TestHandleWS_FiveMalformedFramesCloseWith1003(t *testing.T) {
	srv := newTestServer(t, &fakeIngester{}, nil)
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

func TestHandleWS_BadFrameCounterResetsOnValidFrame(t *testing.T) {
	ing := &fakeIngester{}
	srv := newTestServer(t, ing, nil)
	defer srv.Close()

	conn, _, err := websocket.DefaultDialer.Dial(wsURL(srv.URL, "/ws?client_id=alice"), nil)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

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
	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"to":"bob","body":"hi2"}`)); err != nil {
		t.Fatalf("trailing good write: %v", err)
	}

	waitFor(t, func() bool { return len(ing.snapshotCalls()) == 2 }, 2*time.Second)
	if got := len(ing.snapshotCalls()); got != 2 {
		t.Fatalf("ingest calls = %d, want 2 (connection should not have been closed)", got)
	}
}

func TestHandleWS_IngestErrorSendsErrorAndClosesWith1011(t *testing.T) {
	ing := &fakeIngester{err: errors.New("simulated pg failure")}
	srv := newTestServer(t, ing, nil)
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

func TestHandleWS_PublishesAfterIngest(t *testing.T) {
	ing := &fakeIngester{}
	dao := &fakeDAO{}
	srv := newTestServer(t, ing, dao)
	defer srv.Close()

	conn, _, err := websocket.DefaultDialer.Dial(wsURL(srv.URL, "/ws?client_id=alice"), nil)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"to":"bob","body":"hello bob"}`)); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	waitFor(t, func() bool { return len(dao.snapshot()) == 1 }, 2*time.Second)
	frames := dao.snapshot()
	if len(frames) != 1 {
		t.Fatalf("published = %d, want 1", len(frames))
	}
	f := frames[0]
	if f.Channel != "client:bob" {
		t.Fatalf("channel = %q, want client:bob", f.Channel)
	}

	var got messages.Outbound
	if err := json.Unmarshal(f.Payload, &got); err != nil {
		t.Fatalf("unmarshal payload failed: %v (raw=%s)", err, f.Payload)
	}
	returns := ing.snapshotReturns()
	if len(returns) != 1 {
		t.Fatalf("ingester returns = %d, want 1", len(returns))
	}
	want := returns[0]
	if got.ID != want.ID || got.From != "alice" || got.To != "bob" || got.Body != "hello bob" {
		t.Fatalf("payload = %+v, want id=%s from=alice to=bob body=\"hello bob\"", got, want.ID)
	}
	if !got.CreatedAt.Equal(want.CreatedAt) {
		t.Fatalf("payload CreatedAt = %v, ingested CreatedAt = %v", got.CreatedAt, want.CreatedAt)
	}
}

func TestHandleWS_PublishFailureDoesNotCloseConnection(t *testing.T) {
	ing := &fakeIngester{}
	dao := &fakeDAO{err: errors.New("simulated redis failure")}
	srv := newTestServer(t, ing, dao)
	defer srv.Close()

	conn, _, err := websocket.DefaultDialer.Dial(wsURL(srv.URL, "/ws?client_id=alice"), nil)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"to":"bob","body":"one"}`)); err != nil {
		t.Fatalf("write 1 failed: %v", err)
	}
	waitFor(t, func() bool { return len(ing.snapshotCalls()) == 1 }, 2*time.Second)
	if n := len(ing.snapshotCalls()); n != 1 {
		t.Fatalf("after publish failure, ingest calls = %d, want 1 (insert must not be rolled back)", n)
	}

	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"to":"bob","body":"two"}`)); err != nil {
		t.Fatalf("write 2 failed (connection was closed): %v", err)
	}
	waitFor(t, func() bool { return len(ing.snapshotCalls()) == 2 }, 2*time.Second)
	if n := len(ing.snapshotCalls()); n != 2 {
		t.Fatalf("ingest calls = %d, want 2", n)
	}
}

func TestHandleWS_PublishNotCalledOnIngestFailure(t *testing.T) {
	ing := &fakeIngester{err: errors.New("simulated pg failure")}
	dao := &fakeDAO{}
	srv := newTestServer(t, ing, dao)
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
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			break
		}
	}

	if frames := dao.snapshot(); len(frames) != 0 {
		t.Fatalf("publish called %d times on ingest failure, want 0", len(frames))
	}
}
