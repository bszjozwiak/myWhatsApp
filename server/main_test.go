package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", handleWS)
	return httptest.NewServer(mux)
}

func wsURL(httpURL, path string) string {
	return "ws" + strings.TrimPrefix(httpURL, "http") + path
}

func TestWS_MissingClientIDReturns400(t *testing.T) {
	srv := newTestServer(t)
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

func TestWS_ConnectAndEcho(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()

	conn, resp, err := websocket.DefaultDialer.Dial(wsURL(srv.URL, "/ws?client_id=test"), nil)
	if err != nil {
		t.Fatalf("dial failed: %v (status=%v)", err, resp)
	}
	defer conn.Close()

	want := "hello world"
	if err := conn.WriteMessage(websocket.TextMessage, []byte(want)); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	_, got, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if string(got) != want {
		t.Fatalf("echo = %q, want %q", string(got), want)
	}
}

func TestWS_EmptyClientIDReturns400(t *testing.T) {
	srv := newTestServer(t)
	defer srv.Close()

	_, resp, err := websocket.DefaultDialer.Dial(wsURL(srv.URL, "/ws?client_id="), nil)
	if err == nil {
		t.Fatal("expected dial to fail with empty client_id")
	}
	if resp == nil || resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %v, want 400", resp)
	}
}
