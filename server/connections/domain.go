// Package connections owns the WebSocket session lifecycle and the
// Pub/Sub fan-out used to route messages between server replicas
// (spec §3.1 last bullet, §4.3, §6.3).
//
// T2.5 only restructures the file layout — the in-memory connection
// registry that maps client IDs to *websocket.Conn arrives in T2.6
// (spec §6.3, §6.5). For now this file declares only the placeholder
// Connection type so the package keeps the project's required
// domain.go / service.go / dao.go layout.
package connections

// Connection represents an active WebSocket session in the in-memory
// registry.
//
// TODO(T2.6): expand to carry the *websocket.Conn, write mutex,
// cancel func, and Redis subscription handles needed by §6.3/§6.5.
type Connection struct {
	ClientID string
}

// messageChannel is the Redis Pub/Sub channel used to deliver messages
// to the WebSocket of the given client (spec §6.3).
func messageChannel(clientID string) string {
	return "client:" + clientID
}
