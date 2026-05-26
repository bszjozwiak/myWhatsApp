package messages

import "time"

// Message is the persisted record of a chat message accepted by the
// server. It corresponds 1:1 with a row in the PostgreSQL `messages`
// table (spec §5).
type Message struct {
	ID        string
	Sender    string
	Recipient string
	Body      string
	CreatedAt time.Time
}

// Inbound is the JSON envelope sent by a client over the WebSocket
// when emitting a new message (spec §4.2, outbound direction).
type Inbound struct {
	To          string `json:"to"`
	Body        string `json:"body"`
	Traceparent string `json:"traceparent"`
}

// Outbound is the JSON envelope the server publishes (via Redis) and
// forwards to recipient clients (spec §4.2, inbound direction). The
// `traceparent` field defined in the spec is not yet emitted — it
// arrives with the trace-propagation work in T5.9.
type Outbound struct {
	ID        string    `json:"id"`
	From      string    `json:"from"`
	To        string    `json:"to"`
	Body      string    `json:"body"`
	CreatedAt time.Time `json:"created_at"`
}

// AsOutbound returns the wire-format envelope used when forwarding a
// stored Message to a recipient client.
func (m Message) AsOutbound() Outbound {
	return Outbound{
		ID:        m.ID,
		From:      m.Sender,
		To:        m.Recipient,
		Body:      m.Body,
		CreatedAt: m.CreatedAt,
	}
}
