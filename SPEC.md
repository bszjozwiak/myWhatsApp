# myWhatsApp — Specification

**Status:** Draft v0.3
**Last updated:** 2026-05-14

---

## 1. Overview

myWhatsApp is a WebSocket-based direct messaging system used as a testbed for
observability instrumentation (OpenTelemetry + Grafana stack). The system
consists of horizontally scalable Go server instances that route JSON messages
between connected clients, persist them in PostgreSQL, and bridge cross-instance
delivery through Redis Pub/Sub. Client applications connect with an identifier
derived from their pod hostname, emit hardcoded messages on a configurable
cadence in a **ring topology** (each client sends to the next: 0→1, 1→2, …,
N-1→0), and disconnect after a configured message count.

The primary purpose of the system is **not** to be a feature-rich chat app, but
to generate realistic distributed-system traffic (traces, metrics, logs) for
observability experimentation.

### Non-goals (explicit)

- No TLS — WebSocket is plain `ws://`. Cluster-internal only.
- No authentication or authorization — any `client_id` is accepted.
- No multi-device support per client identity.
- No message reconnect/retry logic in the client (v1).

---

## 2. Architecture

```
                     ┌──────────────────────┐
                     │   Grafana            │
                     │   (Dashboards/UI)    │
                     └──────────┬───────────┘
                                │
              ┌─────────────────┼─────────────────┐
              │                 │                 │
          ┌───▼────┐       ┌────▼───┐        ┌────▼───┐
          │ Tempo  │       │  Loki  │        │ Prom.  │
          │(traces)│       │ (logs) │        │(metrics)│
          └───▲────┘       └────▲───┘        └────▲───┘
              │                 │                 │
              └─────────────────┼─────────────────┘
                                │ OTLP
                     ┌──────────┴───────────┐
                     │ OpenTelemetry        │
                     │ Collector            │
                     └──────────▲───────────┘
                                │ OTLP gRPC
              ┌─────────────────┼─────────────────┐
              │                 │                 │
        ┌─────▼─────┐     ┌─────▼─────┐     ┌─────▼─────┐
        │ server-0  │     │ server-1  │ ... │ server-N  │
        └─┬───────┬─┘     └─┬───────┬─┘     └─┬───────┬─┘
          │       │         │       │         │       │
          │       └─────┐   │       │   ┌─────┘       │
          │             │   │       │   │             │
          │       ┌─────▼───▼───────▼───▼─────┐       │
          │       │   Redis (Pub/Sub bus)     │       │
          │       └───────────────────────────┘       │
          │                                           │
          │       ┌───────────────────────────┐       │
          └──────►│   PostgreSQL (StatefulSet)│◄──────┘
                  └───────────────────────────┘
                                ▲
                                │ WebSocket
              ┌─────────────────┼─────────────────┐
              │                 │                 │
        ┌─────▼─────┐     ┌─────▼─────┐     ┌─────▼─────┐
        │ client-1  │     │ client-2  │ ... │ client-N  │
        └───────────┘     └───────────┘     └───────────┘
```

---

## 3. Components

### 3.1 Server Application (`server/`)

- Written in Go.
- Uses `github.com/gorilla/websocket` for the WebSocket protocol.
- Exposes a single WebSocket endpoint (see §4.1).
- Stateless with respect to other server replicas — all cross-replica
  coordination flows through Redis Pub/Sub.
- Connects to PostgreSQL to persist every message it accepts.
- Subscribes to a Redis channel for each connected client to receive messages
  from peer server instances.
- Deployed as a Kubernetes `Deployment` (scalable).

### 3.2 Client Application (`client/`)

- Written in Go.
- Uses `github.com/gorilla/websocket` for the WebSocket client.
- Derives its `CLIENT_ID` from the pod hostname (e.g., `client-0`,
  `client-1`, …). This is automatic — no env var required.
- Sends hardcoded message bodies (`"Hello from Client No. X"`) on a
  configured interval to a single recipient determined by the **ring
  topology**: pod with ordinal `i` sends to ordinal `(i + 1) mod
  TOTAL_CLIENTS`. The recipient `client_id` is constructed as
  `client-<(i+1) mod TOTAL_CLIENTS>`.
- After emitting the configured maximum number of messages, terminates the
  WebSocket cleanly (close code 1000) and exits with status 0.
- Receives and logs any inbound messages (from peers or queued/offline
  delivery).
- Deployed as a Kubernetes `StatefulSet`. Scaling the StatefulSet's
  `replicas` field changes the number of clients in the ring.
  `TOTAL_CLIENTS` must be kept in sync with the StatefulSet `replicas`
  (see §7.2).

### 3.3 PostgreSQL

- Deployed as a Kubernetes `StatefulSet` with a single replica, backed by a
  `PersistentVolumeClaim`.
- Stores all messages — both delivered and pending (see §5).
- Schema is created **in-process at server startup** by issuing
  `CREATE TABLE IF NOT EXISTS` / `CREATE INDEX IF NOT EXISTS` statements
  on every boot. Idempotent and safe with multiple server replicas
  starting simultaneously.

### 3.4 Redis

- Deployed as a Kubernetes `Deployment` (single replica is sufficient for the
  testbed; not high-availability).
- Used exclusively as a Pub/Sub message bus between server replicas. It is
  **not** used as a message store — PostgreSQL is the source of truth.

---

## 4. Interfaces

### 4.1 WebSocket Endpoint

- **URL:** `ws://<server-service>/ws?client_id=<identifier>`
- **Method:** HTTP Upgrade to WebSocket
- **Required query parameters:**
  - `client_id` — non-empty string, unique per concurrently-connected client.

**Connection rejection rules:**

- Missing or empty `client_id` → HTTP 400, no upgrade.
- `client_id` already connected on any server replica → the new connection
  is accepted immediately; the existing connection is closed by its owning
  replica with WebSocket close code 4000 (`"replaced by newer connection"`).
  The takeover is coordinated by publishing a `kick` control message on a
  dedicated Redis channel `client:<id>:control`. The replica owning the
  existing connection subscribes to this channel for its locally connected
  clients and closes the WebSocket on receipt.

  **Race window:** between the new replica accepting the connection and
  the old replica receiving the kick, both connections briefly exist. A
  message arriving in this window may be written to the old socket. The
  message remains persisted in PostgreSQL with `delivered_at` set, so it
  will not be re-delivered. This is accepted as a known minor duplicate-
  delivery risk in a testbed context (see Decision D8).

### 4.2 Message Formats

All messages on the WebSocket are UTF-8 JSON. There are two message types:

**Outbound (client → server):**

```json
{
  "to":          "Client-2",
  "body":        "Hello from Client No. 1",
  "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
}
```

- `to` (string, required) — recipient `client_id`.
- `body` (string, required) — message body.
- `traceparent` (string, required) — W3C Trace Context header value
  identifying the sender's active `message.send` span. The server starts
  a new `message.receive` span linked to this context (see §8.1).
- The `from` field is **not** sent by the client; the server derives it from
  the connection's authenticated `client_id`.

**Inbound (server → client):**

```json
{
  "id":          "01HXYZ...",
  "from":        "Client-1",
  "to":          "Client-2",
  "body":        "Hello from Client No. 1",
  "created_at":  "2026-05-14T12:34:56.789Z",
  "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-b9c7c989f97918e1-01"
}
```

- `id` (string) — server-assigned message identifier (ULID).
- `from`, `to`, `body` — as above.
- `created_at` (RFC 3339 timestamp) — when the server accepted the message
  from the sender.
- `traceparent` (string) — the server's `message.deliver` span context.
  The recipient client uses this to start its `message.receive` span linked
  to the delivery span (continuing the end-to-end trace).

### 4.3 Connection Lifecycle

1. Client opens `ws://server/ws?client_id=X`.
2. Server validates `client_id` and accepts the upgrade.
3. Server subscribes to Redis channel `client:X` and to
   `client:X:control`. Messages received on `client:X` during steps 4–5
   are **buffered in memory** (not yet written to the WebSocket).
4. Server queries PostgreSQL for messages where `recipient = X AND
   delivered_at IS NULL` and writes them to the WebSocket in `created_at`
   ascending order, marking each `delivered_at` immediately after its
   successful write (see §6.4).
5. After the pending-queue flush completes, the server drains the in-memory
   Redis buffer to the WebSocket (also in arrival order), then transitions
   to steady-state where Redis messages are forwarded directly.
6. Client and server exchange messages until either side closes.
7. On close, server unsubscribes from both channels and releases
   connection state.

This buffer-then-drain protocol guarantees in-order delivery from the
client's perspective: pending (oldest) messages first, then live messages
in the order Redis delivered them.

WebSocket keep-alive uses the gorilla/websocket ping/pong handlers:

- Server sends a ping every 30s; expects pong within 10s.
- Client responds to pings automatically.

---

## 5. Data Model

PostgreSQL schema:

```sql
CREATE TABLE messages (
    id           TEXT        PRIMARY KEY,            -- ULID
    sender       TEXT        NOT NULL,
    recipient    TEXT        NOT NULL,
    body         TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    delivered_at TIMESTAMPTZ NULL
);

CREATE INDEX idx_messages_pending
    ON messages (recipient, created_at)
    WHERE delivered_at IS NULL;
```

- `delivered_at IS NULL` means the message has not yet been pushed to the
  recipient's WebSocket.
- The partial index supports efficient retrieval of a client's pending
  message queue on reconnect.

---

## 6. Behavior Specifications

### 6.1 Client connects

```
client                        server                       postgres   redis
  │                             │                             │         │
  ├── WS upgrade ?client_id=X ─►│                             │         │
  │◄────── 101 Switching ───────┤                             │         │
  │                             ├── SUBSCRIBE client:X ───────┼────────►│
  │                             ├── SUBSCRIBE client:X:ctrl ──┼────────►│
  │                             ├── SELECT pending msgs ─────►│         │
  │                             │◄─────── rows ───────────────┤         │
  │◄── inbound msg #1 (pending)─┤                             │         │
  │                             ├── UPDATE delivered_at #1 ──►│         │
  │◄── inbound msg #2 (pending)─┤                             │         │
  │                             ├── UPDATE delivered_at #2 ──►│         │
  │                             │  (drain buffered Redis msgs)│         │
  │◄── inbound msg #3 (live) ───┤                             │         │
  │                             ├── UPDATE delivered_at #3 ──►│         │
```

### 6.2 Client sends a message

```
client                        server                       postgres   redis
  │── {"to":"Y","body":"..."} ─►│                             │         │
  │                             ├── INSERT message ──────────►│         │
  │                             ├── PUBLISH client:Y {msg} ───┼────────►│
```

- The server **always** persists before publishing.
- The server **always** publishes to Redis `client:Y`, regardless of whether
  the recipient is connected to the same replica. This keeps the routing
  path uniform: every delivered message produces the same span structure
  (`message.receive → redis.publish → message.deliver`), which makes
  observability data consistent and removes special-case code.
- If `to` is not connected to any server, no replica receives the publish;
  the message stays in PostgreSQL with `delivered_at = NULL` and is
  delivered when the recipient next connects (§6.4).

### 6.3 Cross-server routing

- Each server replica maintains an in-memory map of `client_id → *websocket.Conn`
  for clients connected **to that replica**.
- For each locally connected client `X`, the replica subscribes to the Redis
  channel `client:X`.
- When any server publishes to `client:X`, only the replica currently holding
  `X`'s WebSocket will receive and forward the message.
- When the message is successfully written to the WebSocket, the server
  updates `delivered_at` in PostgreSQL.

### 6.4 Offline message delivery

When a client connects, the server retrieves all pending messages (§6.1 step
4) and pushes them in `created_at` ascending order. `delivered_at` is set
**after** a successful WebSocket write for each message, individually, to
avoid marking messages delivered that were lost due to a mid-flight
disconnect.

### 6.5 Client disconnects

- When the client emits a clean WebSocket close, the server unsubscribes from
  `client:X` and `client:X:control` and removes the connection from its
  in-memory map.
- On an unclean close (read error, ping timeout, write timeout), the same
  cleanup occurs.
- No state is written to PostgreSQL on disconnect — message state is already
  captured per-message via `delivered_at`.

### 6.6 Server graceful shutdown

On `SIGTERM` (sent by Kubernetes during pod termination):

- The server stops accepting new WebSocket upgrades (returns HTTP 503).
- For every active WebSocket connection, the server sends a close frame
  with code 1001 (`"going away"`) and waits up to 10s for the client's
  close acknowledgment before forcing the TCP socket closed.
- Open spans are ended and flushed to the Collector before exit.
- Total shutdown deadline: 30s (must fit within
  `terminationGracePeriodSeconds` in the Deployment manifest).

---

## 7. Configuration

### 7.1 Server environment variables

| Variable                  | Required | Default            | Description                                            |
|---------------------------|----------|--------------------|--------------------------------------------------------|
| `SERVER_PORT`             | no       | `8080`             | HTTP/WebSocket listen port                             |
| `POSTGRES_DSN`            | yes      | —                  | PostgreSQL connection string                           |
| `REDIS_ADDR`              | yes      | —                  | Redis address (`host:port`)                            |
| `SERVER_REPLICA_ID`       | no       | from `HOSTNAME`    | Replica identity used as `server.replica` attribute    |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | yes  | —                  | OTLP gRPC endpoint of the Collector                    |
| `OTEL_SERVICE_NAME`       | no       | `mywhatsapp-server`| Service name attribute                                 |
| `LOG_LEVEL`               | no       | `info`             | `debug`/`info`/`warn`/`error`                          |

### 7.2 Client environment variables

| Variable                  | Required | Default            | Description                                            |
|---------------------------|----------|--------------------|--------------------------------------------------------|
| `SERVER_URL`              | yes      | —                  | `ws://server-service:8080/ws`                          |
| `TOTAL_CLIENTS`           | yes      | —                  | Total clients in the ring; **must** equal the StatefulSet `replicas` value |
| `MESSAGE_INTERVAL_MS`     | no       | `1000`             | Interval between messages, ms                          |
| `MESSAGE_LIMIT`           | yes      | —                  | Total messages to send before exit                     |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | yes  | —                  | OTLP gRPC endpoint of the Collector                    |
| `OTEL_SERVICE_NAME`       | no       | `mywhatsapp-client`| Service name attribute                                 |
| `LOG_LEVEL`               | no       | `info`             | `debug`/`info`/`warn`/`error`                          |

The client's identity and recipient are **not** configurable via env vars —
they are derived deterministically from the pod hostname:

- `CLIENT_ID` = `HOSTNAME` (e.g., `client-0`).
- Ordinal `i` = the integer suffix of the hostname after the final `-`.
- Recipient `client_id` = `client-<(i + 1) mod TOTAL_CLIENTS>`.

Sending pattern: at every `MESSAGE_INTERVAL_MS` tick, send one message to
the computed recipient. When the total messages sent equals
`MESSAGE_LIMIT`, the client closes the WebSocket (code 1000) and exits 0.

**Edge case:** when `TOTAL_CLIENTS = 1`, the single client sends to itself
(`client-0 → client-0`). This is harmless and produces a valid self-loop
trace — useful for smoke-testing instrumentation with a minimal deployment.

---

## 8. Observability

The canonical OpenTelemetry deployment for Kubernetes is:

- Applications export OTLP (gRPC, port 4317) to an in-cluster **OpenTelemetry
  Collector** (Deployment, not sidecar).
- The Collector fans out to backend-specific exporters: Tempo (traces), Loki
  (logs via OTLP→Loki), Prometheus (metrics via `prometheusremotewrite` or
  Prometheus scrape of the Collector).
- Grafana datasources point at Tempo, Loki, Prometheus.

### 8.1 Traces

Server spans (minimum):

- `ws.connection` — root span per WebSocket lifetime; attributes:
  `client.id`, `server.replica`. Note that this span may be long-lived
  (minutes to hours); the trade-off is accepted because it provides a
  natural parent for all per-message spans of a session.
- `message.receive` — child of `ws.connection`, one per inbound message;
  attributes: `message.id`, `message.from`, `message.to`.
- `db.insert_message` — child of `message.receive`.
- `redis.publish` — child of `message.receive`.
- `message.deliver` — span per outbound write to a WebSocket; attributes:
  `message.id`, `delivery.path` (`redis` | `pending_queue`). There is no
  `direct` value because every live delivery flows through Redis (D1).
- `db.mark_delivered` — child of `message.deliver`.
- `db.fetch_pending` — span per connection, on connect.

Client spans (minimum):

- `client.run` — root span for the client lifetime.
- `message.send` — child of `client.run`, one per outbound message;
  attributes: `message.to`, `message.sequence`.
- `message.receive` — span per inbound message.

Trace propagation: the W3C `traceparent` header is **not** carried over
WebSocket frames natively, so it is embedded in the JSON message envelope
(see §4.2). The chain is:

- Client builds a `message.send` span, serializes its context into the
  outbound JSON's `traceparent` field.
- Server extracts the context and starts `message.receive` as a child
  (same trace).
- Server's `message.deliver` span context is serialized into the inbound
  JSON's `traceparent` field.
- Recipient client extracts it and starts its own `message.receive` span
  as a child.

The result is a single end-to-end trace spanning sender → server →
recipient.

### 8.2 Metrics

Server metrics:

- `mywhatsapp_ws_connections_active` (gauge)
- `mywhatsapp_messages_received_total` (counter)
- `mywhatsapp_messages_delivered_total{path=redis|pending_queue}` (counter)
- `mywhatsapp_message_persist_duration_seconds` (histogram)
- `mywhatsapp_pending_queue_size` (histogram, sampled on connect)
- Standard Go runtime metrics (via `otelruntime`).

Client metrics:

- `mywhatsapp_client_messages_sent_total` (counter)
- `mywhatsapp_client_messages_received_total` (counter)
- `mywhatsapp_client_send_duration_seconds` (histogram)

### 8.3 Logs

- All logs are structured JSON via `slog`, with the OTel `slog` bridge so
  log records are exported via OTLP and carry the active trace/span IDs.
- Mandatory fields: `level`, `msg`, `service.name`, `trace_id`, `span_id`,
  `client_id` (where applicable).

### 8.4 OpenTelemetry Collector pipeline

The Collector is deployed as a `Deployment` (single shared instance) in
the `observability` namespace. DaemonSet mode is unnecessary on a
single-node Minikube cluster.

```
receivers: otlp (grpc:4317, http:4318)
processors: batch, memory_limiter, resource (adds k8s.* attrs)
exporters:
  otlp/tempo          -> Tempo            (traces)
  otlphttp/loki       -> Loki             (logs via OTLP)
  prometheus          -> Prometheus       (scrape endpoint exposed by collector)
```

Logs flow via OTLP all the way from app → Collector → Loki (no Promtail or
Alloy). This preserves the OTel log record's native `trace_id`/`span_id`
fields, enabling trace ↔ log correlation in Grafana.

### 8.5 Grafana stack

Deployed in a separate namespace `observability`:

- `grafana` (Deployment + Service)
- `tempo` (Deployment + Service, single-binary mode is sufficient)
- `loki` (Deployment + Service, monolithic mode)
- `prometheus` (Deployment + Service)
- `otel-collector` (Deployment + Service exposing 4317 and 4318)

Datasources are provisioned via ConfigMap. Initial dashboards are out of
scope for the spec.

---

## 9. Infrastructure (`infrastructure/`)

Directory layout:

```
infrastructure/
├── namespaces.yaml
├── postgres/
│   ├── statefulset.yaml
│   ├── service.yaml
│   └── secret.yaml
├── redis/
│   ├── deployment.yaml
│   └── service.yaml
├── server/
│   ├── deployment.yaml
│   ├── service.yaml
│   └── configmap.yaml
├── client/
│   ├── statefulset.yaml
│   ├── service.yaml          # headless service for stable pod DNS
│   └── configmap.yaml
└── observability/
    ├── otel-collector.yaml
    ├── tempo.yaml
    ├── loki.yaml
    ├── prometheus.yaml
    └── grafana.yaml
```

Two namespaces: `mywhatsapp` (app workloads + Postgres + Redis) and
`observability` (Grafana stack + OTel Collector). Manifests are plain YAML
(no Helm/Kustomize) unless we later decide otherwise.

---

## 10. Error Handling

| Condition                                  | Server behavior                                      |
|--------------------------------------------|------------------------------------------------------|
| Missing/empty `client_id` on connect       | 400, no upgrade                                      |
| `client_id` already connected elsewhere    | Accept new; kick old via `client:<id>:control` (code 4000) |
| Malformed `traceparent` in inbound message | Log warning, accept message, start an unlinked span  |
| PostgreSQL write fails on inbound message  | Send JSON error message, close connection with code 1011 |
| Redis publish fails after successful write | Log error, increment counter, do not roll back DB    |
| WebSocket write fails during delivery      | Leave `delivered_at` NULL, close connection          |
| WebSocket write blocks > 10s (slow consumer) | Treat as write failure: close connection with code 1011 |
| Client sends malformed JSON                | Log, drop frame, do not close connection             |
| Client exceeds 5 consecutive bad frames    | Close connection with code 1003                      |
| Server receiving SIGTERM                   | Refuse new upgrades (503); close active with code 1001 (see §6.6) |

Client side:

- Any WebSocket error before `MESSAGE_LIMIT` is reached → exit non-zero,
  log the cause. No reconnect logic in v1.

---

## Decision Log

Resolved during spec drafting (2026-05-14):

| ID  | Decision                                                                       |
|-----|--------------------------------------------------------------------------------|
| D1  | Always publish to Redis, even when sender and recipient share a replica.       |
| D2  | Embed W3C `traceparent` in the JSON envelope for end-to-end traces.            |
| D3  | Duplicate `client_id` → accept new, kick old via `client:<id>:control` channel.|
| D4  | OpenTelemetry Collector runs as a `Deployment` (not DaemonSet).                |
| D5  | Logs flow via OTLP → Collector → Loki (no Promtail/Alloy scraping).            |
| D6  | Client workload is a `StatefulSet`; ring topology (i → (i+1) mod N).           |
| D7  | `CLIENT_ID` is derived from pod `HOSTNAME`, not configurable via env.          |
| D8  | Duplicate-connect race may produce one duplicate delivery — accepted in v1.    |
| D9  | DB schema is bootstrapped in-process via `CREATE TABLE IF NOT EXISTS` at start.|
| D10 | `SERVER_REPLICA_ID` defaults to `HOSTNAME` if not set.                         |
| D11 | Bad-frames threshold = 5 before closing with code 1003.                        |
| D12 | Server shutdown: stop accepting (503), close active connections with 1001.    |
| D13 | Per-connection WS write timeout = 10s; on timeout, close with code 1011.       |
| D14 | Pending-queue flush buffers Redis messages until done, then drains in order.   |
| D15 | Explicit non-goals: no TLS, no auth, no multi-device, no client reconnect.     |
| D16 | `delivery.path` enum = `{redis, pending_queue}` (no `direct`; matches D1).     |
