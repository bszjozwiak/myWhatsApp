# myWhatsApp — Implementation Plan

**Spec version targeted:** `SPEC.md` v0.3
**Last updated:** 2026-05-14

This plan slices `SPEC.md` into ordered tasks. Every task references the
spec section(s) it implements and has a concrete acceptance criterion. **No
design decisions should happen during implementation — if you find one,
update `SPEC.md` first, then return to coding.**

## How to read this plan

- Tasks are grouped into **Stages**. A stage must be fully complete before
  the next stage starts. Within a stage, tasks marked with the same
  dependency level may be done in parallel.
- **Implements:** points at the spec section(s) the task fulfills.
- **Depends on:** lists task IDs that must be complete first.
- **Acceptance:** an objectively verifiable check. If the check passes, the
  task is done.

---

## Stage 0 — Repository foundation

### T0.1 — Monorepo scaffolding

- **Implements:** §9 (directory layout), §3.1, §3.2
- **Depends on:** —
- **What:** Create the top-level directory structure:
  ```
  client/    server/    infrastructure/    SPEC.md    PLAN.md    .gitignore    README.md
  ```
  Initialize two Go modules (`server/go.mod`, `client/go.mod`) with module
  paths `github.com/<owner>/myWhatsApp/server` and `.../client`. Optionally
  use a `go.work` at the root for IDE convenience.
- **Acceptance:** `cd server && go build ./...` and `cd client && go build
  ./...` both succeed with no source files (just empty `main.go` stubs).

---

## Stage 1 — Infrastructure primitives

Both tasks can run in parallel.

### T1.1 — Namespaces

- **Implements:** §9
- **Depends on:** T0.1
- **What:** `infrastructure/namespaces.yaml` creating `mywhatsapp` and
  `observability`.
- **Acceptance:** `kubectl apply -f infrastructure/namespaces.yaml` succeeds;
  `kubectl get ns` shows both namespaces.

### T1.2 — PostgreSQL StatefulSet

- **Implements:** §3.3, §9 (`postgres/`)
- **Depends on:** T1.1
- **What:** `infrastructure/postgres/{statefulset.yaml, service.yaml,
  secret.yaml}`. Single replica, PVC-backed. Secret holds `POSTGRES_USER`,
  `POSTGRES_PASSWORD`, `POSTGRES_DB=mywhatsapp`.
- **Acceptance:** Pod reaches `Ready`; `kubectl exec` → `psql -U <user> -d
  mywhatsapp -c '\dt'` returns an empty table list (schema is created later
  by the server, per D9).

### T1.3 — Redis Deployment

- **Implements:** §3.4, §9 (`redis/`)
- **Depends on:** T1.1
- **What:** `infrastructure/redis/{deployment.yaml, service.yaml}`. Single
  replica, no persistence (Pub/Sub only).
- **Acceptance:** `kubectl exec` into the pod → `redis-cli PING` returns
  `PONG`.

---

## Stage 2 — Server core (no observability yet)

Observability is deliberately deferred to Stage 5 so we first get a
working chat system, then instrument it. This avoids re-doing
instrumentation each time we touch a span boundary.

### T2.1 — Server skeleton + WS upgrade

- **Implements:** §4.1 (basic accept path), §3.1, §10 (missing `client_id`)
- **Depends on:** T0.1
- **What:** HTTP server on `SERVER_PORT`; `/ws` endpoint upgrades using
  `gorilla/websocket`. Reject with 400 if `client_id` query param is
  missing/empty. No DB or Redis yet; just echo every received frame back.
- **Acceptance:** With a local Go test client, connecting to `ws://localhost:8080/ws?client_id=test`
  succeeds; connecting without `client_id` returns HTTP 400; sending text
  comes back echoed.

### T2.2 — PostgreSQL connection + schema bootstrap

- **Implements:** §3.3 (D9), §5
- **Depends on:** T2.1, T1.2
- **What:** Open `*sql.DB` from `POSTGRES_DSN` at startup. Run idempotent
  `CREATE TABLE IF NOT EXISTS messages (…)` and `CREATE INDEX IF NOT
  EXISTS idx_messages_pending …` per §5. Fail-fast on connection error.
- **Acceptance:** Start server pointed at the PG service. `psql -c '\d
  messages'` shows the exact schema from §5, including the partial index.

### T2.3 — Message ingest (parse + persist)

- **Implements:** §4.2 (outbound), §5, §6.2 (persist step), §10 (PG-write failure, malformed JSON)
- **Depends on:** T2.2
- **What:** On each inbound frame, parse JSON `{to, body, traceparent}`.
  Derive `from` from the connection's `client_id`. Generate a ULID for `id`.
  Insert into `messages`. Handle malformed JSON (drop frame, increment a
  local bad-frame counter; close with 1003 after 5 — D11). On PG failure,
  send a JSON error message and close with 1011.
- **Acceptance:** Test client sends a well-formed message → row appears in
  `messages` with correct `sender`, `recipient`, `body`, `id`, `created_at`,
  `delivered_at IS NULL`. Sending 5 malformed frames closes the connection
  with code 1003.

### T2.4 — Redis publish on ingest

- **Implements:** §6.2 (D1), §6.3, §10 (Redis publish failure)
- **Depends on:** T2.3, T1.3
- **What:** After successful PG insert, `PUBLISH client:<to>` a JSON payload
  matching the inbound message format in §4.2 (without `traceparent`
  yet — that arrives in Stage 5). Publish failure is logged but does **not**
  roll back the insert.
- **Acceptance:** `redis-cli SUBSCRIBE client:test2` in one shell, send a
  message `{to:"test2",…}` from a test client → published payload appears
  in the subscriber.

### T2.5 — Redis subscribe per client connection

- **Implements:** §4.3 steps 3 & 5–6, §6.3, §6.5
- **Depends on:** T2.4
- **What:** On WS accept, subscribe to `client:<id>` and
  `client:<id>:control`. Forward `client:<id>` messages to the WebSocket.
  On WS close, unsubscribe.
- **Acceptance:** Two test clients (A, B). A sends `{to:"B",…}`. B
  receives the JSON payload over its WebSocket. Closing B's connection,
  then sending another message from A → B's old subscription is gone (no
  Redis subscribers reported by `PUBSUB NUMSUB client:B`).

### T2.6 — Pending-queue flush + buffer-and-drain

- **Implements:** §4.3 steps 4–5, §6.1, §6.4, §10 (write failure)
- **Depends on:** T2.5
- **What:** Implement the buffer-then-drain protocol from §4.3:
  1. After subscribing, but before flushing, buffer any Redis messages
     in memory.
  2. SELECT `WHERE recipient = X AND delivered_at IS NULL ORDER BY
     created_at ASC`.
  3. For each row, write to WS; on success, `UPDATE messages SET
     delivered_at = NOW() WHERE id = …` **per message**.
  4. Drain the in-memory Redis buffer in arrival order, writing each and
     updating `delivered_at` similarly.
  5. Transition to steady-state forwarding.

  Write failure on any step leaves the row's `delivered_at` NULL and
  closes the WS.
- **Acceptance:**
  1. Stop client B. Client A sends 3 messages to B. Verify 3 rows with
     `delivered_at IS NULL`.
  2. Reconnect B. B receives all 3 messages in `created_at` order. All
     `delivered_at` columns are now populated.
  3. Race test: while B's pending flush is in progress (insert a manual
     delay), have A send a 4th live message. B receives messages 1–3
     before 4, then 4.

### T2.7 — Duplicate `client_id` kick

- **Implements:** §4.1 (kick), §10 (already-connected row)
- **Depends on:** T2.5
- **What:** On new connection, publish `kick` on `client:<id>:control`.
  Locally, on receiving a `kick` for a client we currently own, close that
  WS with code 4000 and clean up.
- **Acceptance:** Connect twice as `client_id=test`. The first WS receives
  close code 4000 within ~100ms of the second connecting; the second
  remains usable.

### T2.8 — Per-connection write timeout + bad-frames counter

- **Implements:** §10 (slow-consumer 10s timeout, 5 bad frames)
- **Depends on:** T2.3, T2.5
- **What:** Use `SetWriteDeadline(time.Now().Add(10s))` before each
  `WriteMessage`. Maintain a per-connection counter for consecutive
  malformed frames; reset on any valid frame.
- **Acceptance:** A test consumer that never reads → server's write
  deadline fires; connection closes with code 1011 within ~10s of the next
  delivery attempt.

### T2.9 — Graceful shutdown

- **Implements:** §6.6 (D12), §10 (SIGTERM row)
- **Depends on:** T2.5
- **What:** On `SIGTERM`: HTTP handler starts returning 503 for new `/ws`
  upgrades. Iterate active connections, send WS close frame 1001, wait up
  to 10s for ack, force-close after. Hard deadline 30s; configure pod
  `terminationGracePeriodSeconds: 30` in the manifest.
- **Acceptance:** Run server, connect a client, send `SIGTERM` to the
  process. Client receives a 1001 close. New connections during shutdown
  fail with HTTP 503. Process exits within 30s.

### T2.10 — Server Deployment + Service manifests

- **Implements:** §3.1 (Deployment, scalable), §9 (`server/`)
- **Depends on:** T2.2 through T2.9
- **What:** `infrastructure/server/{deployment.yaml, service.yaml,
  configmap.yaml}`. ClusterIP Service. Container image built from
  `server/Dockerfile`. Env wiring per §7.1. `terminationGracePeriodSeconds:
  30`.
- **Acceptance:** `kubectl apply -f infrastructure/server` →
  `kubectl get pods -n mywhatsapp` shows the server `Ready`; scaling to 3
  replicas works; from a debug pod, `wscat ws://server:8080/ws?client_id=x`
  connects.

---

## Stage 3 — Client core (no observability yet)

### T3.1 — Client skeleton + identity

- **Implements:** §3.2, §7.2 (CLIENT_ID derivation)
- **Depends on:** T0.1
- **What:** Read `HOSTNAME`. Parse trailing integer to obtain ordinal.
  Compute `recipient = "client-" + ((ordinal+1) mod TOTAL_CLIENTS)`. Log
  identity and recipient at startup.
- **Acceptance:** Running the binary with `HOSTNAME=client-3 TOTAL_CLIENTS=10`
  logs `client_id=client-3 recipient=client-4`. With `TOTAL_CLIENTS=1` it
  logs the self-loop case.

### T3.2 — WebSocket connect

- **Implements:** §4.1, §4.3
- **Depends on:** T3.1, T2.1
- **What:** Connect to `SERVER_URL` with `?client_id=<derived>`. Reconnect
  is **not** implemented (D15). On connection failure, exit non-zero with
  a clear error.
- **Acceptance:** Pointed at a running server, the client logs a
  successful connection; pointed at a wrong URL, exits non-zero.

### T3.3 — Send loop with ring topology

- **Implements:** §3.2, §7.2 (interval, limit, ring), §4.2 (outbound JSON)
- **Depends on:** T3.2
- **What:** Every `MESSAGE_INTERVAL_MS`, send `{"to":"<recipient>","body":"Hello
  from <client_id>"}` (no `traceparent` yet — Stage 5). After sending
  `MESSAGE_LIMIT` messages, send a clean close frame (1000) and exit 0.
- **Acceptance:** With `TOTAL_CLIENTS=2 MESSAGE_LIMIT=3 MESSAGE_INTERVAL_MS=500`,
  the server's `messages` table records exactly 3 inserts from this client
  within ~1.5s; the client exits 0.

### T3.4 — Receive loop

- **Implements:** §3.2 (receives & logs)
- **Depends on:** T3.2
- **What:** Concurrent reader goroutine that decodes inbound frames per
  §4.2 (inbound) and logs them. Does **not** affect the send loop's
  termination criterion.
- **Acceptance:** With two clients in a ring, each client's log shows
  inbound messages from its predecessor.

### T3.5 — Client StatefulSet manifest

- **Implements:** §3.2 (StatefulSet), §9 (`client/`), §7.2
- **Depends on:** T3.1 through T3.4
- **What:** `infrastructure/client/{statefulset.yaml, service.yaml,
  configmap.yaml}`. Headless service (`clusterIP: None`) for stable pod
  DNS. Env wiring per §7.2. The pod's `HOSTNAME` will be
  `client-0`/`client-1`/…
- **Acceptance:** `kubectl apply -f infrastructure/client/ &&
  kubectl scale sts/client --replicas=3` → 3 pods running, each logging a
  distinct identity and the expected ring recipient.

---

## Stage 4 — Observability backends

These can run mostly in parallel after T4.1; T4.6 must come last.

### T4.1 — OpenTelemetry Collector

- **Implements:** §8.4 (D4)
- **Depends on:** T1.1
- **What:** `infrastructure/observability/otel-collector.yaml`. Deployment
  + Service exposing 4317 (gRPC) and 4318 (HTTP). Pipeline per §8.4:
  receivers `otlp`; processors `memory_limiter`, `batch`, `resource`;
  exporters `otlp/tempo`, `otlphttp/loki`, `prometheus`.
- **Acceptance:** Pod `Ready`; from a debug pod, sending an OTLP probe
  payload to `:4317` returns success.

### T4.2 — Tempo

- **Implements:** §8.5
- **Depends on:** T1.1
- **What:** Tempo in single-binary mode; in-memory or PVC-backed local
  storage is fine. Service exposes OTLP receivers and HTTP query API.
- **Acceptance:** Pod `Ready`; HTTP `/ready` returns 200.

### T4.3 — Loki

- **Implements:** §8.5 (D5)
- **Depends on:** T1.1
- **What:** Loki in monolithic mode. Service on default ports.
- **Acceptance:** `GET /ready` returns 200.

### T4.4 — Prometheus

- **Implements:** §8.5
- **Depends on:** T1.1, T4.1
- **What:** Prometheus configured to scrape the Collector's metrics
  endpoint (the `prometheus` exporter from T4.1).
- **Acceptance:** `GET /-/ready` returns 200; targets page shows the
  Collector as `UP`.

### T4.5 — Grafana

- **Implements:** §8.5
- **Depends on:** T4.2, T4.3, T4.4
- **What:** Grafana with three provisioned datasources via ConfigMap:
  Tempo, Loki, Prometheus.
- **Acceptance:** Login to Grafana UI; Datasource → each one's "Save &
  Test" returns green.

### T4.6 — Sanity test the OTLP pipeline (no app yet)

- **Implements:** §8.4
- **Depends on:** T4.1–T4.5
- **What:** From a debug pod, send a hand-crafted OTLP test payload
  (trace + metric + log) into the Collector. Verify each appears in its
  backend via Grafana.
- **Acceptance:** A test trace shows up in Tempo; a test metric in
  Prometheus; a test log line in Loki.

---

## Stage 5 — Application instrumentation

This is the meat of the project — everything before this just creates the
substrate.

### T5.1 — Server OTel SDK init

- **Implements:** §7.1 (env), §8 (general)
- **Depends on:** T2.10, T4.6
- **What:** Initialize tracer/meter/logger providers from
  `OTEL_EXPORTER_OTLP_ENDPOINT` + `OTEL_SERVICE_NAME`. Use OTLP gRPC.
  Set `service.name` and `server.replica` (from `SERVER_REPLICA_ID`/`HOSTNAME`,
  D10) as resource attributes. Wire `slog` through the OTel slog bridge so
  log records carry `trace_id`/`span_id`.
- **Acceptance:** With server running, the Collector's logs show batches
  arriving on the OTLP receiver from `mywhatsapp-server`.

### T5.2 — Server trace instrumentation

- **Implements:** §8.1 (server spans)
- **Depends on:** T5.1
- **What:** Create the spans listed in §8.1 with the listed attributes:
  `ws.connection`, `message.receive`, `db.insert_message`, `redis.publish`,
  `message.deliver`, `db.mark_delivered`, `db.fetch_pending`. Use
  `otelsql` or manual span wrapping for the DB calls; manual for Redis.
- **Acceptance:** Send a message through; in Grafana → Tempo, find a trace
  with the full nesting: `ws.connection ▸ message.receive ▸
  db.insert_message + redis.publish`, and (for the recipient connection)
  `ws.connection ▸ message.deliver ▸ db.mark_delivered`.

### T5.3 — Server metrics instrumentation

- **Implements:** §8.2 (server metrics)
- **Depends on:** T5.1
- **What:** Register every metric in §8.2 (counters, gauge, histograms).
  Add Go runtime metrics via `otelruntime`. Ensure `delivery.path` label
  matches D16 (`redis | pending_queue`).
- **Acceptance:** In Grafana → Prometheus, querying
  `mywhatsapp_ws_connections_active` returns a non-zero value while a
  client is connected; `mywhatsapp_messages_delivered_total{path="redis"}`
  increments per message.

### T5.4 — Server log bridge (OTLP → Loki)

- **Implements:** §8.3, §8.4 (D5)
- **Depends on:** T5.1
- **What:** Confirm all `slog` records flow via OTLP to the Collector and
  on to Loki with `trace_id`/`span_id` fields. Mandatory fields per §8.3.
- **Acceptance:** In Grafana → Loki, query
  `{service_name="mywhatsapp-server"}`; logs show `trace_id` and
  `span_id`. Clicking a `trace_id` opens the matching trace in Tempo.

### T5.5–T5.8 — Client equivalents

- **T5.5 — Client OTel SDK init.** Mirror T5.1 for the client.
  **Acceptance:** Collector receives batches from `mywhatsapp-client`.
- **T5.6 — Client trace instrumentation.** Spans per §8.1 (client):
  `client.run`, `message.send`, `message.receive` with attributes.
  **Acceptance:** Sending one message produces a trace in Tempo with a
  `message.send` span.
- **T5.7 — Client metrics instrumentation.** Metrics per §8.2 (client).
  **Acceptance:** `mywhatsapp_client_messages_sent_total` increments in
  Prometheus.
- **T5.8 — Client log bridge.** Same as T5.4 for client.
  **Acceptance:** Loki shows client logs with trace IDs.

### T5.9 — Traceparent propagation (end-to-end traces)

- **Implements:** §4.2 (`traceparent` field), §8.1 (propagation chain), §10 (malformed traceparent)
- **Depends on:** T5.2, T5.6
- **What:**
  - **Client sender:** before sending, serialize the active `message.send`
    span context into the outbound JSON's `traceparent` field using the
    W3C TraceContext propagator.
  - **Server receive:** extract the `traceparent` from the inbound JSON
    and start `message.receive` as a child of it. Log a warning and start
    an unlinked span if the field is malformed.
  - **Server deliver:** serialize the `message.deliver` span context into
    the inbound JSON's `traceparent` field before writing.
  - **Client receive:** extract from inbound JSON and start
    `message.receive` as a child.
- **Acceptance:** Send a single message from client-0 to client-1. In
  Grafana → Tempo, find **one** trace ID spanning all of:
  `client.run/message.send ▸ ws.connection (server, sender side)
  ▸ message.receive ▸ db.insert_message + redis.publish ▸
  message.deliver (server, recipient side) ▸ db.mark_delivered ▸
  client.run/message.receive (client-1)`.

---

## Stage 6 — End-to-end verification

The final stage is purely verification — checking that the deployed system
behaves as `SPEC.md` says it should.

### T6.1 — Smoke test: TOTAL_CLIENTS=1 self-loop

- **Implements:** §7.2 edge case (V13)
- **Depends on:** T5.9
- **What:** Scale client StatefulSet to 1; set `TOTAL_CLIENTS=1
  MESSAGE_LIMIT=5 MESSAGE_INTERVAL_MS=1000`.
- **Acceptance:** Within ~6s, exactly 5 messages with `sender = recipient =
  client-0` exist in `messages`; client exits 0; 5 distinct end-to-end
  traces appear in Tempo, each self-looping back to the same pod.

### T6.2 — Small ring: TOTAL_CLIENTS=3

- **Implements:** §3.2 (ring), §6 (all flows)
- **Depends on:** T6.1
- **What:** Scale to 3; `MESSAGE_LIMIT=10 MESSAGE_INTERVAL_MS=500`.
- **Acceptance:** Each client sends to the correct neighbor (0→1, 1→2,
  2→0); 30 total rows in `messages`; all `delivered_at` populated within
  ~6s.

### T6.3 — Offline delivery test

- **Implements:** §6.4, §4.3 (buffer-and-drain)
- **Depends on:** T6.2
- **What:** Scale to 2 clients. Kill client-1 mid-run; verify rows
  accumulate with `delivered_at IS NULL`. Bring client-1 back; verify it
  receives all pending in `created_at` order.
- **Acceptance:** Pending count goes ≥ 5 then drops to 0 within 1s of
  reconnect; client-1 log shows pending messages in correct order; trace
  for each pending message has `delivery.path="pending_queue"`.

### T6.4 — Cross-replica routing test

- **Implements:** §6.3, D1
- **Depends on:** T6.2
- **What:** Scale server to 3 replicas. Run 6 clients. Use `kubectl get
  endpoints` and `kubectl exec` netstat to identify which client connected
  to which server replica. Verify a message from a client on `server-0`
  to a client on `server-2` is delivered.
- **Acceptance:** Trace shows `redis.publish` on the sender replica and
  `message.deliver` on the recipient replica (different `server.replica`
  attributes on the two spans within one trace).

### T6.5 — Scale test: TOTAL_CLIENTS=1000

- **Implements:** project goal (Overview)
- **Depends on:** T6.4
- **What:** Scale client to 1000 and server to e.g. 3 replicas. Pick a
  modest `MESSAGE_INTERVAL_MS` (e.g. 2000) and `MESSAGE_LIMIT` (e.g. 20).
  Watch dashboards.
- **Acceptance:** All clients connect; `mywhatsapp_ws_connections_active`
  reaches ~1000 across the server pods; all clients exit 0; no
  PostgreSQL/Redis/Collector pods restarted; Grafana dashboards remain
  responsive throughout.

### T6.6 — Verification of observability correlation

- **Implements:** §8 (overall)
- **Depends on:** T6.5
- **What:** In Grafana, pick a random successful end-to-end trace from
  Tempo. Click the trace ID; confirm Loki shows the corresponding logs
  from both client and server pods for that trace. Cross-check
  Prometheus shows the same counter increments at the trace's wall-clock
  time.
- **Acceptance:** A single trace can be correlated to (a) logs from at
  least two services and (b) the metric increments it caused. This is
  the project's ultimate success criterion.

---

## Out of scope (recorded for completeness)

- TLS, authentication, multi-device (D15).
- Reconnect logic in clients (D15).
- Initial Grafana dashboards (§8.5).
- Resource requests/limits in K8s manifests (V14).
- Helm/Kustomize (§9 — plain YAML only for now).

When you finish T6.6, the spec has been fully realized end-to-end and
the project's stated goal — "generate realistic distributed-system
traffic for observability experimentation" — is met.
