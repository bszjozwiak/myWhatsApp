# Project rules for AI assistants

This project follows **Spec Driven Development (SDD)**. Read this file in
full at the start of every session before touching any code.

## The contract

Two files define everything about this project:

- **`SPEC.md`** — the source of truth for *what* the system does. Every
  binding design decision is recorded here, with a Decision Log at the
  bottom (D1, D2, …) explaining *why*.
- **`PLAN.md`** — the ordered task list (T0.1, T1.1, T2.1, …). Each task
  names the spec sections it implements and an objectively verifiable
  **Acceptance** criterion.

You never make design decisions during implementation. The spec already
made them. If you find yourself wondering "should it…?", **stop and read
the spec**. If the spec is silent on the question, see "Ambiguity" below.

## Rules

1. **Implement only the active task.** When the user says "Implement
   T2.3", read PLAN.md → find T2.3 → read every SPEC.md section listed
   under its **Implements**. Touch only files needed for that task. Do
   not pre-implement future tasks, refactor unrelated code, or add
   "useful" helpers not required by the task.

2. **Ambiguity → stop, don't guess.** If the spec is unclear,
   contradictory, or silent on something the task needs:
   - Stop coding.
   - Describe what's unclear.
   - Propose an edit to `SPEC.md`, including a new Decision Log entry
     if the choice is binding.
   - Wait for the user to approve the spec change before resuming.

3. **Acceptance is the bar for "done".** Every task in PLAN.md has an
   Acceptance check. Run it. Paste its output. Only then claim the task
   is complete. "Looks good to me" is not acceptable.

4. **One task per branch, one commit per task.**
   - Branch name: `task/T<ID>-<short-slug>` (e.g.,
     `task/T2.3-message-ingest`).
   - Commit message: `T<ID>: <short summary>` (e.g.,
     `T2.3: parse and persist inbound messages`).
   - Wait for the user to say "commit" before committing.

5. **Pinned decisions stay pinned.** When the spec says something
   specific (e.g., "always publish to Redis, even on same replica" — D1),
   don't optimize it away. Decisions in the Decision Log have a reason;
   if you think a decision is wrong, propose a spec change (Rule 2),
   don't silently violate it.

## Project shape (quick reference)

- Go monorepo with two modules: `server/` and `client/`.
- WebSocket via `github.com/gorilla/websocket`.
- PostgreSQL for message persistence; Redis for inter-replica Pub/Sub.
- Kubernetes-native deployment under `infrastructure/`, targeting local
  Minikube.
- Goal: observability testbed (OpenTelemetry → Grafana stack).
- Client topology is a ring: pod `client-i` sends to `client-(i+1) mod
  TOTAL_CLIENTS`. Identity comes from `HOSTNAME`. Client is a
  StatefulSet; scaling it changes the ring size.

## Code structure convention (binding)

Go code is organized by **domain**, not by layer. Each domain is its
own package directory containing at minimum three files: `domain.go`
(types), `service.go` (business logic), `dao.go` (data access). See
`.claude/skills/domain-structure/SKILL.md` for the full rules. Apply
this convention to every new file and every refactor.

## Non-goals (do not implement)

- TLS, authentication, or authorization.
- Multi-device support.
- Client reconnect / retry logic.
- Helm or Kustomize (plain YAML manifests only).

See `SPEC.md` §1 "Non-goals" and Decision Log D15 for the full list.

## When in doubt

`SPEC.md` is the answer. If `SPEC.md` doesn't answer it, the answer is
"stop and ask".
