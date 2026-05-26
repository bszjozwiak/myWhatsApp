---
name: domain-structure
description: Use when creating new Go code, adding a new feature/area of behavior, reviewing layout, or refactoring file/package organization in this project's server/ or client/ modules. Enforces domain-based directory structure with three required files per domain — domain.go (types), service.go (business logic), dao.go (data access).
---

# Go domain-based code structure (myWhatsApp)

This project organizes Go code by **domain**, not by technical layer. A
"domain" is a cohesive area of behavior (e.g., messages, connections,
clients), not a layer (handlers, services, dtos).

Each domain is a Go package (= a directory). Within each package, at
minimum, the following three files **must** exist:

| File         | Purpose                                                                |
|--------------|------------------------------------------------------------------------|
| `domain.go`  | Type declarations for the domain's entities (structs, IDs, errors).    |
| `service.go` | Business logic operating on those entities. Depends on a DAO interface.|
| `dao.go`     | Data access — Postgres, Redis, an external HTTP API, etc.              |

## Required layout

```
server/
├── main.go              ← composition root: wires DAOs → services → handlers
├── messages/            ← "messages" domain
│   ├── domain.go        ← Message, MessageID, ErrNotFound, …
│   ├── service.go       ← Service: Ingest, FetchPending, MarkDelivered
│   └── dao.go           ← DAO interface + Postgres implementation
├── connections/         ← "connections" domain (WS session state)
│   ├── domain.go        ← Connection, Registry types
│   ├── service.go       ← Service: Register, Kick, Broadcast
│   └── dao.go           ← DAO interface + Redis pub/sub implementation
└── …                    ← more domains as the spec adds them
```

Mirror this layout in `client/` for any client-side domains.

## Hard rules

1. **One package per domain.** Group by *what the code is about*, not
   by *what kind of thing it is*. Resist the temptation to create
   `handlers/`, `services/`, `repositories/` top-level directories.

2. **All three files always exist.** If a domain has no real DAO yet
   (e.g., purely in-memory), `dao.go` still exists with a minimal
   interface and an in-memory stub, plus a `TODO` referencing the
   `SPEC.md` section that will make it real.

3. **No infrastructure types in `service.go`.** A service depends on a
   DAO interface declared in `dao.go`. It must not import
   `database/sql`, `*pgxpool.Pool`, `redis.Client`, `http.Client`, or
   similar directly.

4. **`main.go` is the composition root.** It is the only file allowed
   to construct concrete DAOs with their drivers/clients and inject them
   into services.

5. **Cross-domain calls go service → service.** Never DAO → DAO, and
   never an HTTP/WS handler → DAO directly. Handlers depend on a
   service.

6. **DAO interfaces live next to their implementation.** Define the DAO
   interface in `dao.go` of the *owning* domain. Other packages that
   need data access call into the owning domain's service, not its DAO.

7. **Tests live next to the file under test.** `service_test.go` next to
   `service.go`, etc. Tests for the service mock the DAO interface.

## When a domain grows

You may add more files **alongside** the required three — never replacing
them. Common extensions:

- `handler.go` — HTTP/WebSocket handlers if the domain owns an entry point.
- `events.go` — event types/publishing if the domain emits events.
- `<something>.go` — split out a coherent sub-concern when `service.go`
  exceeds ~300 lines.

## When NOT to create a new domain

- **Generic utilities** (logging adapters, ID generators, error helpers)
  go in a flat `internal/` package, not a domain.
- **One-off types used by exactly one domain** stay inside that domain;
  don't promote them to a new package.

## Procedure when starting work that touches code structure

1. Identify the domain the task in `PLAN.md` is about (e.g., T2.3 is the
   `messages` domain, T2.5 is `connections`).
2. If the domain directory doesn't exist, create it with all three
   required files.
3. If it does exist, add to it — don't create a parallel package.
4. If the work doesn't fit cleanly into an existing domain and you're
   uncertain which domain to use, **stop and ask the user** rather than
   creating a new domain on your own judgment.

## Procedure when reviewing/refactoring

If you see code that violates these rules (e.g., DB calls in
`main.go`, a `handlers/` top-level dir, missing `dao.go`), propose a
refactor in a new PLAN.md task — do **not** silently restructure during
an unrelated task.
