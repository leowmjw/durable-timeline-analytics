# AGENTS.md — Go + Temporal Port of Durable Timeline Analytics

Instructions for agents working on the Go port of the Durable Timeline Analytics system.

---

## Project Context

This is a **port** of a Rust/Golem durable timeline analytics system to **Go 1.26 + Temporal**. The original system implements the [TimeLine Analytics paper (CIDR 2023)](https://www.cidrdb.org/cidr2023/papers/p22-milner.pdf) — a composable DSL for expressing temporal queries over event streams, backed by a durable execution engine.

**The original Rust code lives in the parent directory**. The Go port lives under `go/`.

### Reference Documents

- `go/ARCHITECTURE.md` — Full system architecture, data types, agent types, data flow, Temporal mapping
- `go/PRD.md` — Product requirements, functional requirements, test scenarios
- `go/TECHSPEC.md` — Exact behavioral specifications, algorithms, all test cases with expected values

**Read all three before making any changes.**

---

## What You're Building

A Go + Temporal implementation that produces **identical results** to the Rust original for the same inputs. The system has four workflow types:

1. **EventProcessor** (leaf) — ingests raw events, evaluates leaf operations (LatestEventToState, HasExisted, HasExistedWithin), pushes state changes to parent via Temporal signal
2. **TimelineProcessor** (derived) — receives child state changes via signal, recomputes (Comparison, Negation, And, Or, DurationWhere, DurationInCurState), pushes upward
3. **Aggregator** — cross-session accumulator (count, sum, avg), receives deltas via signal
4. **TimelineDriver** — orchestrator that walks a DSL expression tree, spawns all workflows, wires parent/child relationships

Plus:
5. **DSL Parser** — lexer + parser for the timeline query language
6. **StateDynamicsTimeLine** — core data structure (sorted map of time-bounded state intervals)

---

## Build and Test

```bash
cd go
mise install          # Install Go 1.26 + Temporal CLI
make build            # Build all packages
make test             # Run unit tests (DSL parser, domain types)
make integration-test # Run integration tests (requires Temporal server)
make compare          # Cross-runtime comparison with Rust original
```

---

## Architecture Quick Reference

### Push-Based Agent Graph

Events flow bottom-up: leaf nodes ingest events → push state changes to parent → cascade to root → root pushes delta to aggregator.

```
EventProcessor (leaf) → TimelineProcessor (derived) → ... → TimelineProcessor (root) → Aggregator
```

### Agent Naming

`{session_id}-{operation_kebab}-{counter}` where counter is pre-order DFS number.

Example for CIRR with session "sess-42":
```
sess-42-duration-where-1     (root)
sess-42-and-2
sess-42-and-3
sess-42-has-existed-4        (LEAF)
sess-42-not-5
sess-42-has-existed-within-6 (LEAF)
sess-42-equal-to-7
sess-42-latest-event-to-state-8 (LEAF)
```

### Temporal Mapping

| Concept | Implementation |
|---|---|
| Each agent | Long-running Temporal workflow |
| `add_event()` | Signal to leaf workflow |
| `on_child_state_changed()` | Signal to parent workflow |
| `get_leaf_result()` / `get_derived_result()` | Query on workflow |
| `on_delta()` / `register_session()` | Signal to aggregator workflow |
| `get_aggregation_result()` | Query on aggregator workflow |
| Agent state | Workflow local variables (durable via replay) |

---

## Key Implementation Details

### StateDynamicsTimeLine

The most critical data structure. A sorted map (`BTreeMap` equivalent in Go) of time-bounded state intervals. See TECHSPEC.md §1 for the exact `add_state_dynamic_info` algorithm.

**Critical**: `get_state_at(t)` returns the last point with `t1 < t` (strict less-than).

### And/Or Sibling Lookup

When `And` or `Or` receives a child state change at time `t`, it reads both children via `get_state_at(t + 1)`. The `+1` ensures the just-written value is visible.

### TlHasExistedWithin

A single matching event produces **TWO** parent notifications: one for `true` at time `t`, one for `false` at time `t + duration`.

### DurationState

```
Climbing{base, since}: counter = base + (now - since)
Flat{value}: counter stopped at this value
```

Query at time t: if Climbing and `t >= since` → `base + (t - since)`.

### Aggregator Delta

```
delta = event_value_to_f64(new_value) - last_aggregated_value
```
- `IntValue(n)` → `float64(n)`
- `FloatValue(f)` → `f`
- `BoolValue(true)` → `1.0`, `BoolValue(false)` → `0.0`
- Skip if delta == 0.0. Lazy-create aggregator on first non-zero delta.

---

## Coding Conventions

- **No panics**. Return errors and propagate with proper Go error handling.
- **Named types** for all domain concepts. No raw `string` or `int64` where a domain type exists.
- Use `EventColumnName` (not `string`), `EventValue` (not `interface{}`), etc.
- Comprehensive error messages with context.
- Idiomatic Go: interfaces for workflow contracts, structs for state.
- Tests must match the exact expected values from TECHSPEC.md.

---

## Test Scenarios Summary

### Unit Tests (DSL Parser)
35+ test cases in TECHSPEC.md §6 covering all grammar productions, operator precedence, aggregation, and error cases.

### Integration Tests

1. **Smoke — Single Leaf**: LatestEventToState, feed one event, query result
2. **Smoke — State Transitions**: Three events, query at intermediate times
3. **Boolean Logic Propagation**: And + Not, two events changing root from true→false
4. **CIRR Propagation**: Full 8-agent CIRR tree, verify DurationWhere counting
5. **Multi-Session Aggregation**: Two sessions, shared aggregator, verify count=2

### Cross-Runtime Comparison
Run same events through Rust original and Go port, compare all query results at all time points.

---

## Directory Structure

```
go/
├── cmd/worker/              # Temporal worker main.go
├── internal/
│   ├── domain/              # EventValue, Event, StateDynamicsTimeLine, TimeLineOp, etc.
│   ├── dsl/                 # Lexer, Parser
│   └── workflows/           # EventProcessor, TimelineProcessor, Aggregator, TimelineDriver
├── test/                    # Integration tests
├── go.mod
├── go.sum
├── Makefile
├── mise.toml
├── ARCHITECTURE.md          # System architecture
├── PRD.md                   # Product requirements
└── TECHSPEC.md              # Technical specification with test cases
```

---

## Things to Watch Out For

- **get_state_at uses strict less-than**: `t1 < t`, not `t1 <= t`. This is why And/Or use `time+1`.
- **TlHasExistedWithin produces two notifications** per matching event (true at t, false at t+duration).
- **DurationWhere stores IntValue** (not FloatValue) for the cumulative count.
- **Aggregator naming**: `aggregator-{column}-{value}`, e.g., `aggregator-cdn-akamai`.
- **Pre-order DFS counter** increments BEFORE recursion, producing deterministic agent names.
- **EventPredicate evaluation** returns false if the column is missing from the event.
- **Graph nodes[0] is always the root** in the flat TimelineOpGraph encoding.
- **Comparison operations** in the DSL predicate context only support `==`, `>`, `<` (no `>=`, `<=`). The `>=` and `<=` are only available as postfix comparison operators on timeline expressions.
