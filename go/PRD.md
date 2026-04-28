# Product Requirements Document — Timeline Analytics Go Port

## 1. Background

The **Durable Timeline Analytics** system implements the [TimeLine Analytics paper (CIDR 2023)](https://www.cidrdb.org/cidr2023/papers/p22-milner.pdf) on top of a durable execution runtime. The original implementation uses Rust compiled to WebAssembly, running on [Golem](https://learn.golem.cloud). This document specifies the requirements for porting the system to **Go + Temporal**.

### What the System Does

The system provides a **composable DSL for expressing temporal analytics over event streams**. Users write timeline queries like:

```javascript
duration_where(
  has_existed(playerStateChange == "play")
  && !has_existed_within(playerStateChange == "seek", 5)
  && latest_event_to_state(playerStateChange) == "buffer"
) | aggregate(group_by(cdn), count, sum, avg)
```

The system compiles this into a **tree of durable workers** (agents). Each node in the expression tree becomes a separate durable workflow that:
- Ingests events (leaves)
- Computes derived state from children (derived nodes)
- Pushes state changes upward (push-based cascade)
- Accumulates metrics across sessions (aggregator)

### Primary Use Case: CIRR (Connection-Induced Rebuffering Ratio)

A video streaming platform (like Netflix, Disney+) wants to measure how long users experience buffering that is NOT caused by seeking. The CIRR metric combines three conditions:

1. The user has started playing (`has_existed(playerStateChange == "play")`)
2. There was no recent seek event (`!has_existed_within(playerStateChange == "seek", 5)`)
3. The current player state is "buffer" (`latest_event_to_state(playerStateChange) == "buffer"`)

When all three are true, the system counts the duration — this is the CIRR value.

### Additional Use Cases

- **Time spent idle per region**: `duration_in_cur_state(latest_event_to_state(status) == "idle") | aggregate(group_by(region), count, avg, max)`
- **Credit card location anomaly**: `duration_in_cur_state(latest_event_to_state(location)) < 600`
- **User engagement**: `has_existed(playerStateChange == "play") && !has_existed(error == "fatal")`

---

## 2. Goals

### 2.1 Functional Parity

The Go port MUST produce **identical results** to the Rust original for the same inputs. This is the primary correctness requirement. Specifically:

- Given the same timeline expression and the same sequence of events, every node in the agent graph must produce the same state at every point in time.
- The same aggregation queries must return the same `{count, sum, avg}` values.
- The DSL parser must accept the same grammar and produce the same AST.

### 2.2 Runtime

- **Go 1.26** (or latest stable)
- **Temporal** as the durable execution engine (replacing Golem)
- **mise** for local development environment management
- Standalone execution — no Golem, no WASM

### 2.3 Test Strategy

1. **Unit tests**: Port all DSL parser tests (35+ test cases from `timeline-dsl/src/tests.rs`).
2. **Integration tests**: Port all four integration test suites:
   - Smoke tests (single leaf, state transitions)
   - Propagation tests (boolean logic, CIRR cascade)
   - Aggregation tests (multi-session, cross-CDN)
   - Kafka pipeline tests (optional — can substitute with direct event feeding)
3. **Cross-runtime comparison**: Run the same event sequences through both the Rust original and the Go port, compare outputs at every query point.

---

## 3. Functional Requirements

### FR-1: Timeline DSL Parser

The Go port must implement a lexer and parser for the timeline DSL that accepts the full grammar:

**Supported timeline operations**:
- `latest_event_to_state(column)` — track latest event value for a column
- `has_existed(predicate)` — has the predicate ever been true?
- `has_existed_within(predicate, duration)` — has the predicate been true within a time window?
- `duration_where(expr)` — cumulative duration where expression is true
- `duration_in_cur_state(expr)` — duration in the current state

**Supported comparison operators** (postfix on timeline expressions):
- `==`, `>`, `>=`, `<`, `<=` with string, int, float, or bool constants

**Supported boolean combinators**:
- `&&` (AND), `||` (OR), `!` (NOT)
- Parentheses for grouping
- Operator precedence: `||` < `&&` < `!` < comparison

**Aggregation** (optional suffix):
- `| aggregate(group_by(column), fn1, fn2, ...)` where `fn` is one of: `count`, `sum`, `avg`, `min`, `max`

**Predicate operators** (inside `has_existed` / `has_existed_within`):
- `==`, `>`, `<` only (no `>=`, `<=`)

### FR-2: Event Processing (Leaf Nodes)

Each leaf workflow must:
- Accept `add_event(Event)` signals containing `{time, event_columns}`
- Evaluate its configured leaf operation against the event
- Maintain a `StateDynamicsTimeLine` of computed state
- Push state changes to parent via signal: `on_child_state_changed(child_index, time, value, group_by_value)`
- Support `get_leaf_result(t)` queries returning the state at time `t`

### FR-3: Derived Processing (Internal Nodes)

Each derived workflow must:
- Accept `on_child_state_changed(child_index, time, value, group_by_value)` signals
- Recompute its operation (Comparison, Negation, And, Or, DurationWhere, DurationInCurState)
- Push changes upward if state changed
- If root with aggregation config: compute delta and push to aggregator
- Support `get_derived_result(t)` queries

### FR-4: Cross-Session Aggregation

The aggregator workflow must:
- Accept `register_session()` to increment session count
- Accept `on_delta(delta)` to accumulate running sum
- Support `get_aggregation_result()` returning `{count, sum, avg, min, max}`
- Be lazily created on first delta from a root node
- Be named `aggregator-{group_by_column}-{value}` (e.g., `aggregator-cdn-akamai`)

### FR-5: Timeline Driver (Orchestrator)

The driver workflow must:
- Accept a `TimelineOpGraph` (flat graph encoding) and optional `AggregationConfig`
- Convert to recursive form and walk depth-first pre-order
- Spawn all leaf and derived workflows with deterministic names
- Wire parent/child references
- Configure group_by columns on all leaves
- Configure aggregation on root
- Return `InitializeResult{root_agent, leaf_agents}`
- Be idempotent — calling initialize on an already-initialized session returns the same result

### FR-6: State Management

`StateDynamicsTimeLine<T>` must be implemented with these exact semantics:
- Sorted map of `{t1, t2, value}` intervals
- `add_state_dynamic_info(time, value)`: insert with interval merging (see TECHSPEC for exact algorithm)
- `get_state_at(t)`: last point with `t1 < t`
- `is_empty()`, `future_is(value)`, `last()`
- Boolean operations: `negate()`, `and()`, `or()` via `zip_with()`

### FR-7: Graph Encoding

Support both representations:
- `TimelineOpGraph` (flat, nodes array, root at index 0, children by index) — wire format
- `TimeLineOp` (recursive enum) — internal computation format
- Bidirectional conversion between the two

---

## 4. Non-Functional Requirements

### NFR-1: Development Environment
- Use `mise` for toolchain management (Go version, Temporal CLI)
- Single `make` or `mise run` command to build and test
- No external infrastructure required for unit tests (Temporal test environment for integration tests)

### NFR-2: Cross-Runtime Comparison
- Provide a test harness that can run the same event sequence through both the Rust/Golem and Go/Temporal implementations
- Compare leaf results, derived results, and aggregation results at specified query times
- This is the ultimate correctness validation

### NFR-3: Code Quality
- Idiomatic Go code with proper error handling (no panics)
- Named types for domain concepts (no raw primitives)
- Comprehensive test coverage matching or exceeding the Rust test suite

---

## 5. Test Scenarios (Port Requirements)

### Scenario 1: Smoke — Single Leaf
- Timeline: `latest_event_to_state(playerStateChange)`
- Feed: `{time: 100, playerStateChange: "play"}`
- Assert: `get_leaf_result(101)` returns `"play"`

### Scenario 2: Smoke — State Transitions
- Timeline: `latest_event_to_state(playerStateChange)`
- Feed: `{time: 10, "init"}`, `{time: 20, "buffer"}`, `{time: 30, "play"}`
- Assert: `get_leaf_result(25)` = `"buffer"`, `get_leaf_result(31)` = `"play"`

### Scenario 3: Boolean Logic Propagation
- Timeline: `has_existed(status == "active") && !has_existed(status == "error")`
- Feed `{status: "active"}` at t=10 → root should be `true` (active ∧ ¬error)
- Feed `{status: "error"}` at t=20 → root should be `false` (active ∧ ¬(¬error) = false)

### Scenario 4: CIRR Propagation
- Timeline: `duration_where(has_existed(playerStateChange=="play") && !has_existed_within(playerStateChange=="seek", 5) && latest_event_to_state(playerStateChange)=="buffer")`
- Feed `{playerStateChange: "play"}` at t=100
- Feed `{playerStateChange: "buffer"}` at t=200
- Assert: `get_derived_result(250)` on root returns a positive duration (50 seconds of CIRR)

### Scenario 5: Multi-Session Aggregation
- Timeline: `latest_event_to_state(playerStateChange) == "buffer" | aggregate(group_by(cdn), count, sum, avg)`
- Initialize 2 sessions, both with cdn="akamai"
- Feed `{playerStateChange: "init"}` then `{playerStateChange: "buffer"}` with `{cdn: "akamai"}` to each
- Assert: `get_aggregation_result("aggregator-cdn-akamai")` has `count=2`

### Scenario 6: DSL Parser (35+ test cases)
Port all parser tests from `common-rust/timeline-dsl/src/tests.rs`:
- Simple expressions: `latest_event_to_state`, `has_existed`, `has_existed_within`
- Comparisons: `==`, `>`, `>=`, `<`, `<=` with string/int/float/bool
- Boolean: `&&`, `||`, `!`, parentheses, precedence
- Aggregation: single function, multiple functions
- Complex: CIRR with aggregation, chained and/or, double negation
- Errors: unterminated string, missing rparen, unexpected token, bad predicate op

---

## 6. Out of Scope (for initial port)

- Dashboard / UI
- Kafka integration (test with direct event feeding)
- Metric Registry service
- Compute reuse across workflows (future design)
- Min/Max aggregation tracking (not implemented in original)
- Compound predicates (And/Or) in event predicates (not yet supported in API encoding)
