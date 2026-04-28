# Architecture — Durable Timeline Analytics (Go + Temporal Port)

This document describes the architecture of the original Rust/Golem system and how it maps to a Go + Temporal implementation.

---

## 1. System Overview

The system implements **Timeline Analytics** — a composable DSL for expressing temporal queries over event streams. Each node in a timeline expression tree becomes a durable worker (Golem agent → Temporal workflow/activity). Events flow **bottom-up**: leaf nodes ingest raw events, compute local state, and push state changes upward through derived nodes to a root. Point-in-time queries are local lookups on precomputed state — no cascading RPC at query time.

### Core Concept: Push-Based Agent Graph

```
                          ┌─────────────────┐
                          │ TimelineDriver  │  Walks DSL tree, spawns workers,
                          │ (orchestrator)  │  wires parent/child refs
                          └────────┬────────┘
                                   │ spawns & wires
             ┌─────────────────────┼─────────────────────┐
             ▼                     ▼                     ▼
  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐
  │ EventProcessor   │  │ EventProcessor   │  │ EventProcessor   │
  │ (leaf: has_exist)│  │ (leaf: latest)   │  │ (leaf: within)   │
  └────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘
           │ push                │ push                │ push
           ▼                     ▼                     ▼
  ┌──────────────────┐  ┌──────────────────┐
  │TimelineProcessor │  │TimelineProcessor │  Receives on_child_state_changed,
  │ (derived: And)   │  │ (derived: EqualTo│  recomputes, pushes to parent
  └────────┬─────────┘  └────────┬─────────┘
           │ push                │ push
           └─────────┬───────────┘
                     ▼
          ┌──────────────────┐       ┌──────────────────┐
          │TimelineProcessor │──────▶│   Aggregator     │  Root pushes deltas
          │ (root: Duration) │ delta │ (cross-session)  │  to aggregator
          └──────────────────┘       └──────────────────┘
```

---

## 2. Four Agent Types

### 2.1 EventProcessor (Leaf Node)

**Purpose**: Ingest raw events, evaluate a leaf operation, maintain local state, push changes to parent.

**Leaf Operations**:
| Operation | Behavior |
|---|---|
| `LatestEventToState(column)` | Track the latest event value for a named column. State = the most recent value. |
| `TlHasExisted(predicate)` | Has the predicate ever been true? Once true, stays true forever (cumulative OR). |
| `TlHasExistedWithin(predicate, duration)` | Has the predicate been true within the last `duration` time units? True flips to false after the window expires. |

**State**: Each leaf maintains a `StateDynamicsTimeLine<T>` — a sorted map (BTreeMap) of time-bounded state intervals.

**Push behavior**: After computing new state from an event, if the state changed, call `parent.on_child_state_changed(child_index, time, value, group_by_value)`.

**Critical behavioral details for `TlHasExisted`**:
- On first event: if predicate matches → state = true, push true. If not → state = false, push false.
- Once state is true and extends to future (`future_is(true)`), no further processing needed — the predicate was already satisfied.
- Only evaluates the predicate when the timeline is empty OR the current future state is false.

**Critical behavioral details for `TlHasExistedWithin`**:
- When predicate matches at time `t`: insert `true` at time `t`, AND insert `false` at time `t + duration` (the window expiry).
- This means a single matching event produces TWO parent notifications: one for `true` at `t`, one for `false` at `t + duration`.
- Only evaluates when timeline is empty OR current future state is false.

### 2.2 TimelineProcessor (Derived Node)

**Purpose**: Receive state changes from children, recompute its own state, push upward.

**Derived Operations**:
| Operation | Inputs | Behavior |
|---|---|---|
| `Comparison(op, constant)` | 1 child | Compare child value against constant (EqualTo, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual) → bool |
| `Negation` | 1 child | Negate a boolean child → bool |
| `And` | 2 children | Boolean AND. Uses `get_state_at(time+1)` to read sibling's latest state. |
| `Or` | 2 children | Boolean OR. Same sibling lookup as And. |
| `DurationWhere` | 1 child (bool) | Cumulative duration where child is true. Uses `DurationState`: `Climbing{base, since}` while true, `Flat{value}` while false. Query at time t returns `base + (t - since)`. |
| `DurationInCurState` | 1 child | Duration in current state. Resets to `Climbing{base:0, since:t}` on every state change. |

**State**: Maintains `left_child_state`, `right_child_state`, `result_state` (all `StateDynamicsTimeLine<GolemEventValue>`), plus optional `DurationState`.

**Push behavior**: Same as leaf — if result changed, push to parent. If root with aggregation config, push delta to Aggregator.

**And/Or sibling state lookup**: When `And` or `Or` receives a child state change at time `t`:
1. Store the value in `left_child_state` (if child_index=0) or `right_child_state` (if child_index=1).
2. Read both sides via `get_state_at(time + 1)` to get the latest state including the just-written value.
3. If both sides have bool values, compute the boolean result.
4. The `time+1` trick ensures the newly written value is visible (since `get_state_at` returns the last point with `t1 < t`).

### 2.3 Aggregator (Cross-Session Accumulator)

**Purpose**: Accumulate metrics across multiple independent sessions.

**State**: `count: uint64`, `sum: float64`. One aggregator per `(group_by_column, group_by_value)` pair.

**Operations**:
- `initialize_aggregator(aggregations)` → store which aggregations to compute
- `register_session()` → increment count
- `on_delta(delta: float64)` → `sum += delta`
- `get_aggregation_result()` → `{count, sum, avg: sum/count, min: None, max: None}`

**Naming**: `aggregator-{group_by_column}-{group_by_value}` (e.g., `aggregator-cdn-akamai`)

**Lazy creation**: The aggregator is created on first delta push by the root TimelineProcessor. First call does: `initialize_aggregator()`, `register_session()`, then `on_delta()`.

### 2.4 TimelineDriver (Orchestrator)

**Purpose**: Walk a timeline expression tree, spawn all leaf and derived workers, wire parent/child refs, configure aggregation.

**Algorithm** (recursive, depth-first pre-order):
1. Convert `TimelineOpGraph` (flat, non-recursive) to `TimeLineOp` (recursive) via `to_recursive()`.
2. Call `setup_node(op, counter)` recursively. Counter starts at 0, increments before each node.
3. For leaf nodes: create `EventProcessor` with name `{session}-{op-kebab}-{counter}`, call `initialize_leaf(operation)`, add to `leaves` list.
4. For derived nodes: recursively set up children first, then create `TimelineProcessor` with name `{session}-{op-kebab}-{counter}`, call `initialize_derived(operation, children)`.
5. Wire `set_parent(ParentRef{agent_name, child_index})` on each child pointing to its parent.
6. After full tree setup: if aggregation configured, call `set_group_by_column(column)` on every leaf, call `set_aggregation(config)` on root.
7. Return `InitializeResult{root_agent, leaf_agents}`.

**Agent Naming Convention**: `{session_id}-{operation_kebab}-{counter}`

Example for CIRR with session "sess-42":
```
sess-42-duration-where-1     (counter=1, root)
sess-42-and-2                (counter=2)
sess-42-and-3                (counter=3)
sess-42-has-existed-4        (counter=4, LEAF)
sess-42-not-5                (counter=5)
sess-42-has-existed-within-6 (counter=6, LEAF)
sess-42-equal-to-7           (counter=7)
sess-42-latest-event-to-state-8 (counter=8, LEAF)
```

---

## 3. Data Types

### 3.1 EventValue
```
enum EventValue {
    StringValue(string)
    IntValue(int64)
    FloatValue(float64)
    BoolValue(bool)
}
```

Ordering: `StringValue < IntValue < FloatValue < BoolValue` by variant; within variant, natural ordering.

### 3.2 Event
```
struct Event {
    time: uint64
    event: []Tuple{Key: string, Value: EventValue}  // ordered key-value pairs
}
```

### 3.3 EventPredicate
```
struct EventPredicate {
    col_name: string
    value: EventValue
    op: PredicateOp  // Equal | GreaterThan | LessThan
}
```

Evaluation: look up `col_name` in the event's column map, compare with `value` using `op`. Returns false if column is missing.

### 3.4 StateDynamicsTimeLine\<T\>

Core data structure: a sorted map keyed by `t1` (start time) of `StateDynamicsTimeLinePoint<T>`:
```
struct StateDynamicsTimeLinePoint<T> {
    t1: uint64           // interval start (inclusive)
    t2: *uint64          // interval end (exclusive), nil = extends to future
    value: T
}
```

**Key operations**:
- `add_state_dynamic_info(time, value)` — insert a new state point, adjusting neighbor intervals:
  - If previous point exists with different value: truncate previous to end at `time`, insert new point inheriting previous's end.
  - If previous point exists with same value and ended before `time`: extend previous to cover gap.
  - If next point exists with same value: merge by replacing next's start with `time`.
  - If no neighbors: insert with `t2 = nil` (extends to future).
- `get_state_at(t)` — find the last point with `t1 < t` (range query `..t`, take last).
- `is_empty()` — no points in the map.
- `future_is(value)` — last point has `t2 == nil` AND `value == value`.
- `last()` — the point with the highest `t1`.
- `contains(t)` on a point — `t >= t1 && (t2 == nil || t < t2)`.

### 3.5 DurationState
```
enum DurationState {
    Climbing { base: uint64, since: uint64 }  // counter = base + (now - since)
    Flat { value: uint64 }                     // counter stopped at this value
}
```

Transition rules in `DurationWhere`:
- Child becomes true → `Climbing{base: current_count, since: time}`
- Child becomes false → `Flat{value: current_count}`
- Query at time t: if Climbing and `t >= since` → `base + (t - since)`, if Flat → `value`

### 3.6 TimelineOpGraph (Wire Format)

Flat, non-recursive encoding. `nodes[0]` is the root. Children referenced by `NodeIndex` (int64).
```
enum TimelineNode {
    Comparison(CompareOp, NodeIndex, EventValue)
    Negation(NodeIndex)
    And(NodeIndex, NodeIndex)
    Or(NodeIndex, NodeIndex)
    TlHasExisted(EventPredicate)
    TlHasExistedWithin(EventPredicate, uint64)
    TlLatestEventToState(string)
    TlDurationWhere(NodeIndex)
    TlDurationInCurState(NodeIndex)
}
```

Conversion to recursive form: `build_node(0)` → recursively resolve children by index.

### 3.7 Configuration Types
```
struct AggregationConfig {
    group_by_column: string
    aggregations: []AggregationType  // Count | Sum | Avg | Min | Max
}

struct InitializeResult {
    root_agent: string
    leaf_agents: []string
}

struct AggregationResult {
    count: uint64
    sum: float64
    avg: float64
    min: *float64
    max: *float64
}

struct ParentRef {
    agent_name: string
    child_index: uint32  // 0 for left/only child, 1 for right child
}

struct ChildAgentRef {
    agent_name: string
    is_leaf: bool
}
```

---

## 4. Data Flow

1. **Event ingestion**: Raw event arrives at an `EventProcessor` leaf via `add_event(Event)`.
2. **Leaf computation**: Leaf evaluates its operation, records result in its `StateDynamicsTimeLine`.
3. **Parent notification**: If state changed, leaf calls `on_child_state_changed(child_index, time, value, group_by_value)` on parent.
4. **Derived recomputation**: Parent updates its own state and, if changed, pushes upward. Cascade continues to root.
5. **Aggregator update**: Root computes delta, calls `on_delta(delta)` on Aggregator (lazily created).
6. **Query**: `get_leaf_result(t)` or `get_derived_result(t)` performs local point lookup on precomputed state.

### Aggregator Delta Calculation

```
new_value = event_value_to_f64(value)  // IntValue→float64, FloatValue→float64, BoolValue→1.0/0.0
delta = new_value - last_aggregated_value (default 0.0)
last_aggregated_value = new_value
if delta == 0.0: skip
if first_call:
    aggregator_name = "aggregator-{group_by_column}-{group_by_value_as_string}"
    aggregator.initialize_aggregator(aggregations)
    aggregator.register_session()
aggregator.on_delta(delta)
```

---

## 5. DSL Grammar

```
query       = expr ("|" aggregate)?
expr        = or_expr
or_expr     = and_expr ("||" and_expr)*
and_expr    = unary ("&&" unary)*
unary       = "!" unary | postfix
postfix     = primary (cmp_op value)?
primary     = "(" expr ")"
            | "latest_event_to_state" "(" column ")"
            | "has_existed" "(" predicate ")"
            | "has_existed_within" "(" predicate "," int ")"
            | "duration_where" "(" expr ")"
            | "duration_in_cur_state" "(" expr ")"
predicate   = ident pred_op value
pred_op     = "==" | ">" | "<"
cmp_op      = "==" | ">" | ">=" | "<" | "<="
value       = string | int | float | bool
aggregate   = "aggregate" "(" "group_by" "(" ident ")" "," agg_fn ("," agg_fn)* ")"
agg_fn      = "count" | "sum" | "avg" | "min" | "max"
```

**Operator precedence** (low to high): `||`, `&&`, `!`, comparison (`==`, `>`, `>=`, `<`, `<=`).
**Associativity**: Left-associative for `&&` and `||`.

---

## 6. Mapping to Go + Temporal

| Rust/Golem Concept | Go/Temporal Equivalent |
|---|---|
| Golem Agent (worker) | Temporal Workflow with durable state |
| `#[agent_definition]` trait | Go interface |
| `#[agent_implementation]` struct | Go struct implementing the interface |
| `AgentClient::get(name)` → RPC call | Temporal `SignalWorkflow` or `SignalChildWorkflow` |
| `on_child_state_changed()` async call | Temporal signal to parent workflow |
| `get_leaf_result()` / `get_derived_result()` | Temporal workflow query |
| `add_event()` | Temporal signal to leaf workflow |
| Golem durable state (auto-persisted) | Temporal workflow state (replayed on recovery) |
| `initialize_timeline()` → spawns agents | Temporal workflow that starts child workflows |
| `StateDynamicsTimeLine<T>` (BTreeMap) | Go sorted map (e.g., `btree.Map` or sorted slice) |
| `TimelineOpGraph` (flat graph) | Same struct in Go |

### Key Temporal Design Decisions

1. **Each agent → one long-running Temporal workflow** with signal handlers for `add_event`, `on_child_state_changed`, and query handlers for `get_leaf_result`, `get_derived_result`.
2. **TimelineDriver** → a Temporal workflow that starts child workflows and signals their initialization.
3. **Parent notification** → Temporal signal from child workflow to parent workflow.
4. **Aggregator** → a long-running Temporal workflow receiving `on_delta` signals and supporting `get_aggregation_result` queries.
5. **State** is maintained in workflow local variables (automatically durable via Temporal replay).

---

## 7. Proposed Go Directory Structure

```
go/
├── cmd/
│   └── worker/              # Temporal worker binary (main.go)
├── internal/
│   ├── domain/              # Core domain types
│   │   ├── event_value.go   # EventValue enum
│   │   ├── event.go         # Event, EventPredicate, GolemEvent
│   │   ├── timeline_op.go   # TimeLineOp (recursive), TimelineOpGraph (flat)
│   │   ├── state_timeline.go # StateDynamicsTimeLine, StateDynamicsTimeLinePoint
│   │   └── types.go         # ParentRef, ChildAgentRef, AggregationConfig, etc.
│   ├── dsl/                 # DSL lexer + parser
│   │   ├── lexer.go
│   │   ├── parser.go
│   │   └── dsl_test.go
│   └── workflows/           # Temporal workflows
│       ├── event_processor.go
│       ├── timeline_processor.go
│       ├── aggregator.go
│       └── timeline_driver.go
├── test/
│   ├── smoke_test.go
│   ├── propagation_test.go
│   ├── aggregation_test.go
│   └── comparison_test.go   # Cross-runtime comparison with Rust original
├── go.mod
├── go.sum
├── Makefile
└── mise.toml
```
