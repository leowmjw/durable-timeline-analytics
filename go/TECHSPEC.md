# Technical Specification — Go + Temporal Port

This document provides the exact behavioral specifications and test scenarios needed to verify the Go port produces identical results to the Rust original.

---

## 1. StateDynamicsTimeLine — Exact Semantics

This is the most critical data structure. It's a sorted map of time-bounded state intervals.

### 1.1 Data Structure

```go
type StateDynamicsTimeLine[T comparable] struct {
    Points *btree.Map[uint64, StateDynamicsTimeLinePoint[T]]  // sorted by t1
}

type StateDynamicsTimeLinePoint[T any] struct {
    T1    uint64   // interval start (inclusive)
    T2    *uint64  // interval end (exclusive); nil = extends to future
    Value T
}
```

### 1.2 add_state_dynamic_info(new_time, value) — Exact Algorithm

Find the previous point (`prev`: last point with `t1 < new_time`) and next point (`next`: first point with `t1 >= new_time`).

**Case 1: prev exists AND next exists** (new time falls between existing intervals)
- If `prev.value != value`: truncate prev to end at `new_time`, insert new point inheriting prev's original `t2`.
  ```
  prev.t2 = new_time
  insert {t1: new_time, t2: prev_original_t2, value: value}
  ```
- If `prev.value == value`: no change (value already covers this interval).

**Case 2: prev exists, no next** (new time falls after all existing intervals)
- If `prev.value == value` AND `prev.t2 != nil` AND `prev.t2 < new_time`: extend prev to future.
  ```
  prev.t2 = nil  (extends prev to cover the gap)
  ```
- If `prev.value != value`:
  - If prev covers `new_time` (prev.t2 is nil or `prev.t2 > new_time`): split prev.
    ```
    prev.t2 = new_time
    insert {t1: new_time, t2: prev_original_t2, value: value}
    ```
  - If prev ended before `new_time` (prev.t2 < new_time): insert after gap.
    ```
    prev.t2 = new_time  (truncate prev to end at new_time instead of gap)
    insert {t1: new_time, t2: nil, value: value}
    ```

**Case 3: no prev, next exists** (new time falls before all existing intervals)
- If `next.value == value`: merge by replacing next with new start time.
  ```
  remove next
  insert {t1: new_time, t2: next.t2, value: value}
  ```
- If `next.value != value`: insert before next.
  ```
  insert {t1: new_time, t2: next.t1, value: value}
  ```

**Case 4: no prev, no next** (empty timeline)
```
insert {t1: new_time, t2: nil, value: value}
```

### 1.3 get_state_at(t) — Point Lookup

Return the last point where `t1 < t`. This means:
- A point at `t1=100` is visible at `t=101` but NOT at `t=100`.
- This is a strict less-than lookup.

### 1.4 contains(t) — Point Containment

```
t >= t1 && (t2 == nil || t < t2)
```

### 1.5 future_is(value)

```
last point exists AND last.t2 == nil AND last.value == value
```

### 1.6 Boolean Operations

`negate()`: map each point's bool value to its inverse.

`and(other)` / `or(other)`: Use `zip_with(other, f)` where `f` is `&&` / `||`.

`zip_with(other, f)`: Aligns two timelines by finding all boundary points across both, then applies `f` at each aligned interval. This is the most complex operation — it handles:
- Points in one timeline but not the other
- Overlapping intervals that need splitting
- Merging aligned intervals using `f(left_value, right_value)`

---

## 2. EventProcessor (Leaf) — Exact Behavior

### 2.1 LatestEventToState(column_name)

```
on add_event(event):
    val = event.get(column_name)
    if val is None: return  // column not in event, skip
    previous = latest_event_state.get_state_at(event.time)
    latest_event_state.add_state_dynamic_info(event.time, val)
    if previous != val:
        notify_parent(time, val, group_by_value)
```

### 2.2 TlHasExisted(predicate)

```
on add_event(event):
    if timeline is NOT empty AND future_is(true):
        return  // already permanently true, nothing to do

    if timeline is empty OR future_is(false):
        result = predicate.evaluate(event)
        if result == true:
            tl_has_existed_state.add_state_dynamic_info(time, true)
            notify_parent(time, BoolValue(true), group_by_value)
        else if NOT future_is(false):  // first event, need to establish initial state
            tl_has_existed_state.add_state_dynamic_info(time, false)
            notify_parent(time, BoolValue(false), group_by_value)
```

**Important**: Once `has_existed` becomes true, it stays true forever. The `future_is(true)` check prevents unnecessary re-evaluation.

### 2.3 TlHasExistedWithin(predicate, within_duration)

```
on add_event(event):
    if timeline is NOT empty AND future_is(false):
        // Window expired, check again
    if timeline is empty OR future_is(false):
        result = predicate.evaluate(event)
        if result == true:
            // Set true NOW
            tl_has_existed_within_state.add_state_dynamic_info(time, true)
            notify_parent(time, BoolValue(true), group_by_value)
            // Set false AFTER window expires
            tl_has_existed_within_state.add_state_dynamic_info(time + within_duration, false)
            notify_parent(time + within_duration, BoolValue(false), group_by_value)
        else if NOT future_is(false):
            tl_has_existed_within_state.add_state_dynamic_info(time, false)
            notify_parent(time, BoolValue(false), group_by_value)
```

**Critical**: A single matching event produces TWO parent notifications at two different times.

### 2.4 get_leaf_result(t1)

```
match operation:
    LatestEventToState: return latest_event_state.get_state_at(t1)
    TlHasExisted: return tl_has_existed_state.get_state_at(t1)
    TlHasExistedWithin: return tl_has_existed_within_state.get_state_at(t1)
```

Returns `{t1, t2, value}` of the matching interval, or empty if no state at that time.

---

## 3. TimelineProcessor (Derived) — Exact Behavior

### 3.1 Comparison(compare_op, constant)

```
on on_child_state_changed(child_index, time, value, group_by_value):
    result = compare(value, constant, compare_op)  // ==, >, >=, <, <=
    prev = result_state.get_state_at(time)
    new_val = BoolValue(result)
    if prev != new_val:
        result_state.add_state_dynamic_info(time, new_val)
        notify_parent(time, BoolValue(result), group_by_value)
        notify_aggregator(...)
```

### 3.2 Negation

```
on on_child_state_changed(child_index, time, value, group_by_value):
    if value is not bool: return
    negated = !value
    prev = result_state.get_state_at(time)
    if prev != BoolValue(negated):
        result_state.add_state_dynamic_info(time, BoolValue(negated))
        notify_parent(time, BoolValue(negated), group_by_value)
        notify_aggregator(...)
```

### 3.3 And / Or

```
on on_child_state_changed(child_index, time, value, group_by_value):
    if child_index == 0:
        left_child_state.add_state_dynamic_info(time, value)
    else:
        right_child_state.add_state_dynamic_info(time, value)

    // KEY: use time+1 to see the just-written value
    left_val = left_child_state.get_state_at(time + 1)?.get_bool()
    right_val = right_child_state.get_state_at(time + 1)?.get_bool()

    if left_val AND right_val both exist:
        result = left_val && right_val  // or || for Or
        prev = result_state.get_state_at(time)
        if prev != BoolValue(result):
            result_state.add_state_dynamic_info(time, BoolValue(result))
            notify_parent(time, BoolValue(result), group_by_value)
            notify_aggregator(...)
```

**KEY DETAIL**: The `time+1` lookup is essential. Since `get_state_at(t)` returns the last point with `t1 < t`, using `time+1` ensures the value just written at `time` is visible.

### 3.4 DurationWhere

```
on on_child_state_changed(child_index, time, value, group_by_value):
    if value is not bool: return

    current_count = match duration_state:
        Climbing{base, since} → base + (time - since)
        Flat{value} → value
        None → 0

    if value == true:
        duration_state = Climbing{base: current_count, since: time}
    else:
        duration_state = Flat{value: current_count}

    result_state.add_state_dynamic_info(time, IntValue(current_count))
    notify_parent(time, IntValue(current_count), group_by_value)
    notify_aggregator(...)
```

**get_derived_result(t1) for DurationWhere**:
```
if duration_state exists:
    match duration_state:
        Climbing{base, since}:
            if t1 >= since: return IntValue(base + (t1 - since))
            else: return IntValue(base)
        Flat{value}: return IntValue(value)
else:
    return result_state.get_state_at(t1)
```

### 3.5 DurationInCurState

```
on on_child_state_changed(child_index, time, value, group_by_value):
    // Reset on every state change
    duration_state = Climbing{base: 0, since: time}
    result_state.add_state_dynamic_info(time, IntValue(0))
    notify_parent(time, IntValue(0), group_by_value)
    notify_aggregator(...)
```

Query returns `0 + (t - since)` = elapsed time since last state change.

---

## 4. Aggregator — Exact Behavior

```go
type Aggregator struct {
    aggregations []AggregationType
    count        uint64
    sum          float64
}

func (a *Aggregator) RegisterSession() { a.count++ }
func (a *Aggregator) OnDelta(delta float64) { a.sum += delta }
func (a *Aggregator) GetResult() AggregationResult {
    avg := 0.0
    if a.count > 0 { avg = a.sum / float64(a.count) }
    return AggregationResult{Count: a.count, Sum: a.sum, Avg: avg, Min: nil, Max: nil}
}
```

### Aggregator Delta Calculation (in notify_aggregator helper)

```
if no aggregation config: return
if no group_by_value: return
new_value = event_value_to_f64(value)  // Int→f64, Float→f64, Bool→1.0/0.0, String→skip
if new_value is None: return

delta = new_value - last_aggregated_value (default 0.0)
last_aggregated_value = new_value
if delta == 0.0: return

if first_call:
    aggregator_name = "aggregator-{group_by_column}-{event_value_to_string(group_by_value)}"
    aggregator.initialize_aggregator(aggregations)
    aggregator.register_session()
aggregator.on_delta(delta)
```

---

## 5. TimelineDriver — Exact Setup Algorithm

```
func (d *Driver) InitializeTimeline(graph TimelineOpGraph, agg *AggregationConfig) (InitializeResult, error):
    recursive_op = graph.ToRecursive()
    counter = 0
    leaves = []
    result = setup_node(recursive_op, &counter, &leaves)

    if agg != nil:
        for each leaf_name in leaves:
            event_processor(leaf_name).set_group_by_column(agg.group_by_column)
        if !result.is_leaf:
            timeline_processor(result.agent_name).set_aggregation(agg)

    return InitializeResult{root_agent: result.agent_name, leaf_agents: leaves}
```

### setup_node — Pre-Order DFS

```
func setup_node(op, counter, leaves) → (SetupResult, error):
    counter++
    n = counter

    match op:
        TlLatestEventToState(col):
            name = "{session}-latest-event-to-state-{n}"
            event_processor(name).initialize_leaf(LatestEventToState(col))
            leaves.append(name)
            return SetupResult{name, is_leaf: true}

        TlHasExisted(pred):
            name = "{session}-has-existed-{n}"
            event_processor(name).initialize_leaf(TlHasExisted(pred))
            leaves.append(name)
            return SetupResult{name, is_leaf: true}

        TlHasExistedWithin(pred, dur):
            name = "{session}-has-existed-within-{n}"
            event_processor(name).initialize_leaf(TlHasExistedWithin(pred, dur))
            leaves.append(name)
            return SetupResult{name, is_leaf: true}

        EqualTo(child, val):
            name = "{session}-equal-to-{n}"
            child_result = setup_node(child, counter, leaves)
            timeline_processor(name).initialize_derived(Comparison(EqualTo, val), [child_result])
            set_child_parent(child_result, name, 0)
            return SetupResult{name, is_leaf: false}

        // Similarly for GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual...

        Not(child):
            name = "{session}-not-{n}"
            child_result = setup_node(child, counter, leaves)
            timeline_processor(name).initialize_derived(Negation, [child_result])
            set_child_parent(child_result, name, 0)
            return SetupResult{name, is_leaf: false}

        And(left, right):
            name = "{session}-and-{n}"
            left_result = setup_node(left, counter, leaves)
            right_result = setup_node(right, counter, leaves)
            timeline_processor(name).initialize_derived(And, [left_result, right_result])
            set_child_parent(left_result, name, 0)
            set_child_parent(right_result, name, 1)
            return SetupResult{name, is_leaf: false}

        // Similarly for Or, DurationWhere, DurationInCurState...
```

### Operation name mapping for agent names:
| Operation | Kebab name |
|---|---|
| TlLatestEventToState | `latest-event-to-state` |
| TlHasExisted | `has-existed` |
| TlHasExistedWithin | `has-existed-within` |
| EqualTo | `equal-to` |
| GreaterThan | `greater-than` |
| GreaterThanOrEqual | `greater-than-or-equal` |
| LessThan | `less-than` |
| LessThanOrEqual | `less-than-or-equal` |
| Not | `not` |
| And | `and` |
| Or | `or` |
| TlDurationWhere | `duration-where` |
| TlDurationInCurState | `duration-in-cur-state` |

---

## 6. DSL Parser — Test Cases

### 6.1 Simple Expressions

| Input | Expected AST (Display) |
|---|---|
| `latest_event_to_state(status)` | `TlLatestEventToState(status)` |
| `has_existed(status == "active")` | `TlHasExisted(status == active)` |
| `has_existed(score > 100)` | `TlHasExisted(score > 100)` |
| `has_existed(health < 50)` | `TlHasExisted(health < 50)` |
| `has_existed_within(status == "error", 3600)` | `TlHasExistedWithin(status == error, 3600)` |

### 6.2 Comparisons

| Input | Expected AST |
|---|---|
| `latest_event_to_state(status) == "active"` | `EqualTo(TlLatestEventToState(status), active)` |
| `latest_event_to_state(score) > 100` | `GreaterThan(TlLatestEventToState(score), 100)` |
| `latest_event_to_state(score) >= 50` | `GreaterThanOrEqual(TlLatestEventToState(score), 50)` |
| `latest_event_to_state(health) < 20` | `LessThan(TlLatestEventToState(health), 20)` |
| `latest_event_to_state(health) <= 0` | `LessThanOrEqual(TlLatestEventToState(health), 0)` |
| `latest_event_to_state(temperature) > 36.5` | `GreaterThan(TlLatestEventToState(temperature), 36.5)` |
| `latest_event_to_state(flag) == true` | `EqualTo(TlLatestEventToState(flag), true)` |

### 6.3 Boolean Logic

| Input | Expected AST |
|---|---|
| `!has_existed(error == "fatal")` | `Not(TlHasExisted(error == fatal))` |
| `has_existed(status == "active") && has_existed(region == "us")` | `And(TlHasExisted(status == active), TlHasExisted(region == us))` |
| `has_existed(status == "a") \|\| has_existed(status == "b")` | `Or(TlHasExisted(status == a), TlHasExisted(status == b))` |
| `!!has_existed(x == 1)` | `Not(Not(TlHasExisted(x == 1)))` |

### 6.4 Operator Precedence

| Input | Expected AST |
|---|---|
| `has_existed(x == 1) \|\| has_existed(y == 2) && has_existed(z == 3)` | `Or(TlHasExisted(x == 1), And(TlHasExisted(y == 2), TlHasExisted(z == 3)))` |
| `(has_existed(x == 1) \|\| has_existed(y == 2)) && has_existed(z == 3)` | `And(Or(TlHasExisted(x == 1), TlHasExisted(y == 2)), TlHasExisted(z == 3))` |

### 6.5 Chained Operators

| Input | Expected AST |
|---|---|
| `has_existed(a == 1) && has_existed(b == 2) && has_existed(c == 3)` | `And(And(TlHasExisted(a == 1), TlHasExisted(b == 2)), TlHasExisted(c == 3))` |
| `has_existed(a == 1) \|\| has_existed(b == 2) \|\| has_existed(c == 3)` | `Or(Or(TlHasExisted(a == 1), TlHasExisted(b == 2)), TlHasExisted(c == 3))` |

### 6.6 Duration Operations

| Input | Expected AST |
|---|---|
| `duration_where(has_existed(online == true))` | `TlDurationWhere(TlHasExisted(online == true))` |
| `duration_in_cur_state(latest_event_to_state(status) == "idle")` | `TlDurationInCurState(EqualTo(TlLatestEventToState(status), idle))` |

### 6.7 Aggregation

| Input | Expected aggregation |
|---|---|
| `has_existed(status == "active") \| aggregate(group_by(region), count)` | group_by="region", functions=[Count] |
| `latest_event_to_state(score) > 0 \| aggregate(group_by(team), count, sum, avg, min, max)` | group_by="team", functions=[Count, Sum, Avg, Min, Max] |

### 6.8 Complex Expressions

| Input | Expected AST |
|---|---|
| `duration_where(has_existed(status == "active") && !has_existed_within(error > 0, 300))` | `TlDurationWhere(And(TlHasExisted(status == active), Not(TlHasExistedWithin(error > 0, 300))))` |
| CIRR query (see below) | `TlDurationWhere(And(And(TlHasExisted(playerStateChange == play), Not(TlHasExistedWithin(playerStateChange == seek, 5))), EqualTo(TlLatestEventToState(playerStateChange), buffer)))` |

**Full CIRR query**:
```
duration_where(
    has_existed(playerStateChange == "play")
    && !has_existed_within(playerStateChange == "seek", 5)
    && latest_event_to_state(playerStateChange) == "buffer"
)
```

**Full CIRR with aggregation**:
```
duration_where(
    has_existed(playerStateChange == "play")
    && !has_existed_within(playerStateChange == "seek", 5)
    && latest_event_to_state(playerStateChange) == "buffer"
) | aggregate(group_by(cdn), count, sum, avg)
```
→ aggregation: group_by="cdn", functions=[Count, Sum, Avg]

### 6.9 Error Cases

| Input | Expected error contains |
|---|---|
| `latest_event_to_state("oops)` | "unterminated string" |
| `latest_event_to_state("x"` | "expected ')'" |
| `42` | "unexpected token" |
| `has_existed(col >= 1)` | "predicate operator" |
| `has_existed(x == 1) \| aggregate(group_by(r), )` | "aggregation function" |

### 6.10 Whitespace Insensitivity
```
has_existed(x==1)&&has_existed(y==2)
```
must parse identically to:
```
has_existed(x == 1) && has_existed(y == 2)
```

---

## 7. Integration Test Scenarios

### 7.1 Smoke — Single Leaf

```
Timeline: {nodes: [tl-latest-event-to-state("playerStateChange")]}
Session: "smoke-leaf-1"

Step 1: Initialize timeline → driver creates "smoke-leaf-1-latest-event-to-state-1"
Step 2: add_event to leaf: {time: 100, event: [("playerStateChange", "play")]}
Step 3: get_leaf_result(101) on leaf → result contains "play"
```

### 7.2 Smoke — State Transitions

```
Timeline: {nodes: [tl-latest-event-to-state("playerStateChange")]}
Session: "smoke-trans-1"

Step 1: Initialize
Step 2: add_event {time: 10, playerStateChange: "init"}
Step 3: add_event {time: 20, playerStateChange: "buffer"}
Step 4: add_event {time: 30, playerStateChange: "play"}
Step 5: get_leaf_result(25) → "buffer"
Step 6: get_leaf_result(31) → "play"
```

### 7.3 Boolean Logic Propagation

```
Timeline: And(TlHasExisted(status=="active"), Not(TlHasExisted(status=="error")))

Graph: {nodes: [and(1,2), tl-has-existed(status=="active"), negation(3), tl-has-existed(status=="error")]}
Session: "prop-bool-1"

Agents created:
  and-1 (TimelineProcessor, root)
  has-existed-2 (EventProcessor, leaf)
  not-3 (TimelineProcessor)
  has-existed-4 (EventProcessor, leaf)

Step 1: add_event to BOTH leaves: {time: 10, status: "active"}
  - has-existed-2: predicate matches → true → pushes to and-1
  - has-existed-4: predicate does NOT match ("active" != "error") → false → pushes to not-3
  - not-3: negates false → true → pushes to and-1
  - and-1: left=true, right=true → true
Step 2: get_derived_result(11) on and-1 → true

Step 3: add_event to BOTH leaves: {time: 20, status: "error"}
  - has-existed-2: already true (future_is(true)), no change
  - has-existed-4: predicate matches → true → pushes to not-3
  - not-3: negates true → false → pushes to and-1
  - and-1: left=true, right=false → false
Step 4: get_derived_result(21) on and-1 → false
```

### 7.4 CIRR Propagation

```
Timeline: DurationWhere(And(And(HasExisted(psc=="play"), Not(HasExistedWithin(psc=="seek",5))), EqualTo(LatestEventToState(psc),"buffer")))

Graph: {nodes: [
  tl-duration-where(1),
  and(2,6),
  and(3,4),
  tl-has-existed(psc=="play"),
  negation(5),
  tl-has-existed-within(psc=="seek", 5),
  comparison(equal-to, 7, "buffer"),
  tl-latest-event-to-state("playerStateChange")
]}
Session: "prop-cirr-1"

Agents: duration-where-1 (root), and-2, and-3, has-existed-4 (leaf), not-5, has-existed-within-6 (leaf), equal-to-7, latest-event-to-state-8 (leaf)

Step 1: add_event to ALL 3 leaves: {time: 100, playerStateChange: "play"}
  - has-existed-4: play matches → true
  - has-existed-within-6: play != seek → false initially (or no match)
  - latest-event-to-state-8: state = "play"
  - Cascade: and-3=true∧true, equal-to-7=("play"=="buffer")=false, and-2=true∧false=false
  - duration-where-1: false → Flat{0}

Step 2: add_event to ALL 3 leaves: {time: 200, playerStateChange: "buffer"}
  - latest-event-to-state-8: state = "buffer"
  - equal-to-7: "buffer"=="buffer" → true
  - and-2: true∧true → true
  - duration-where-1: true → Climbing{base: 0, since: 200}

Step 3: get_derived_result(250) on duration-where-1
  → Climbing: 0 + (250 - 200) = 50 → IntValue(50)
```

### 7.5 Multi-Session Aggregation

```
Timeline: EqualTo(LatestEventToState("playerStateChange"), "buffer")
  with aggregation: group_by(cdn), count, sum, avg

Graph: {nodes: [comparison(equal-to, 1, "buffer"), tl-latest-event-to-state("playerStateChange")]}
Aggregation: {group_by_column: "cdn", aggregations: [count, sum, avg]}

Sessions: "agg-sess-1", "agg-sess-2"

For EACH session:
  Step 1: Initialize with aggregation config
  Step 2: add_event to leaf: {time: 100, playerStateChange: "init", cdn: "akamai"}
    - latest-event-to-state-2: state = "init"
    - equal-to-1: "init"=="buffer" → false → BoolValue(false)
    - aggregator delta: 0.0 - 0.0 = 0.0 → skip (delta==0)
  Step 3: add_event to leaf: {time: 200, playerStateChange: "buffer", cdn: "akamai"}
    - latest-event-to-state-2: state = "buffer"
    - equal-to-1: "buffer"=="buffer" → true → BoolValue(true)
    - aggregator delta: 1.0 - 0.0 = 1.0 → on_delta(1.0)

After both sessions:
  aggregator-cdn-akamai: count=2, sum=2.0, avg=1.0
```

---

## 8. Graph Encoding — Conversion Examples

### Recursive → Flat (to_graph)

```
EqualTo(TlLatestEventToState("playerStateChange"), "buffer")

→ nodes[0] = Comparison(EqualTo, 1, StringValue("buffer"))
  nodes[1] = TlLatestEventToState("playerStateChange")
```

### Flat → Recursive (to_recursive)

```
nodes[0] = TlDurationWhere(1)
nodes[1] = And(2, 6)
nodes[2] = And(3, 4)
nodes[3] = TlHasExisted(playerStateChange == "play")
nodes[4] = Negation(5)
nodes[5] = TlHasExistedWithin(playerStateChange == "seek", 5)
nodes[6] = Comparison(EqualTo, 7, "buffer")
nodes[7] = TlLatestEventToState("playerStateChange")

→ TlDurationWhere(
    And(
      And(TlHasExisted(psc=="play"), Not(TlHasExistedWithin(psc=="seek",5))),
      EqualTo(TlLatestEventToState("playerStateChange"), "buffer")
    )
  )
```

---

## 9. Cross-Runtime Comparison Test

To verify the Go port produces identical results to the Rust original:

1. Define a set of event sequences (the same events used in integration tests).
2. Run through the Rust/Golem system, capture query results at specific times.
3. Run through the Go/Temporal system with the same events and same query times.
4. Assert all results match exactly.

### Comparison Points for CIRR:

| Query | Target | Time | Expected |
|---|---|---|---|
| get_leaf_result | has-existed-4 | 101 | BoolValue(true) |
| get_leaf_result | has-existed-within-6 | 101 | BoolValue(false) |
| get_leaf_result | latest-event-to-state-8 | 101 | StringValue("play") |
| get_derived_result | and-3 | 101 | BoolValue(true) |
| get_derived_result | equal-to-7 | 101 | BoolValue(false) |
| get_derived_result | and-2 | 101 | BoolValue(false) |
| get_derived_result | duration-where-1 | 101 | IntValue(0) |
| get_leaf_result | latest-event-to-state-8 | 201 | StringValue("buffer") |
| get_derived_result | equal-to-7 | 201 | BoolValue(true) |
| get_derived_result | and-2 | 201 | BoolValue(true) |
| get_derived_result | duration-where-1 | 250 | IntValue(50) |
