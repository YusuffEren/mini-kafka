# Findings

## pkg/client: concurrency safety (W1 / T-15)

### What was fixed
- `Producer`, `Consumer`, `GroupConsumer` now serialise full request/response
  round-trips over their shared connection via a dedicated `reqMu sync.Mutex`
  (separate from the existing `mu` which only guards connection/closed state).
- Response `CorrelationID` is now validated against the request's `corrID` on
  every round-trip. On mismatch the connection is closed and an error is
  returned, because a mismatch means the response stream is desynchronised and
  any subsequent reads would return the wrong response.

### Out of scope (future work): true pipelining
The current fix serialises all requests on a single connection. This is
correct and safe, but it sacrifices throughput: a producer cannot have more
than one in-flight request at a time.

True pipelining would allow multiple in-flight requests on the same
connection by:
- Keeping a map of `correlationID -> chan *ResponseFrame` (or a pending
  request registry).
- A single reader goroutine that reads response frames and dispatches each
  to the waiting caller by `correlationID`.
- Callers block on their channel waiting for *their* response, while the
  connection stays writable for others.

This is intentionally **not** implemented here. It is a larger change that
touches the connection lifecycle (dedicated reader goroutine, shutdown
semantics, error propagation to all waiters) and should be tracked as a
separate task.

### Note on GroupConsumer
`GroupConsumer.sendRequest` dials a **new** connection per call (unlike
`Producer`/`Consumer` which reuse a single cached connection). The
`reqMu` serialisation is still applied for correctness and to match the
shared contract, but the primary concurrency hazard there is lower than in
`Producer`/`Consumer`. Consolidating `GroupConsumer` onto a single shared
connection (and then benefiting from `reqMu`) is also future work.