# fix: handle empty JSON-RPC responses during server shutdown

## Problem

The CI test `TestInvalidReceiptHashHighMgas` in `execution/tests` fails intermittently with:

```
unexpected end of JSON input
```

This is a race condition between the RPC server shutting down (during test cleanup) and in-flight engine API requests. When the server's `run` flag is set to `false` during shutdown, `serveSingleRequest` returns `nil`, and `ServeHTTP` sends a 200 OK response with an empty body. The client then tries to `json.Unmarshal` the empty body, producing the "unexpected end of JSON input" error.

Because this error doesn't match any of the configured retryable error patterns (only "connection refused" was retried), the backoff library treats it as a permanent failure and the test fails immediately.

## Root Cause

The failure chain:

1. Server shutdown sets `s.run` to `false`
2. An in-flight request reaches `ServeHTTP` → `serveSingleRequest`
3. `serveSingleRequest` checks `!s.run.Load()` and returns `nil` (no error message)
4. `ServeHTTP` sees `nil` error, writes no response body, returns 200 OK with empty body
5. Client's `doRequest` returns empty `[]byte{}` with no error (status 200 passes the check)
6. `sendHTTP` calls `json.Unmarshal([]byte{}, &respMsg)` → "unexpected end of JSON input"
7. `maybeMakePermanent` doesn't recognize this as retryable → `backoff.Permanent(err)`
8. Test fails

## Fix

Three-part defensive fix addressing both server and client sides:

### 1. Server-side: Early shutdown check in `ServeHTTP` (`rpc/http.go`)

Added a check for `s.run.Load()` immediately after request validation. If the server is stopped, return 503 Service Unavailable instead of proceeding to `serveSingleRequest` which would silently produce an empty response.

### 2. Client-side: Reject empty response bodies (`rpc/http.go`)

Added a check in `doRequest` that returns a clear error ("empty response from JSON-RPC server") when the response body is empty, instead of letting `json.Unmarshal` fail with the cryptic "unexpected end of JSON input".

### 3. Client-side: Default retryable error checkers (`execution/engineapi/engine_api_jsonrpc_client.go`)

Added default retryable error checkers in `DialJsonRpcClient` that always retry on:
- `"empty response from JSON-RPC server"` — transient empty responses during server restart/shutdown
- `"503 Service Unavailable"` — server explicitly signaling it's not ready

These defaults are prepended to any caller-supplied checkers, ensuring transient server unavailability is always retried regardless of the caller's configuration.

## Testing

- Ran `TestInvalidReceiptHashHighMgas` 12+ consecutive times — all passed
- `make lint` passes with 0 issues
- `make erigon` builds successfully
- `make integration` builds successfully

## Files Changed

- `rpc/http.go` — Server-side 503 on shutdown + client-side empty body rejection
- `execution/engineapi/engine_api_jsonrpc_client.go` — Default retryable error checkers
