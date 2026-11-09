package dbg

import "sync/atomic"

// NpStartNanos and NpStageStartNanos are probe clocks: unix nanos of the last newPayload and execution-stage start.
var NpStartNanos, NpStageStartNanos atomic.Int64
