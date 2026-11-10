package commitment

import (
	"sync/atomic"
	"time"
)

var Probe [32]atomic.Int64

func ProbeAdd(i int, t time.Time) { Probe[i].Add(int64(time.Since(t))) }
