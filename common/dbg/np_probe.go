package dbg

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
)

// probe only: splits newPayload wall time around the Execution stage.
var (
	npOn       = EnvBool("NEWPAYLOAD_PHASES", false)
	npMu       sync.Mutex
	npActive   bool
	npMarks    []npMark
	npJWTAt    atomic.Int64
	npHTTPAt   atomic.Int64
	npBatchAt  atomic.Int64
	npParsedAt atomic.Int64
)

type npMark struct {
	name string
	at   time.Time
}

func NpJWTStart() {
	if npOn {
		npJWTAt.Store(time.Now().UnixNano())
	}
}

func NpHTTPStart() {
	if npOn {
		npHTTPAt.Store(time.Now().UnixNano())
	}
}

func NpBatchRead() {
	if npOn {
		npBatchAt.Store(time.Now().UnixNano())
	}
}

func NpArgsParsed() {
	if npOn {
		npParsedAt.Store(time.Now().UnixNano())
	}
}

func NpBegin() {
	if !npOn {
		return
	}
	npMu.Lock()
	defer npMu.Unlock()
	npActive = true
	npMarks = append(npMarks[:0],
		npMark{"jwtStart", time.Unix(0, npJWTAt.Load())},
		npMark{"httpStart", time.Unix(0, npHTTPAt.Load())},
		npMark{"httpRead", time.Unix(0, npBatchAt.Load())},
		npMark{"argsParse", time.Unix(0, npParsedAt.Load())},
		npMark{"handlerIn", time.Now()})
}

func NpMark(name string) { NpMarkAt(name, time.Now()) }

func NpMarkAt(name string, at time.Time) {
	if !npOn {
		return
	}
	npMu.Lock()
	defer npMu.Unlock()
	if npActive {
		npMarks = append(npMarks, npMark{name, at})
	}
}

func NpFlush(logger log.Logger, blk uint64) {
	if !npOn {
		return
	}
	npMu.Lock()
	defer npMu.Unlock()
	if !npActive {
		return
	}
	npActive = false
	npMarks = append(npMarks, npMark{"npEnd", time.Now()})
	marks := npMarks
	sort.SliceStable(marks, func(i, j int) bool { return marks[i].at.Before(marks[j].at) })
	pre := []any{"blk", blk}
	post := []any{"blk", blk}
	var preTotal, postTotal time.Duration
	section := 0
	for i := 1; i < len(marks); i++ {
		d := marks[i].at.Sub(marks[i-1].at)
		switch {
		case marks[i].name == "stageEnd":
			section = 1
		case section == 0:
			pre = append(pre, marks[i].name, d)
			preTotal += d
		default:
			post = append(post, marks[i].name, d)
			postTotal += d
		}
	}
	logger.Info("[np-phase] pre-stage", append(pre, "total", preTotal)...)
	logger.Info("[np-phase] post-exec", append(post, "total", postTotal)...)
}
