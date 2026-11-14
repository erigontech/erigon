package dbg

import (
	"fmt"
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
	npHTTPAt   atomic.Int64
	npBatchAt  atomic.Int64
	npParsedAt atomic.Int64
)

type npSpan struct {
	name       string
	start, end time.Time
}

var npSpans []npSpan

// NpSpan records a concurrent span (warmup, prefetch); NpFlush logs it relative to the handler start.
func NpSpan(name string, start time.Time) {
	if !npOn {
		return
	}
	end := time.Now()
	npMu.Lock()
	defer npMu.Unlock()
	npSpans = append(npSpans, npSpan{name, start, end})
}

type npMark struct {
	name string
	at   time.Time
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
	if npCountOn {
		for i := range npCnt {
			npCnt[i].Store(0)
		}
	}
	npMarks = append(npMarks[:0],
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
	var h time.Time
	for _, m := range marks {
		if m.name == "handlerIn" {
			h = m.at
		}
	}
	spans := []any{"blk", blk, "np", marks[len(marks)-1].at.Sub(h)}
	for _, sp := range npSpans {
		if sp.end.Before(h) {
			continue
		}
		spans = append(spans, sp.name, fmt.Sprintf("%.1f..%.1f", float64(sp.start.Sub(h).Microseconds())/1000, float64(sp.end.Sub(h).Microseconds())/1000))
	}
	npSpans = npSpans[:0]
	logger.Info("[np-phase] spans", spans...)
	logger.Info("[np-phase] pre-stage", append(pre, "total", preTotal)...)
	logger.Info("[np-phase] post-exec", append(post, "total", postTotal)...)
	npCountsLog(logger, blk)
}

// probe only: per-newPayload counters, on with NP_COUNT=true; NpBegin resets them, NpFlush logs them.
const (
	NpStoGet = iota
	NpStoMem
	NpStoCacheHit
	NpStoCacheMiss
	NpStoOrigin
	NpWarmStoHit
	NpWarmStoMiss
	NpInternKey
	NpColdAcc
	NpColdSto
	NpStoBatchOK
	npCountN
)

var (
	npCountOn  = EnvBool("NP_COUNT", false)
	npCnt      [npCountN]atomic.Int64
	npCntNames = [npCountN]string{"stoGet", "stoMem", "stoCacheHit", "stoCacheMiss", "stoOrigin", "warmStoHit", "warmStoMiss", "internKey", "coldAcc", "coldSto", "stoBatchOK"}
)

func NpCount(i int) {
	if npCountOn {
		npCnt[i].Add(1)
	}
}

func npCountsLog(logger log.Logger, blk uint64) {
	if !npCountOn {
		return
	}
	kv := []any{"blk", blk}
	for i := range npCnt {
		kv = append(kv, npCntNames[i], npCnt[i].Swap(0))
	}
	logger.Info("[np-phase] counts", kv...)
}
