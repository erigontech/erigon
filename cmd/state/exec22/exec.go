package exec22

import (
	"fmt"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/log/v3"
)

func NewProgress(prevOutputBlockNum uint64) *Progress {
	return &Progress{prevTime: time.Now(), prevOutputBlockNum: prevOutputBlockNum}
}

type Progress struct {
	prevTime           time.Time
	prevCount          uint64
	prevOutputBlockNum uint64
	prevRepeatCount    uint64
}

func (p *Progress) Log(rs *state.State22, rws state.TxTaskQueue, count, inputBlockNum, outputBlockNum, repeatCount uint64, resultsSize uint64) {
	var m runtime.MemStats
	common.ReadMemStats(&m)
	sizeEstimate := rs.SizeEstimate()
	currentTime := time.Now()
	interval := currentTime.Sub(p.prevTime)
	speedTx := float64(count-p.prevCount) / (float64(interval) / float64(time.Second))
	speedBlock := float64(outputBlockNum-p.prevOutputBlockNum) / (float64(interval) / float64(time.Second))
	var repeatRatio float64
	if count > p.prevCount {
		repeatRatio = 100.0 * float64(repeatCount-p.prevRepeatCount) / float64(count-p.prevCount)
	}
	log.Info("Transaction replay",
		//"workers", workerCount,
		"at block", outputBlockNum,
		"input block", atomic.LoadUint64(&inputBlockNum),
		"blk/s", fmt.Sprintf("%.1f", speedBlock),
		"tx/s", fmt.Sprintf("%.1f", speedTx),
		"result queue", rws.Len(),
		"results size", common.ByteCount(resultsSize),
		"repeat ratio", fmt.Sprintf("%.2f%%", repeatRatio),
		"buffer", common.ByteCount(sizeEstimate),
		"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys),
	)
	p.prevTime = currentTime
	p.prevCount = count
	p.prevOutputBlockNum = outputBlockNum
	p.prevRepeatCount = repeatCount
}
