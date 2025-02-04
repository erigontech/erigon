package utils

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/metrics"
	"github.com/ledgerwatch/log/v3"
)

type TxGasLogger struct {
	ctx             context.Context
	logEvery        *time.Ticker
	initialBlock    uint64
	logBlock        uint64
	currentBlockNum uint64
	total           uint64
	logTx           uint64
	lastLogTx       uint64
	logTime         time.Time
	gas             uint64
	currentStateGas uint64
	gasLimit        uint64
	logPrefix       string
	batch           *kv.PendingMutations
	tx              kv.RwTx
	metric          metrics.Gauge
}

func NewTxGasLogger(ctx context.Context, logInterval time.Duration, logBlock, total, gasLimit uint64, logPrefix string, batch *kv.PendingMutations, tx kv.RwTx, metric metrics.Gauge) *TxGasLogger {
	return &TxGasLogger{
		ctx:          ctx,
		logEvery:     time.NewTicker(logInterval),
		initialBlock: logBlock,
		logBlock:     logBlock,
		total:        total,
		logTx:        0,
		lastLogTx:    0,
		logTime:      time.Now(),
		gasLimit:     gasLimit,
		logPrefix:    logPrefix,
		batch:        batch,
		tx:           tx,
		metric:       metric,
	}
}

func (g *TxGasLogger) Start() {
	go func() {
		defer g.logEvery.Stop()
		for {
			select {
			case <-g.ctx.Done():
				log.Info(fmt.Sprintf("[%s] Stopping TxGasLogger", g.logPrefix))
				return
			case <-g.logEvery.C:
				func() {
					// if tx gets committed during metrics collection, ensure we recover and start logging again
					defer func() {
						if r := recover(); r != nil {
							log.Warn(fmt.Sprintf("[%s] Encountered a panic during TxGasLogger, recovering...", g.logPrefix), "error", r)
						}
					}()
					g.logProgress()
					g.logTx = g.lastLogTx
					g.logBlock = g.currentBlockNum
					g.logTime = time.Now()
					g.gas = 0
					if g.tx != nil {
						g.tx.CollectMetrics()
					}
					g.metric.SetUint64(g.logBlock)
				}()
			}
		}
	}()
}

func (g *TxGasLogger) Stop() {
	g.logEvery.Stop()
}

func (g *TxGasLogger) SetTx(tx kv.RwTx) {
	g.tx = tx
}

func (g *TxGasLogger) AddBlock(blockTxCount, gas, currentStateGas, currentBlockNum uint64) {
	g.lastLogTx += blockTxCount
	g.gas += gas
	g.currentStateGas = currentStateGas
	g.currentBlockNum = currentBlockNum
}

func (g *TxGasLogger) logProgress() {
	interval := time.Since(g.logTime)
	speed := float64(g.currentBlockNum-g.logBlock) / (float64(interval) / float64(time.Second))
	speedTx := float64(g.lastLogTx-g.logTx) / (float64(interval) / float64(time.Second))
	speedMgas := float64(g.gas) / 1_000_000 / (float64(interval) / float64(time.Second))
	percent := float64(g.currentBlockNum-g.initialBlock) / float64(g.total) * 100
	gasState := float64(g.currentStateGas) / float64(g.gasLimit) * 100

	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	var logpairs = []interface{}{
		"number", g.currentBlockNum,
		"%", percent,
		"blk/s", fmt.Sprintf("%.1f", speed),
		"tx/s", fmt.Sprintf("%.1f", speedTx),
		"Mgas/s", fmt.Sprintf("%.1f", speedMgas),
		"gasState%", fmt.Sprintf("%.2f", gasState),
	}
	if g.batch != nil {
		logpairs = append(logpairs, "batch", common.ByteCount(uint64((*g.batch).BatchSize())))
	}
	logpairs = append(logpairs, "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
	log.Info(fmt.Sprintf("[%s] Executed blocks", g.logPrefix), logpairs...)
}
