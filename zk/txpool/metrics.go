package txpool

import (
	"fmt"
	"time"

	"github.com/erigontech/erigon-lib/metrics"
)

var PoolMetrics = map[string]metrics.Gauge{}

func init() {
	PoolMetrics["txpool_size"] = metrics.GetOrCreateGauge(fmt.Sprintf(`txpool{metric="pool_size"}`))
	PoolMetrics["txpool_tx_in"] = metrics.GetOrCreateGauge(fmt.Sprintf(`txpool{metric="tx-in-1m"}`))
	PoolMetrics["txpool_tx_out"] = metrics.GetOrCreateGauge(fmt.Sprintf(`txpool{metric="tx-out-1m"}`))
	PoolMetrics["txpool_median_wait_time_seconds"] = metrics.GetOrCreateGauge(fmt.Sprintf(`txpool{metric="median-wait-seconds-1m"}`))
}

const MetricsRunTime = 1 * time.Minute

type Metrics struct {
	txCounter    uint64
	lastPoolSize uint64
	// TxIn is the number of transactions that entered the pool in the last minute
	TxIn uint64
	// TxOut is the number of transactions that left the pool in the last minute
	TxOut uint64
	// MedianWaitTime is the median wait time in seconds for transactions in the pool over 1 minute
	MedianWaitTimeSeconds uint64
}

func (m *Metrics) IncrementCounter() {
	m.txCounter++
}

func (m *Metrics) ResetCounter() {
	m.txCounter = 0
}

func (m *Metrics) Update(pool *TxPool) {
	currentPoolSize := uint64(pool.pending.Len() + pool.baseFee.Len() + pool.queued.Len())

	out := m.lastPoolSize + m.txCounter - currentPoolSize

	pool.lock.Lock()
	waitTimes := make([]time.Duration, 0, pool.all.tree.Len())
	createdTimes := make([]int64, 0, pool.all.tree.Len())
	pool.all.ascendAll(func(mt *metaTx) bool {
		createdTimes = append(createdTimes, int64(mt.created))
		return true
	})
	pool.lock.Unlock()

	for _, created := range createdTimes {
		waitTimes = append(waitTimes, time.Since(time.Unix(created, 0)))
	}

	medianWaitTime := 0
	if len(waitTimes) > 0 {
		mid := len(waitTimes) / 2
		var median time.Duration
		if len(waitTimes)%2 == 0 {
			median = (waitTimes[mid-1] + waitTimes[mid]) / 2
		} else {
			median = waitTimes[mid]
		}
		medianWaitTime = int(median.Seconds())
		if medianWaitTime < 0 {
			medianWaitTime = 0
		}
	}

	m.lastPoolSize = currentPoolSize
	m.TxIn = m.txCounter
	m.TxOut = out
	m.MedianWaitTimeSeconds = uint64(medianWaitTime)

	m.UpdatePrometheusMetrics(currentPoolSize)
	m.ResetCounter()
}

func (m *Metrics) UpdatePrometheusMetrics(poolSize uint64) {
	for id, pm := range PoolMetrics {
		switch id {
		case "txpool_size":
			pm.SetUint64(poolSize)
		case "txpool_tx_in":
			pm.SetUint64(m.TxIn)
		case "txpool_tx_out":
			pm.SetUint64(m.TxOut)
		case "txpool_median_wait_time_seconds":
			pm.SetUint64(m.MedianWaitTimeSeconds)
		}
	}
}
