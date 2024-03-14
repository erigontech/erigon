package metrics

import (
	"math/big"
	"time"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/metrics"
)

var (
	BlockConsumerHeaderDownloadDelay = metrics.NewSummary(`block_consumer_delay{type="header_download"}`)
	BlockConsumerBodyDownloadDelay   = metrics.NewSummary(`block_consumer_delay{type="body_download"}`)
	BlockConsumerPreExecutionDelay   = metrics.NewSummary(`block_consumer_delay{type="pre_execution"}`)
	BlockConsumerPostExecutionDelay  = metrics.NewSummary(`block_consumer_delay{type="post_execution"}`)
	BlockProducerProductionDelay     = metrics.NewSummary(`block_producer_delay{type="production"}`)
	DelayLoggingEnabled              bool
)

func UpdateBlockConsumerHeaderDownloadDelay(parentTime uint64, blockNumber big.Int, log log.Logger) {
	BlockConsumerHeaderDownloadDelay.ObserveDuration(time.Unix(int64(parentTime), 0))

	if DelayLoggingEnabled {
		log.Info("[consumer-delay] Header", "blockNumber", blockNumber.Uint64(), "delay", time.Since(time.Unix(int64(parentTime), 0)))
	}
}

func UpdateBlockConsumerBodyDownloadDelay(parentTime uint64, blockNumber big.Int, log log.Logger) {
	BlockConsumerBodyDownloadDelay.ObserveDuration(time.Unix(int64(parentTime), 0))

	if DelayLoggingEnabled {
		log.Info("[consumer-delay] Body", "blockNumber", blockNumber.Uint64(), "delay", time.Since(time.Unix(int64(parentTime), 0)))
	}
}

func UpdateBlockConsumerPreExecutionDelay(parentTime uint64, blockNumber uint64, log log.Logger) {
	BlockConsumerPreExecutionDelay.ObserveDuration(time.Unix(int64(parentTime), 0))

	if DelayLoggingEnabled {
		log.Info("[consumer-delay] Pre-execution", "blockNumber", blockNumber, "delay", time.Since(time.Unix(int64(parentTime), 0)))
	}
}

func UpdateBlockConsumerPostExecutionDelay(parentTime uint64, blockNumber uint64, log log.Logger) {
	BlockConsumerPostExecutionDelay.ObserveDuration(time.Unix(int64(parentTime), 0))

	if DelayLoggingEnabled {
		log.Info("[consumer-delay] Post-execution", "blockNumber", blockNumber, "delay", time.Since(time.Unix(int64(parentTime), 0)))
	}
}

func UpdateBlockProducerProductionDelay(parentTime uint64, blockNumber big.Int, log log.Logger) {
	BlockProducerProductionDelay.ObserveDuration(time.Unix(int64(parentTime), 0))

	if DelayLoggingEnabled {
		log.Info("[producer-delay] Production", "blockNumber", blockNumber.Uint64(), "delay", time.Since(time.Unix(int64(parentTime), 0)))
	}
}
