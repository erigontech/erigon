package metrics

import (
	"time"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/metrics"
)

var (
	DelayLoggingEnabled bool

	BlockConsumerHeaderDownloadDelay = metrics.NewSummary(`block_consumer_delay{type="header_download"}`)
	BlockConsumerBodyDownloadDelay   = metrics.NewSummary(`block_consumer_delay{type="body_download"}`)
	BlockConsumerPreExecutionDelay   = metrics.NewSummary(`block_consumer_delay{type="pre_execution"}`)
	BlockConsumerPostExecutionDelay  = metrics.NewSummary(`block_consumer_delay{type="post_execution"}`)
	BlockProducerProductionDelay     = metrics.NewSummary(`block_producer_delay{type="production"}`)
)

func UpdateBlockConsumerHeaderDownloadDelay(parentTime uint64, blockNumber uint64, log log.Logger) {
	t := time.Unix(int64(parentTime), 0)
	BlockConsumerHeaderDownloadDelay.ObserveDuration(t)

	if DelayLoggingEnabled {
		log.Info("[consumer-delay] Header", "blockNumber", blockNumber, "delay", time.Since(t))
	}
}

func UpdateBlockConsumerBodyDownloadDelay(parentTime uint64, blockNumber uint64, log log.Logger) {
	t := time.Unix(int64(parentTime), 0)
	BlockConsumerBodyDownloadDelay.ObserveDuration(t)

	if DelayLoggingEnabled {
		log.Info("[consumer-delay] Body", "blockNumber", blockNumber, "delay", time.Since(t))
	}
}

func UpdateBlockConsumerPreExecutionDelay(parentTime uint64, blockNumber uint64, log log.Logger) {
	t := time.Unix(int64(parentTime), 0)
	BlockConsumerPreExecutionDelay.ObserveDuration(t)

	if DelayLoggingEnabled {
		log.Info("[consumer-delay] Pre-execution", "blockNumber", blockNumber, "delay", time.Since(t))
	}
}

func UpdateBlockConsumerPostExecutionDelay(parentTime uint64, blockNumber uint64, log log.Logger) {
	t := time.Unix(int64(parentTime), 0)
	BlockConsumerPostExecutionDelay.ObserveDuration(t)

	if DelayLoggingEnabled {
		log.Info("[consumer-delay] Post-execution", "blockNumber", blockNumber, "delay", time.Since(t))
	}
}

func UpdateBlockProducerProductionDelay(parentTime uint64, blockNumber uint64, log log.Logger) {
	t := time.Unix(int64(parentTime), 0)
	BlockProducerProductionDelay.ObserveDuration(t)

	if DelayLoggingEnabled {
		log.Info("[producer-delay] Production", "blockNumber", blockNumber, "delay", time.Since(t))
	}
}
