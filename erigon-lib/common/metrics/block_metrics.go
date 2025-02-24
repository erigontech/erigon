// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package metrics

import (
	"time"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/metrics"
)

var (
	delayBuckets = []float64{0.05, 0.125, 0.25, 0.5, 1, 2, 4, 8}

	DelayLoggingEnabled bool

	BlockConsumerHeaderDownloadDelay = metrics.NewSummary(`block_consumer_delay{type="header_download"}`)
	BlockConsumerBodyDownloadDelay   = metrics.NewSummary(`block_consumer_delay{type="body_download"}`)
	BlockConsumerPreExecutionDelay   = metrics.NewSummary(`block_consumer_delay{type="pre_execution"}`)
	BlockConsumerPostExecutionDelay  = metrics.NewSummary(`block_consumer_delay{type="post_execution"}`)
	BlockProducerProductionDelay     = metrics.NewSummary(`block_producer_delay{type="production"}`)

	ChainTipMgasPerSec = metrics.NewGauge(`chain_tip_mgas_per_sec`)

	BlockConsumerHeaderDownloadDelayHistogram = metrics.NewHistogram(`block_consumer_delay_hist{type="header_download"}`, delayBuckets)
	BlockConsumerBodyDownloadDelayHistogram   = metrics.NewHistogram(`block_consumer_delay_hist{type="body_download"}`, delayBuckets)
	BlockConsumerPreExecutionDelayHistogram   = metrics.NewHistogram(`block_consumer_delay_hist{type="pre_execution"}`, delayBuckets)
	BlockConsumerPostExecutionDelayHistogram  = metrics.NewHistogram(`block_consumer_delay_hist{type="post_execution"}`, delayBuckets)
)

func UpdateBlockConsumerHeaderDownloadDelay(blockTime uint64, blockNumber uint64, log log.Logger) {
	t := time.Unix(int64(blockTime), 0)
	BlockConsumerHeaderDownloadDelay.ObserveDuration(t)
	BlockConsumerHeaderDownloadDelayHistogram.ObserveDuration(t)

	if DelayLoggingEnabled {
		log.Info("[consumer-delay] Header", "blockNumber", blockNumber, "delay", time.Since(t))
	}
}

func UpdateBlockConsumerBodyDownloadDelay(blockTime uint64, blockNumber uint64, log log.Logger) {
	t := time.Unix(int64(blockTime), 0)
	BlockConsumerBodyDownloadDelay.ObserveDuration(t)
	BlockConsumerBodyDownloadDelayHistogram.ObserveDuration(t)

	if DelayLoggingEnabled {
		log.Info("[consumer-delay] Body", "blockNumber", blockNumber, "delay", time.Since(t))
	}
}

func UpdateBlockConsumerPreExecutionDelay(blockTime uint64, blockNumber uint64, log log.Logger) {
	t := time.Unix(int64(blockTime), 0)
	BlockConsumerPreExecutionDelay.ObserveDuration(t)
	BlockConsumerPreExecutionDelayHistogram.ObserveDuration(t)

	if DelayLoggingEnabled {
		log.Info("[consumer-delay] Pre-execution", "blockNumber", blockNumber, "delay", time.Since(t))
	}
}

func UpdateBlockConsumerPostExecutionDelay(blockTime uint64, blockNumber uint64, log log.Logger) {
	t := time.Unix(int64(blockTime), 0)
	BlockConsumerPostExecutionDelay.ObserveDuration(t)
	BlockConsumerPostExecutionDelayHistogram.ObserveDuration(t)

	if DelayLoggingEnabled {
		log.Info("[consumer-delay] Post-execution", "blockNumber", blockNumber, "delay", time.Since(t))
	}
}

func UpdateBlockProducerProductionDelay(parentBlockTime uint64, producedBlockNum uint64, log log.Logger) {
	t := time.Unix(int64(parentBlockTime), 0)
	BlockProducerProductionDelay.ObserveDuration(t)

	if DelayLoggingEnabled {
		log.Info("[producer-delay] Production", "blockNumber", producedBlockNum, "delay", time.Since(t))
	}
}
