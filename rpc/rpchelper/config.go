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

package rpchelper

import (
	"fmt"
	"time"

	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/filters"
)

// DefaultFilterTimeout matches geth's deadline for evicting idle filters; 0 disables eviction.
const DefaultFilterTimeout = 5 * time.Minute

// FiltersConfig defines resource limits for RPC filters. A value of 0 disables a limit.
// Queue limits evict the oldest entries, while criteria limits reject oversized subscriptions.
type FiltersConfig struct {
	RpcSubscriptionFiltersMaxLogs      int           // Maximum number of logs to store per subscription. Default: 10000
	RpcSubscriptionFiltersMaxHeaders   int           // Maximum number of block headers to store per subscription. Default: 10000
	RpcSubscriptionFiltersMaxTxs       int           // Maximum number of transactions to store per subscription. Default: 10000
	RpcSubscriptionFiltersMaxAddresses int           // Maximum number of addresses accepted per log subscription. Default: 0 (no limit)
	RpcSubscriptionFiltersMaxTopics    int           // Maximum topic alternatives accepted across all positions per log subscription. Default: 0 (no limit)
	RpcSubscriptionFiltersTimeout      time.Duration // Timeout before idle filters are evicted. Default: 5m; 0 disables eviction
}

// LogFilterLimits defines configured resource limits for log subscriptions.
type LogFilterLimits struct {
	MaxAddresses         int
	MaxTopicAlternatives int
}

func (limits LogFilterLimits) Validate(criteria filters.FilterCriteria) error {
	if limits.MaxAddresses > 0 && len(criteria.Addresses) > limits.MaxAddresses {
		return &rpc.InvalidParamsError{
			Message: fmt.Sprintf("log filter has %d addresses, maximum is %d", len(criteria.Addresses), limits.MaxAddresses),
		}
	}
	if limits.MaxTopicAlternatives <= 0 {
		return nil
	}

	topicCount := 0
	for _, topics := range criteria.Topics {
		topicCount += len(topics)
	}
	if topicCount > limits.MaxTopicAlternatives {
		return &rpc.InvalidParamsError{
			Message: fmt.Sprintf("log filter has %d topic alternatives, maximum is %d", topicCount, limits.MaxTopicAlternatives),
		}
	}
	return nil
}

func (config FiltersConfig) logFilterLimits() LogFilterLimits {
	return LogFilterLimits{
		MaxAddresses:         config.RpcSubscriptionFiltersMaxAddresses,
		MaxTopicAlternatives: config.RpcSubscriptionFiltersMaxTopics,
	}
}

// DefaultFiltersConfig defines the default settings for filter configurations.
// Logs, headers and transactions are capped at 10000 items per subscription to prevent
// unbounded memory growth when polling clients stop calling eth_getFilterChanges.
// Oldest items are evicted first (FIFO) when the cap is reached.
var DefaultFiltersConfig = FiltersConfig{
	RpcSubscriptionFiltersMaxLogs:      10000,
	RpcSubscriptionFiltersMaxHeaders:   10000,
	RpcSubscriptionFiltersMaxTxs:       10000,
	RpcSubscriptionFiltersMaxAddresses: 0, // no limit
	RpcSubscriptionFiltersMaxTopics:    0, // no limit
	RpcSubscriptionFiltersTimeout:      DefaultFilterTimeout,
}
