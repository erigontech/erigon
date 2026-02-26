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

// FiltersConfig defines the configuration settings for RPC subscription filters.
// Each field represents a limit on the number of respective items that can be stored per subscription.
// A value of 0 disables the limit (no cap). Oldest items are evicted first (FIFO) when the limit is reached.
type FiltersConfig struct {
	RpcSubscriptionFiltersMaxLogs      int // Maximum number of logs to store per subscription. Default: 10000
	RpcSubscriptionFiltersMaxHeaders   int // Maximum number of block headers to store per subscription. Default: 10000
	RpcSubscriptionFiltersMaxTxs       int // Maximum number of transactions to store per subscription. Default: 10000
	RpcSubscriptionFiltersMaxAddresses int // Maximum number of addresses per subscription to filter logs by. Default: 0 (no limit)
	RpcSubscriptionFiltersMaxTopics    int // Maximum number of topics per subscription to filter logs by. Default: 0 (no limit)
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
}
