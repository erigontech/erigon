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
type FiltersConfig struct {
	RpcSubscriptionFiltersMaxLogs      int // Maximum number of logs to store per subscription. Default: 0 (no limit)
	RpcSubscriptionFiltersMaxHeaders   int // Maximum number of block headers to store per subscription. Default: 0 (no limit)
	RpcSubscriptionFiltersMaxTxs       int // Maximum number of transactions to store per subscription. Default: 0 (no limit)
	RpcSubscriptionFiltersMaxAddresses int // Maximum number of addresses per subscription to filter logs by. Default: 0 (no limit)
	RpcSubscriptionFiltersMaxTopics    int // Maximum number of topics per subscription to filter logs by. Default: 0 (no limit)
}

// DefaultFiltersConfig defines the default settings for filter configurations.
// These default values set no limits on the number of logs, block headers, transactions,
// addresses, or topics that can be stored per subscription.
var DefaultFiltersConfig = FiltersConfig{
	RpcSubscriptionFiltersMaxLogs:      0, // No limit on the number of logs per subscription
	RpcSubscriptionFiltersMaxHeaders:   0, // No limit on the number of block headers per subscription
	RpcSubscriptionFiltersMaxTxs:       0, // No limit on the number of transactions per subscription
	RpcSubscriptionFiltersMaxAddresses: 0, // No limit on the number of addresses per subscription to filter logs by
	RpcSubscriptionFiltersMaxTopics:    0, // No limit on the number of topics per subscription to filter logs by
}
