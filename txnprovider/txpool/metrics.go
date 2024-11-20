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

package txpool

import "github.com/erigontech/erigon-lib/metrics"

var (
	processBatchTxnsTimer   = metrics.NewSummary(`pool_process_remote_txs`)
	addRemoteTxnsTimer      = metrics.NewSummary(`pool_add_remote_txs`)
	newBlockTimer           = metrics.NewSummary(`pool_new_block`)
	writeToDBTimer          = metrics.NewSummary(`pool_write_to_db`)
	propagateToNewPeerTimer = metrics.NewSummary(`pool_propagate_to_new_peer`)
	propagateNewTxnsTimer   = metrics.NewSummary(`pool_propagate_new_txs`)
	writeToDBBytesCounter   = metrics.GetOrCreateGauge(`pool_write_to_db_bytes`)
	pendingSubCounter       = metrics.GetOrCreateGauge(`txpool_pending`)
	queuedSubCounter        = metrics.GetOrCreateGauge(`txpool_queued`)
	basefeeSubCounter       = metrics.GetOrCreateGauge(`txpool_basefee`)
)
