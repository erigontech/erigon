// Copyright 2026 The Erigon Authors
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

package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/types"
)

func TestApplyLogIndexesPreservesReceiptCache(t *testing.T) {
	savedRCache := statecfg.Schema.RCacheDomain
	statecfg.EnableHistoricalRCache()
	t.Cleanup(func() { statecfg.Schema.RCacheDomain = savedRCache })

	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	tx, domains := temporaltest.NewTestTxSD(t, db)
	rs := NewStateV3(domains, true, log.New())
	receipt := &types.Receipt{Status: types.ReceiptStatusSuccessful, CumulativeGasUsed: 21000, GasUsed: 21000}
	require.NoError(t, rs.ApplyTxIndexes(tx, 100, receipt, 0, nil, nil, nil))
	cachedReceipt, _, err := domains.GetLatest(kv.RCacheDomain, tx, rawtemporaldb.ReceiptCacheKey)
	require.NoError(t, err)
	require.NotEmpty(t, cachedReceipt)

	logs := []*types.Log{{Address: common.Address{19: 1}, Topics: []common.Hash{{31: 2}}}}
	require.NoError(t, rs.ApplyLogIndexes(101, logs))
	require.NoError(t, domains.Flush(t.Context(), tx))

	got, _, err := tx.GetLatest(kv.RCacheDomain, rawtemporaldb.ReceiptCacheKey, kv.GetLatestOptions{})
	require.NoError(t, err)
	require.Equal(t, cachedReceipt, got)
}
