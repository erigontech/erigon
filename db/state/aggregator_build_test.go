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

	"github.com/erigontech/erigon/db/kv"
)

func TestAggregatorBuildKeepsBackfilledReceiptVisible(t *testing.T) {
	const stepSize = uint64(10)
	const buildEnd = 2 * stepSize
	type entry struct {
		key   byte
		txNum uint64
	}
	for _, tc := range []struct {
		name      string
		domain    kv.Domain
		receipts  []entry
		mustBuild bool
	}{
		{
			name: "older_receipt_sorts_after_newer_key", domain: kv.ReceiptDomain,
			receipts: []entry{{key: 2, txNum: 1}, {key: 1, txNum: stepSize}},
		},
		{
			name: "backfilled_receipt_cache", domain: kv.RCacheDomain,
			receipts: []entry{{key: 0, txNum: 1}},
		},
		{
			name: "receipt_at_start_of_build_step", domain: kv.ReceiptDomain,
			receipts: []entry{{key: 1, txNum: stepSize}}, mustBuild: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			db, agg := testDbAndAggregatorv3(t, stepSize)
			tdb := asTemporalRoDB(db)
			agg.d[tc.domain].Enabled = false
			require.NoError(t, agg.buildFiles(ctx, tdb, 0, unboundedFinalityCtx, nil))
			agg.EnableDomain(tc.domain)
			require.Equal(t, stepSize, agg.EndTxNumMinimax())

			value := []byte("receipt data")
			require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
				at := agg.BeginFilesRo()
				defer at.Close()
				require.Zero(t, at.TxNumsInFiles(tc.domain))
				write := func(domain kv.Domain, entries ...entry) {
					w := at.d[domain].NewWriter(db)
					defer w.Close()
					for _, e := range entries {
						require.NoError(t, w.PutWithPrev([]byte{e.key}, value, e.txNum, nil))
					}
					require.NoError(t, w.Flush(ctx, tx))
				}
				write(tc.domain, tc.receipts...)
				write(kv.AccountsDomain, entry{key: 1, txNum: stepSize}, entry{key: 2, txNum: buildEnd})
				return nil
			}))

			requireReceipts := func() {
				t.Helper()
				require.NoError(t, db.View(ctx, func(tx kv.Tx) error {
					at := agg.BeginFilesRo()
					defer at.Close()
					for _, e := range tc.receipts {
						got, _, found, err := at.GetLatest(tc.domain, []byte{e.key}, tx, kv.GetLatestOptions{})
						require.NoError(t, err)
						require.Truef(t, found, "receipt at txNum %d must remain readable", e.txNum)
						require.Equal(t, value, got)
					}
					return nil
				}))
			}

			requireReceipts()
			done, started := agg.buildFilesInBackground(tdb, buildEnd, false, unboundedFinalityCtx)
			require.True(t, started)
			<-done
			requireReceipts()
			testAggregatorPrune(t, db, agg)
			requireReceipts()

			at := agg.BeginFilesRo()
			defer at.Close()
			require.Equal(t, buildEnd, at.TxNumsInFiles(kv.AccountsDomain))
			if tc.mustBuild {
				require.Equal(t, buildEnd, at.TxNumsInFiles(tc.domain), "all receipt data fits in the new snapshot")
			}
		})
	}
}
