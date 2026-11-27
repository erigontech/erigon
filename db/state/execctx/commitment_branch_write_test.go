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

package execctx_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestCommitmentNewBranchesDoNotReadPreviousValues(t *testing.T) {
	for _, deferred := range []bool{false, true} {
		t.Run(fmt.Sprintf("deferred_%t", deferred), func(t *testing.T) {
			db := newTestDb(t, 16)
			tx, err := db.BeginTemporalRw(t.Context())
			require.NoError(t, err)
			defer tx.Rollback()
			counted := &countingLatestTx{TemporalTx: tx}
			cfg := commitment.DefaultTrieConfig()
			cfg.Variant = commitment.VariantHexPatriciaTrie
			cfg.DeferBranchUpdates = deferred
			doms, err := execctx.NewSharedDomains(t.Context(), counted, log.New(), execctx.WithTrieConfig(cfg), execctx.WithoutSharedBranchCache())
			require.NoError(t, err)
			defer doms.Close()
			cs := &changeset.StateChangeSet{}
			doms.SetChangesetAccumulator(cs)
			acc := accounts.Account{Nonce: 1, CodeHash: accounts.EmptyCodeHash}
			for i := range 32 {
				require.NoError(t, doms.DomainPut(kv.AccountsDomain, counted, bytes.Repeat([]byte{byte(i)}, 20), accounts.SerialiseV3(&acc), 1, []byte{}))
			}

			counted.reads = 0
			_, err = doms.ComputeCommitment(t.Context(), counted, false, 1, 1, "", nil)
			require.NoError(t, err)
			require.NoError(t, doms.FlushPendingUpdates(counted))
			require.Greater(t, cs.Diffs[kv.CommitmentDomain].Len(), 1, "create interior branches as well as the root")
			// The empty trie needs one root lookup. New branch writes must not
			// read storage again to discover that their previous value is empty.
			require.Equal(t, 1, counted.reads)
		})
	}
}
