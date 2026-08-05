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

package engineapi_test

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/testlog"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/state/changeset"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/node/ethconfig"
)

// TestEngineApiSplitApplyChangesetIsComplete pins that under splitApply the
// per-block changeset records ALL state domains — accounts, storage, code and
// commitment — with prev-values that match the pre-block state. Under splitApply
// the apply loop (not the exec loop) is the sole sd.mem state writer, so the
// block's account/storage/code writes are recorded into the changeset only if the
// apply-loop fold binds the block's changeset (bindBlockChangesetForFold). A
// regression that leaves them out produces a commitment-only changeset that
// silently fails to revert state on an unwind, giving a deterministic wrong trie
// root when a reorg re-executes a competing block on the un-reverted base.
func TestEngineApiSplitApplyChangesetIsComplete(t *testing.T) {
	restore := stagedsync.SetSplitApplyStackForTest()
	t.Cleanup(restore)

	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlWarn)
	genesis, coinbaseKey, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)
	// The tester defaults to Amsterdam (BAL). Under the parallel gate stack the
	// block-assembler and parallel-validation paths derive different BALs — an
	// issue orthogonal to the changeset invariant here — so run pre-Amsterdam
	// (no BAL). The splitApply apply-loop fold and its changeset construction are
	// fork-independent, so the invariant under test is unchanged.
	genesis.Config.AmsterdamTime = nil
	eat, err := engineapitester.InitialiseEngineApiTester(ctx, engineapitester.EngineApiTesterInitArgs{
		Logger:      logger,
		DataDir:     t.TempDir(),
		Genesis:     genesis,
		CoinbaseKey: coinbaseKey,
		EthConfigTweaker: func(config *ethconfig.Config) {
			config.MaxReorgDepth = stateChurnReorgDepthBudget
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, eat.Close()) })
	require.NotNil(t, eat.ChainDB, "tester must expose a temporal ChainDB")

	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		const pokes = 4
		// payloads[0] = StateChurn deploy (height 2, writes code); payloads[k+1] =
		// poke k (height 3+k, churns storage + touches accounts).
		payloads, _, _, _ := buildChurnChain(ctx, t, eat, pokes, func(k int) int64 { return int64(k) })

		tx, err := eat.ChainDB.BeginTemporalRo(ctx)
		require.NoError(t, err)
		defer tx.Rollback()

		assertComplete := func(p *engineapitester.MockClPayload, wantCode bool) {
			num := p.ExecutionPayload.BlockNumber.Uint64()
			hash := p.ExecutionPayload.BlockHash
			diffs, ok, err := changeset.ReadDiffSet(tx, num, hash)
			require.NoError(t, err)
			require.Truef(t, ok, "block %d must have a persisted changeset", num)

			// Matches the domain writes: a churn block writes accounts, storage and
			// commitment; the deploy block also writes code. Before the fix
			// accounts/storage/code were 0 (commitment-only) — the bug.
			require.NotEmptyf(t, diffs[kv.AccountsDomain], "block %d changeset missing ACCOUNTS diffs", num)
			require.NotEmptyf(t, diffs[kv.StorageDomain], "block %d changeset missing STORAGE diffs", num)
			require.NotEmptyf(t, diffs[kv.CommitmentDomain], "block %d changeset missing COMMITMENT diffs", num)
			if wantCode {
				require.NotEmptyf(t, diffs[kv.CodeDomain], "block %d (deploy) changeset missing CODE diffs", num)
			}

			// Matches the state: each recorded prev-value must equal the value as of
			// the block's first txNum — the pre-block state a reverse-apply restores.
			firstTxNum, err := rawdbv3.TxNums.Min(ctx, tx, num)
			require.NoError(t, err)
			for _, d := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain} {
				for _, e := range diffs[d] {
					if len(e.Value) == 0 {
						continue // "different step" marker; no restore value to compare
					}
					key := []byte(e.Key)[:len(e.Key)-8] // strip the 8-byte step suffix
					prev, _, err := tx.GetAsOf(d, key, firstTxNum)
					require.NoError(t, err)
					require.Equalf(t, e.Value, prev,
						"block %d %s key %x: changeset prev-value must equal the pre-block state", num, d, key)
				}
			}
		}

		assertComplete(payloads[0], true)      // deploy block: code + accounts + commitment
		assertComplete(payloads[pokes], false) // a poke block: storage churn + accounts + commitment
	})
}

// TestEngineApiSplitApplyUnwindRevertsState is the end-to-end proof that the
// changeset the apply-loop fold builds is actually USED correctly on an unwind:
// it churns storage, then repeatedly unwinds (fcu to an earlier block) and redoes,
// asserting after each that the contract's live state matches the value originally
// recorded at that height. Under splitApply, a commitment-only changeset (the bug)
// leaves a flushed block's account/storage un-reverted, so a poke re-read after the
// unwind trips the in-EVM invariant (or a redo re-executes on a corrupt base and
// diverges). Mirrors TestEngineApiUnwindRedoStateChurnPreservesState but under the
// splitApply gate stack; pre-Amsterdam to avoid the (orthogonal) block-assembler
// BAL mismatch.
func TestEngineApiSplitApplyUnwindRevertsState(t *testing.T) {
	restore := stagedsync.SetSplitApplyStackForTest()
	t.Cleanup(restore)

	ctx := t.Context()
	logger := testlog.Logger(t, log.LvlWarn)
	genesis, coinbaseKey, err := engineapitester.DefaultEngineApiTesterGenesis()
	require.NoError(t, err)
	genesis.Config.AmsterdamTime = nil // pre-Amsterdam: no BAL cross-check
	eat, err := engineapitester.InitialiseEngineApiTester(ctx, engineapitester.EngineApiTesterInitArgs{
		Logger:      logger,
		DataDir:     t.TempDir(),
		Genesis:     genesis,
		CoinbaseKey: coinbaseKey,
		EthConfigTweaker: func(config *ethconfig.Config) {
			config.MaxReorgDepth = stateChurnReorgDepthBudget
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, eat.Close()) })

	eat.Run(t, func(ctx context.Context, t *testing.T, eat engineapitester.EngineApiTester) {
		const pokes = 22
		payloads, _, churn, sums := buildChurnChain(ctx, t, eat, pokes, func(k int) int64 { return int64(k) })
		tip := uint64(2 + pokes)
		at := func(h uint64) *engineapitester.MockClPayload { return payloads[h-2] }
		sumAt := func(h uint64) *big.Int { return sums[h-2] }

		assertChurnState(ctx, t, eat, churn, at(tip), sumAt(tip))

		// Shallow and deep unwinds (fcu to ancestor) interleaved with redos. Each
		// fcu-to-ancestor unwinds real state; assertChurnState then proves it was
		// rolled back (not just the head pointer moved).
		sequence := []uint64{tip - 1, tip, tip - 6, tip, tip - 14, tip, 3, tip, 5, 9, 4, tip}
		head := tip
		for _, target := range sequence {
			for h := head + 1; h <= target; h++ { // redo forward
				status, err := eat.MockCl.InsertNewPayload(ctx, at(h))
				require.NoError(t, err)
				require.Equalf(t, enginetypes.ValidStatus, status.Status, "re-insert of block %d while redoing", h)
			}
			require.NoErrorf(t, eat.MockCl.UpdateForkChoice(ctx, at(target)), "fcu (unwind/redo) to block %d", target)
			assertChurnState(ctx, t, eat, churn, at(target), sumAt(target))
			head = target
		}
	})
}
