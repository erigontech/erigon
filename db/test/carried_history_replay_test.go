package test

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestCarriedValues_HistoryReplayRoot replicates the receipts generator's
// post-state flow: touch keys from history, swap in a history state reader,
// SeekCommitment, then ComputeCommitment per replayed tx. History-replay
// touches are read markers — the roots must match plain marker touches.
func TestCarriedValues_HistoryReplayRoot(t *testing.T) {
	t.Parallel()

	stepSize := uint64(5)
	db, _, _ := testDbAndAggregatorv3(t, t.TempDir(), stepSize)

	ctx := t.Context()
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()

	domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(t, err)
	defer domains.Close()

	// Seed a few accounts across two commitment points so history exists.
	mkAcc := func(nonce, bal uint64) []byte {
		a := accounts.Account{Nonce: nonce, Balance: *uint256.NewInt(bal), CodeHash: accounts.EmptyCodeHash}
		return accounts.SerialiseV3(&a)
	}
	addr := func(b byte) []byte {
		k := make([]byte, length.Addr)
		k[0] = b
		return k
	}
	stor := func(b byte, slot byte) []byte {
		k := make([]byte, length.Addr+length.Hash)
		k[0] = b
		k[length.Addr+31] = slot
		return k
	}
	var txNum uint64
	for txNum = 1; txNum <= stepSize*2; txNum++ {
		require.NoError(t, domains.DomainPut(kv.AccountsDomain, rwTx, addr(byte(txNum%4)), mkAcc(txNum, txNum*100), txNum, nil))
		require.NoError(t, domains.DomainPut(kv.StorageDomain, rwTx, stor(byte(txNum%4), byte(txNum%3)), []byte{byte(txNum)}, txNum, nil))
		if txNum%stepSize == 0 {
			_, err := domains.ComputeCommitment(ctx, rwTx, true, txNum/stepSize, txNum, "", nil)
			require.NoError(t, err)
		}
	}
	fillRawdbTxNumsIndexForSharedDomains(t, rwTx, stepSize*2, stepSize)
	require.NoError(t, domains.Flush(ctx, rwTx))
	domains.Close()
	require.NoError(t, rwTx.Commit())

	replayRoots := func(replayed bool) [][]byte {
		tx, err := db.BeginTemporalRw(ctx)
		require.NoError(t, err)
		defer tx.Rollback()
		sd, err := execctx.NewSharedDomains(ctx, tx, log.New(), execctx.WithoutDeferredBranchUpdates(), execctx.WithSequentialCommitment())
		require.NoError(t, err)
		defer sd.Close()

		sd.GetCommitmentContext().SetHistoryStateReader(tx, 1)
		_, _, err = sd.SeekCommitment(ctx, tx)
		require.NoError(t, err)

		// Replay txNums 6..9 one at a time like the receipts generator. The
		// carried arm touches via history replay (TouchChangedKeysFromHistory):
		// those are read markers, and must deliver update==nil so the fold
		// resolves through the history reader — a nil-val touch on the replay
		// path must never become a carried delete.
		var roots [][]byte
		for replayTxNum := stepSize + 1; replayTxNum < stepSize*2; replayTxNum++ {
			a := addr(byte(replayTxNum % 4))
			s := stor(byte(replayTxNum%4), byte(replayTxNum%3))
			// Reader is installed before the touches, like the receipts
			// generator does: nothing may rescue a mis-carried touch after this.
			sd.GetCommitmentContext().SetHistoryStateReader(tx, replayTxNum+1)
			latest, _, err := sd.SeekCommitment(ctx, tx)
			require.NoError(t, err)
			if replayed {
				_, _, err := sd.TouchChangedKeysFromHistory(tx, replayTxNum, replayTxNum+1)
				require.NoError(t, err)
			} else {
				sd.GetCommitmentContext().TouchKey(kv.AccountsDomain, string(a), nil)
				sd.GetCommitmentContext().TouchKey(kv.StorageDomain, string(s), nil)
			}
			root, err := sd.ComputeCommitment(ctx, tx, false, replayTxNum/stepSize, latest, "", nil)
			require.NoError(t, err)
			roots = append(roots, root)
		}
		return roots
	}

	markerRoots := replayRoots(false)
	replayedRoots := replayRoots(true)
	require.Equal(t, markerRoots, replayedRoots, "history-replay touches must resolve through the history reader like markers do")
}
