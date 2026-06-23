package state_test

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestComputeCommitmentStateValueForRange checks that the state key value (and root) can be
// recovered by loading just the root from existing files — without recomputing the range —
// and matches the value the files were built with.
func TestComputeCommitmentStateValueForRange(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	origConcurrent := statecfg.ExperimentalConcurrentCommitment
	t.Cleanup(func() { statecfg.ExperimentalConcurrentCommitment = origConcurrent })
	statecfg.ExperimentalConcurrentCommitment = false

	const (
		numAccounts  = 3000
		totalSteps   = 8
		slotsPerAcct = 2
		stepSize     = 10
	)
	totalTxs := uint64(stepSize * totalSteps)
	ctx := t.Context()

	db, agg, dirs := testDbAndAggregatorForLargeData(t, stepSize, "")
	rawDB := db.(*temporal.DB).InternalDB()
	agg.ForTestReplaceKeysInValues(kv.CommitmentDomain, false)

	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	domains, err := execctx.NewSharedDomains(ctx, rwTx, log.New())
	require.NoError(t, err)

	accPerTx := max((numAccounts+totalTxs-1)/totalTxs, 1)
	storPerTx := max((numAccounts*slotsPerAcct+totalTxs-1)/totalTxs, 1)
	var blockNum, accIdx, storAccIdx, storSlotIdx uint64
	for txNum := uint64(0); txNum < totalTxs; txNum++ {
		for i := uint64(0); i < accPerTx && accIdx < numAccounts; i++ {
			addr := makeAccountAddr(accIdx)
			acc := accounts.Account{Nonce: txNum, Balance: *uint256.NewInt(txNum * 1000), CodeHash: accounts.EmptyCodeHash}
			require.NoError(t, domains.DomainPut(kv.AccountsDomain, rwTx, addr, accounts.SerialiseV3(&acc), txNum, nil))
			accIdx++
		}
		for i := uint64(0); i < storPerTx && (storAccIdx*slotsPerAcct+storSlotIdx) < numAccounts*slotsPerAcct; i++ {
			skey := makeStorageKey(storAccIdx, storSlotIdx, slotsPerAcct)
			var val [32]byte
			binary.BigEndian.PutUint64(val[24:], txNum)
			require.NoError(t, domains.DomainPut(kv.StorageDomain, rwTx, skey, val[:], txNum, nil))
			storSlotIdx++
			if storSlotIdx >= slotsPerAcct {
				storSlotIdx = 0
				storAccIdx++
			}
		}
		if (txNum+1)%stepSize == 0 {
			_, err := domains.ComputeCommitment(ctx, rwTx, true, blockNum, txNum, "", nil)
			require.NoError(t, err)
			require.NoError(t, domains.Flush(ctx, rwTx))
			require.NoError(t, rawdbv3.TxNums.Append(rwTx, blockNum, txNum))
			blockNum++
		}
	}
	require.NoError(t, domains.Flush(ctx, rwTx))
	domains.Close()
	require.NoError(t, rwTx.Commit())
	require.NoError(t, agg.BuildFiles(totalTxs))

	// canonical files via full rebuild (matches what the tool reads from)
	db, agg = reopenAggregator(t, db, agg, stepSize)
	wipeCommitment(t, db, agg, dirs)
	_, err = state.RebuildCommitmentFiles(ctx, db, &rawdbv3.TxNums, log.New(), false)
	require.NoError(t, err)
	_ = rawDB

	for _, r := range fileStepRanges(t, dirs, kv.CommitmentDomain) {
		wantRoot, ok := readCommitmentStateRoot(t, dirs, r.from, r.to)
		require.True(t, ok, "source file %d-%d must have state key", r.from, r.to)

		stateVal, gotRoot, err := state.ComputeCommitmentStateValueForRange(ctx, db, &rawdbv3.TxNums,
			kv.Step(r.from), kv.Step(r.to), log.New())
		require.NoError(t, err, "range %d-%d", r.from, r.to)
		require.Equal(t, wantRoot, common.BytesToHash(gotRoot), "root mismatch for %d-%d", r.from, r.to)

		extractedRoot, bn, txn, err := commitment.HexTrieExtractStateRoot(stateVal)
		require.NoError(t, err)
		require.Equal(t, wantRoot, common.BytesToHash(extractedRoot))
		require.Less(t, txn, r.to*stepSize, "txNum must be within range")
		require.GreaterOrEqual(t, txn, r.from*stepSize)
		t.Logf("range %d-%d: root=%x block=%d txn=%d", r.from, r.to, gotRoot, bn, txn)

		// Full stream path: regenerate the file with the state key. All branch data must be
		// copied verbatim, and the added state key must carry the correct root (its trie
		// working-maps need not be byte-identical — only the root cell matters, which is what
		// integrity's checkCommitmentRootViaSd compares).
		srcKV := readAllCommitmentKV(t, dirs, r.from, r.to)
		outDir := t.TempDir()
		dstPath, _, err := state.RegenerateCommitmentFileWithStateKey(ctx, db, &rawdbv3.TxNums,
			kv.Step(r.from), kv.Step(r.to), outDir, log.New())
		require.NoError(t, err)
		dstKV := readAllCommitmentKVFile(t, dstPath, r.from, r.to)

		dstState, hasState := dstKV["state"]
		require.True(t, hasState, "regenerated file must contain the state key")
		dRoot, _, _, err := commitment.HexTrieExtractStateRoot([]byte(dstState))
		require.NoError(t, err)
		require.Equal(t, wantRoot, common.BytesToHash(dRoot), "regenerated state root must match")

		// branch data must be byte-identical to the source
		delete(srcKV, "state")
		delete(dstKV, "state")
		require.Equal(t, srcKV, dstKV, "branch data must be copied verbatim for %d-%d", r.from, r.to)
	}
}

func readAllCommitmentKV(t *testing.T, dirs datadir.Dirs, from, to uint64) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(dirs.SnapDomain)
	require.NoError(t, err)
	tag := "-" + kv.CommitmentDomain.String() + "."
	suffix := "." + strconv.FormatUint(from, 10) + "-" + strconv.FormatUint(to, 10) + ".kv"
	for _, e := range entries {
		if strings.Contains(e.Name(), tag) && strings.HasSuffix(e.Name(), suffix) {
			return readAllCommitmentKVFile(t, filepath.Join(dirs.SnapDomain, e.Name()), from, to)
		}
	}
	t.Fatalf("no commitment file for %d-%d", from, to)
	return nil
}

func readAllCommitmentKVFile(t *testing.T, path string, from, to uint64) map[string]string {
	t.Helper()
	decomp, err := seg.NewDecompressor(path)
	require.NoError(t, err)
	defer decomp.Close()
	compression := seg.CompressNone
	if to-from > state.DomainMinStepsToCompress {
		compression = statecfg.Schema.GetDomainCfg(kv.CommitmentDomain).Compression
	}
	g := seg.NewReader(decomp.MakeGetter(), compression)
	g.Reset(0)
	out := make(map[string]string)
	for g.HasNext() {
		k, _ := g.Next(nil)
		require.True(t, g.HasNext())
		v, _ := g.Next(nil)
		out[string(k)] = string(v)
	}
	return out
}
