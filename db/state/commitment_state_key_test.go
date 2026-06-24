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

// TestWriteCommitmentFileWithStateKey checks that streaming a commitment file and splicing in a
// state key (built from a given root/block/txn — as the tool does with the block-header root)
// copies all branch data verbatim and inserts a well-formed, correctly-sorted state entry.
func TestWriteCommitmentFileWithStateKey(t *testing.T) {
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
	_ = rawDB

	ranges := fileStepRanges(t, dirs, kv.CommitmentDomain)
	require.NotEmpty(t, ranges)
	for _, r := range ranges {
		// Use the file's own state-key (root/block/txn) as the value to splice back in — this
		// stands in for the header-derived root the command computes.
		want := readAllCommitmentKVFile(t, commitmentFilePath(t, dirs, r.from, r.to), r.from, r.to)
		stateBefore, ok := want["state"]
		require.True(t, ok, "generated file %d-%d should have a state key", r.from, r.to)
		root, bn, txn, err := commitment.HexTrieExtractStateRoot([]byte(stateBefore))
		require.NoError(t, err)

		outDir := t.TempDir()
		dstPath, err := state.WriteCommitmentFileWithStateKey(ctx, db,
			kv.Step(r.from), kv.Step(r.to), root, bn, txn, outDir, log.New())
		require.NoError(t, err)

		got := readAllCommitmentKVFile(t, dstPath, r.from, r.to)
		gotState, ok := got["state"]
		require.True(t, ok, "regenerated file must contain the state key")
		gRoot, gBn, gTxn, err := commitment.HexTrieExtractStateRoot([]byte(gotState))
		require.NoError(t, err)
		require.Equal(t, common.BytesToHash(root), common.BytesToHash(gRoot))
		require.Equal(t, bn, gBn)
		require.Equal(t, txn, gTxn)

		// branch data byte-identical to source
		delete(want, "state")
		delete(got, "state")
		require.Equal(t, want, got, "branch data must be copied verbatim for %d-%d", r.from, r.to)
		t.Logf("range %d-%d: root=%x block=%d txn=%d branches=%d", r.from, r.to, root, bn, txn, len(got))
	}
}

func commitmentFilePath(t *testing.T, dirs datadir.Dirs, from, to uint64) string {
	t.Helper()
	entries, err := os.ReadDir(dirs.SnapDomain)
	require.NoError(t, err)
	tag := "-" + kv.CommitmentDomain.String() + "."
	suffix := "." + strconv.FormatUint(from, 10) + "-" + strconv.FormatUint(to, 10) + ".kv"
	for _, e := range entries {
		if strings.Contains(e.Name(), tag) && strings.HasSuffix(e.Name(), suffix) {
			return filepath.Join(dirs.SnapDomain, e.Name())
		}
	}
	t.Fatalf("no commitment file for %d-%d", from, to)
	return ""
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
