package rawtemporaldb_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

type unwindFilesTx struct {
	kv.TemporalTx
	domain     kv.Domain
	end        uint64
	historyEnd uint64
}

func (tx unwindFilesTx) Debug() kv.TemporalDebugTx {
	return unwindFilesView{TemporalDebugTx: tx.TemporalTx.Debug(), domain: tx.domain, end: tx.end, historyEnd: tx.historyEnd}
}

type unwindFilesView struct {
	kv.TemporalDebugTx
	domain     kv.Domain
	end        uint64
	historyEnd uint64
}

func (view unwindFilesView) DomainFiles(domains ...kv.Domain) kv.VisibleFiles {
	if len(domains) == 1 && domains[0] == view.domain && view.end > 0 {
		return kv.VisibleFiles{unwindFile{end: view.end}, unwindFile{end: max(view.end, view.historyEnd)}}
	}
	return nil
}

func (view unwindFilesView) TxNumsInFiles(domains ...kv.Domain) uint64 {
	if len(domains) == 1 && domains[0] == view.domain {
		return view.end
	}
	return 0
}

type unwindFile struct {
	kv.VisibleFile
	end uint64
}

func (file unwindFile) EndRootNum() uint64 { return file.end }

func TestMinimumUnwindKeepsImmutableTransactions(t *testing.T) {
	for domain := range kv.DomainLen {
		t.Run(domain.String(), func(t *testing.T) {
			_, tx := temporaltest.NewTestTx(t)
			for block := range uint64(5) {
				require.NoError(t, rawdbv3.TxNums.Append(tx, block, 3*block+2))
				if block > 0 {
					diff := &changeset.StateChangeSet{}
					diff.Diffs[kv.AccountsDomain].DomainUpdate([]byte{1}, 0, nil)
					require.NoError(t, changeset.WriteDiffSet(tx, block, common.Hash{byte(block)}, diff))
				}
			}
			for _, tc := range []struct{ end, want uint64 }{{0, 0}, {6, 1}, {7, 2}, {9, 2}, {10, 3}} {
				view := unwindFilesTx{TemporalTx: tx, domain: domain, end: tc.end}
				got, readErr := rawtemporaldb.CanUnwindToBlockNum(view, rawdbv3.TxNums)
				require.NoError(t, readErr)
				require.Equal(t, tc.want, got, "exclusive file end %d", tc.end)
				_, allowed, readErr := rawtemporaldb.CanUnwindBeforeBlockNum(got, view, rawdbv3.TxNums)
				require.NoError(t, readErr)
				require.True(t, allowed)
				if got > 0 {
					_, allowed, readErr = rawtemporaldb.CanUnwindBeforeBlockNum(got-1, view, rawdbv3.TxNums)
					require.NoError(t, readErr)
					require.False(t, allowed)
				}
			}
			_, err := rawtemporaldb.CanUnwindToBlockNum(unwindFilesTx{TemporalTx: tx, domain: domain, end: 100}, rawdbv3.TxNums)
			require.ErrorIs(t, err, commitmentdb.ErrBehindCommitment)
		})
	}
}

func TestMinimumUnwindUsesValueFiles(t *testing.T) {
	_, tx := temporaltest.NewTestTx(t)
	for block := range uint64(5) {
		require.NoError(t, rawdbv3.TxNums.Append(tx, block, 3*block+2))
	}
	diff := &changeset.StateChangeSet{}
	diff.Diffs[kv.AccountsDomain].DomainUpdate([]byte{1}, 0, nil)
	require.NoError(t, changeset.WriteDiffSet(tx, 1, common.Hash{1}, diff))
	view := unwindFilesTx{TemporalTx: tx, domain: kv.AccountsDomain, end: 6, historyEnd: 12}
	got, err := rawtemporaldb.CanUnwindToBlockNum(view, rawdbv3.TxNums)
	require.NoError(t, err)
	require.Equal(t, uint64(1), got)
}

type snapshotTxIndex struct {
	rawdbv3.TxBlockIndex
	t *testing.T
}

func (index snapshotTxIndex) BlockNumber(_ context.Context, _ kv.Tx, txNum uint64) (uint64, bool, error) {
	require.Equal(index.t, uint64(11), txNum)
	return 3, true, nil
}

func TestMinimumUnwindUsesSnapshotBlockIndex(t *testing.T) {
	_, tx := temporaltest.NewTestTx(t)
	diff := &changeset.StateChangeSet{}
	diff.Diffs[kv.AccountsDomain].DomainUpdate([]byte{1}, 0, nil)
	require.NoError(t, changeset.WriteDiffSet(tx, 1, common.Hash{1}, diff))
	view := unwindFilesTx{TemporalTx: tx, domain: kv.AccountsDomain, end: 12}
	reader := rawdbv3.TxNums.WithCustomReadTxNumFunc(snapshotTxIndex{t: t})
	got, err := rawtemporaldb.CanUnwindToBlockNum(view, reader)
	require.NoError(t, err)
	require.Equal(t, uint64(3), got)
}
