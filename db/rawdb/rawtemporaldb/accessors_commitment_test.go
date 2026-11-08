package rawtemporaldb_test

import (
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
	domain kv.Domain
	end    uint64
}

func (tx unwindFilesTx) Debug() kv.TemporalDebugTx {
	return unwindFilesView{TemporalDebugTx: tx.TemporalTx.Debug(), domain: tx.domain, end: tx.end}
}

type unwindFilesView struct {
	kv.TemporalDebugTx
	domain kv.Domain
	end    uint64
}

func (view unwindFilesView) DomainFiles(domains ...kv.Domain) kv.VisibleFiles {
	if len(domains) == 1 && domains[0] == view.domain && view.end > 0 {
		return kv.VisibleFiles{unwindFile{end: view.end}}
	}
	return nil
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
				got, readErr := rawtemporaldb.CanUnwindToBlockNum(view)
				require.NoError(t, readErr)
				require.Equal(t, tc.want, got, "exclusive file end %d", tc.end)
				_, allowed, readErr := rawtemporaldb.CanUnwindBeforeBlockNum(got, view)
				require.NoError(t, readErr)
				require.True(t, allowed)
				if got > 0 {
					_, allowed, readErr = rawtemporaldb.CanUnwindBeforeBlockNum(got-1, view)
					require.NoError(t, readErr)
					require.False(t, allowed)
				}
			}
			_, err := rawtemporaldb.CanUnwindToBlockNum(unwindFilesTx{TemporalTx: tx, domain: domain, end: 100})
			require.ErrorIs(t, err, commitmentdb.ErrBehindCommitment)
		})
	}
}
