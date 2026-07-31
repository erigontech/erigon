package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
)

const minimaxTestStepSize = 16

func addDirtyFiles(t *testing.T, agg *Aggregator, domain kv.Domain, endStep uint64) {
	t.Helper()
	agg.d[domain].dirtyFiles.Set(newFilesItem(0, endStep*minimaxTestStepSize))
}

func TestDirtyFilesEndTxNumMinimax_NoFiles(t *testing.T) {
	t.Parallel()
	_, agg := testDbAndAggregatorv3(t, minimaxTestStepSize)
	require.Zero(t, agg.dirtyFilesEndTxNumMinimax())
}

func TestDirtyFilesEndTxNumMinimax_LaggingCommitmentCapsCeiling(t *testing.T) {
	t.Parallel()
	_, agg := testDbAndAggregatorv3(t, minimaxTestStepSize)
	for _, d := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain} {
		addDirtyFiles(t, agg, d, 4)
	}
	addDirtyFiles(t, agg, kv.CommitmentDomain, 2)
	require.EqualValues(t, 2*minimaxTestStepSize, agg.dirtyFilesEndTxNumMinimax())
}

func TestDirtyFilesEndTxNumMinimax_LaggingStandaloneIdxCapsCeiling(t *testing.T) {
	t.Parallel()
	_, agg := testDbAndAggregatorv3(t, minimaxTestStepSize)
	for _, d := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain, kv.CommitmentDomain} {
		addDirtyFiles(t, agg, d, 4)
	}
	ii := agg.standaloneIIs()[0]
	require.False(t, ii.Disable)
	ii.dirtyFiles.Set(newFilesItem(0, 1*minimaxTestStepSize))
	require.EqualValues(t, 1*minimaxTestStepSize, agg.dirtyFilesEndTxNumMinimax())
}

// A decoupled dependent (commitment while rebuild tooling has switched the
// integrity checks off) is regenerated from the others, so its partial progress
// must not clamp them.
func TestDirtyFilesEndTxNumMinimax_DecoupledCommitmentDoesNotCap(t *testing.T) {
	t.Parallel()
	_, agg := testDbAndAggregatorv3(t, minimaxTestStepSize)
	for _, d := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain} {
		addDirtyFiles(t, agg, d, 4)
	}
	addDirtyFiles(t, agg, kv.CommitmentDomain, 2)

	require.NotNil(t, agg.checker)
	agg.checker.Disable()
	require.EqualValues(t, 4*minimaxTestStepSize, agg.dirtyFilesEndTxNumMinimax())
}
