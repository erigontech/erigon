package state

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/stretchr/testify/require"
)

func testAccumulator(t *testing.T) StateChangeSetAccumulator {
	var stateChangeSetAccumulator StateChangeSetAccumulator
	for i := 0; i < 10; i++ {
		for idx := range stateChangeSetAccumulator.Diffs {
			// select a random number from 1 to 10
			prevStepBytes := [8]byte{byte(i)}
			txNumBytes := [8]byte{byte(i % 4)}
			randomInt := rand.Intn(99999999999)
			key := []byte(fmt.Sprintf("key%d", randomInt))
			value := []byte(fmt.Sprintf("value%d", randomInt))
			stateChangeSetAccumulator.Diffs[idx].DomainUpdate(key, nil, value, prevStepBytes[:], uint64(randomInt), txNumBytes[:])
		}
	}
	for i := 0; i < 10; i++ {
		for idx := range stateChangeSetAccumulator.InvertedIndiciesDiffs {
			// select a random number from 1 to 10
			txNumBytes := [8]byte{byte(i % 4)}
			randomInt := rand.Intn(99999999999)
			key := []byte(fmt.Sprintf("key%d", randomInt))
			stateChangeSetAccumulator.InvertedIndiciesDiffs[idx].InvertedIndexUpdate(key, txNumBytes[:])
		}
	}

	return stateChangeSetAccumulator

}

func TestChangesetAccumulator(t *testing.T) {
	stateChangeSetAccumulator := testAccumulator(t)
	changeset := stateChangeSetAccumulator.Changeset()
	for idx := range changeset.DomainDiffs {
		require.Equal(t, len(stateChangeSetAccumulator.Diffs[idx].prevValues), len(changeset.DomainDiffs[idx]))
	}
	for idx := range changeset.IdxDiffs {
		require.Equal(t, len(stateChangeSetAccumulator.InvertedIndiciesDiffs[idx].keyToTxNum), len(changeset.IdxDiffs[idx]))
	}

}

func TestDbAndChangeset(t *testing.T) {
	_, tx := memdb.NewTestTx(t)

	stateChangeSetAccumulator := testAccumulator(t)

	require.NoError(t, WriteDiffSet(tx, 0, common.Hash{}, stateChangeSetAccumulator.Changeset()))
	gotChangeset, ok, err := ReadDiffSet(tx, 0, common.Hash{})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, stateChangeSetAccumulator.Changeset(), gotChangeset)
}
