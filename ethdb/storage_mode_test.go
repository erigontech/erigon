package ethdb_test

import (
	"testing"

	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/kv"
	"github.com/ledgerwatch/erigon/params"
	"github.com/stretchr/testify/assert"
)

func TestSetStorageModeIfNotExist(t *testing.T) {
	_, tx := kv.NewTestTx(t)
	sm, err := ethdb.GetPruneModeFromDB(tx)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, sm, ethdb.Prune{Initialised: true, TxIndex: params.FullImmutabilityThreshold,
		CallTraces: params.FullImmutabilityThreshold, History: params.FullImmutabilityThreshold,
		Receipts: params.FullImmutabilityThreshold, Experiments: ethdb.Experiments{TEVM: false}})

	err = ethdb.SetPruneModeIfNotExist(tx, ethdb.Prune{
		true,
		math.MaxUint64,
		math.MaxUint64,
		math.MaxUint64,
		math.MaxUint64,
		ethdb.Experiments{TEVM: false},
	})
	if err != nil {
		t.Fatal(err)
	}

	sm, err = ethdb.GetPruneModeFromDB(tx)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, sm, ethdb.Prune{
		true,
		math.MaxUint64,
		math.MaxUint64,
		math.MaxUint64,
		math.MaxUint64,
		ethdb.Experiments{TEVM: false},
	})
}
