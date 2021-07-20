package prune

import (
	"testing"

	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/ethdb/kv"
	"github.com/ledgerwatch/erigon/params"
	"github.com/stretchr/testify/assert"
)

func TestSetStorageModeIfNotExist(t *testing.T) {
	_, tx := kv.NewTestTx(t)
	prune, err := Get(tx)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, prune, Mode{Initialised: true, TxIndex: params.FullImmutabilityThreshold,
		CallTraces: params.FullImmutabilityThreshold, History: params.FullImmutabilityThreshold,
		Receipts: params.FullImmutabilityThreshold, Experiments: Experiments{TEVM: false}})

	err = SetIfNotExist(tx, Mode{
		true,
		math.MaxUint64,
		math.MaxUint64,
		math.MaxUint64,
		math.MaxUint64,
		Experiments{TEVM: false},
	})
	if err != nil {
		t.Fatal(err)
	}

	prune, err = Get(tx)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, prune, Mode{
		true,
		math.MaxUint64,
		math.MaxUint64,
		math.MaxUint64,
		math.MaxUint64,
		Experiments{TEVM: false},
	})
}
