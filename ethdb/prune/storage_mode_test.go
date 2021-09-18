package prune

import (
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/stretchr/testify/assert"
)

func TestSetStorageModeIfNotExist(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	prune, err := Get(tx)
	assert.NoError(t, err)
	assert.Equal(t, Mode{true, math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxUint64, Experiments{TEVM: false}}, prune)

	err = SetIfNotExist(tx, Mode{true, 1, 2, 3, 4, Experiments{TEVM: false}})
	assert.NoError(t, err)

	prune, err = Get(tx)
	assert.NoError(t, err)
	assert.Equal(t, Mode{true, 1, 2, 3, 4, Experiments{TEVM: false}}, prune)
}
