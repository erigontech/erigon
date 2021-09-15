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
	assert.Equal(t, Mode{true, Distance(math.MaxUint64), Distance(math.MaxUint64),
		Distance(math.MaxUint64), Distance(math.MaxUint64), Experiments{TEVM: false}}, prune)

	err = SetIfNotExist(tx, Mode{true, Distance(1), Distance(2),
		Before(3), Before(4), Experiments{TEVM: false}})
	assert.NoError(t, err)

	prune, err = Get(tx)
	assert.NoError(t, err)
	assert.Equal(t, Mode{true, Distance(1), Distance(2),
		Before(3), Before(4), Experiments{TEVM: false}}, prune)
}
