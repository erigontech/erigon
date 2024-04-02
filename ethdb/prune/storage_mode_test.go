package prune

import (
	"math/rand"
	"strconv"
	"testing"

	"github.com/gateway-fm/cdk-erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/stretchr/testify/assert"
)

func TestSetStorageModeIfNotExist(t *testing.T) {
	_, tx := memdb.NewTestTx(t)
	prune, err := Get(tx)
	assert.NoError(t, err)
	assert.Equal(t, Mode{true, Distance(math.MaxUint64), Distance(math.MaxUint64),
		Distance(math.MaxUint64), Distance(math.MaxUint64), Experiments{}}, prune)

	err = setIfNotExist(tx, Mode{true, Distance(1), Distance(2),
		Before(3), Before(4), Experiments{}})
	assert.NoError(t, err)

	prune, err = Get(tx)
	assert.NoError(t, err)
	assert.Equal(t, Mode{true, Distance(1), Distance(2),
		Before(3), Before(4), Experiments{}}, prune)
}

var distanceTests = []struct {
	stageHead uint64
	pruneTo   uint64
	expected  uint64
}{
	{3_000_000, 1, 2_999_999},
	{3_000_000, 4_000_000, 0},
	{3_000_000, math.MaxUint64, 0},
	{3_000_000, 1_000_000, 2_000_000},
}

func TestDistancePruneTo(t *testing.T) {
	for _, tt := range distanceTests {
		t.Run(strconv.FormatUint(tt.pruneTo, 10), func(t *testing.T) {
			stageHead := tt.stageHead
			d := Distance(tt.pruneTo)
			pruneTo := d.PruneTo(stageHead)

			if pruneTo != tt.expected {
				t.Errorf("got %d, want %d", pruneTo, tt.expected)
			}
		})
	}
}

var beforeTests = []struct {
	pruneTo  uint64
	expected uint64
}{
	{0, 0},
	{1_000_000, 999_999},
}

func TestBeforePruneTo(t *testing.T) {
	for _, tt := range beforeTests {
		t.Run(strconv.FormatUint(tt.pruneTo, 10), func(t *testing.T) {
			stageHead := uint64(rand.Int63n(10_000_000))
			b := Before(tt.pruneTo)
			pruneTo := b.PruneTo(stageHead)

			if pruneTo != tt.expected {
				t.Errorf("got %d, want %d", pruneTo, tt.expected)
			}
		})
	}
}
