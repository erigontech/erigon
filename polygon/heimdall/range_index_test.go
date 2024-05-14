package heimdall

import (
	"context"
	"testing"

	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type rangeIndexTest struct {
	index  *RangeIndex
	ctx    context.Context
	logger log.Logger
}

func newRangeIndexTest(t *testing.T) rangeIndexTest {
	tmpDir := t.TempDir()
	ctx := context.Background()
	logger := log.New()
	index, err := NewRangeIndex(ctx, tmpDir, logger)
	require.NoError(t, err)

	t.Cleanup(index.Close)

	return rangeIndexTest{
		index:  index,
		ctx:    ctx,
		logger: logger,
	}
}

func TestRangeIndexEmpty(t *testing.T) {
	test := newRangeIndexTest(t)
	actualId, err := test.index.Lookup(test.ctx, 1000)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), actualId)
}

func TestRangeIndex(t *testing.T) {
	test := newRangeIndexTest(t)
	ctx := test.ctx

	ranges := []ClosedRange{
		{100, 200 - 1},
		{200, 500 - 1},
		{500, 1000 - 1},
		{1000, 1200 - 1},
		{1200, 1500 - 1},
	}

	for i, r := range ranges {
		require.NoError(t, test.index.Put(ctx, r, uint64(i+1)))
	}

	examples := map[uint64]uint64{
		100:  1,
		101:  1,
		102:  1,
		150:  1,
		199:  1,
		200:  2,
		201:  2,
		202:  2,
		300:  2,
		498:  2,
		499:  2,
		500:  3,
		501:  3,
		502:  3,
		900:  3,
		998:  3,
		999:  3,
		1000: 4,
		1001: 4,
		1002: 4,
		1100: 4,
		1199: 4,
		1200: 5,
		1201: 5,
		1400: 5,
		1499: 5,
		1500: 0,
		1501: 0,
		2000: 0,
		5000: 0,
	}

	for blockNum, expectedId := range examples {
		actualId, err := test.index.Lookup(ctx, blockNum)
		require.NoError(t, err)
		assert.Equal(t, expectedId, actualId)
	}
}
