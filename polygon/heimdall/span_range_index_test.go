package heimdall

import (
	"context"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/polygon/polygoncommon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type spanRangeIndexTest struct {
	index  *spanRangeIndex
	ctx    context.Context
	logger log.Logger
}

func newSpanRangeIndexTest(t *testing.T) spanRangeIndexTest {
	tmpDir := t.TempDir()
	ctx, cancel := context.WithCancel(t.Context())
	logger := log.New()

	db, err := mdbx.New(kv.HeimdallDB, logger).
		InMem(tmpDir).
		WithTableCfg(func(_ kv.TableCfg) kv.TableCfg { return kv.TableCfg{kv.BorSpansIndex: {}} }).
		MapSize(1 * datasize.GB).
		Open(ctx)

	require.NoError(t, err)

	index := NewSpanRangeIndex(polygoncommon.AsDatabase(db), kv.BorSpansIndex)

	t.Cleanup(func() { db.Close(); cancel() })

	return spanRangeIndexTest{
		index:  index,
		ctx:    ctx,
		logger: logger,
	}
}

func TestSpanRangeIndexEmpty(t *testing.T) {
	t.Parallel()
	test := newSpanRangeIndexTest(t)
	_, found, err := test.index.Lookup(test.ctx, 1000)
	require.NoError(t, err)
	assert.False(t, found)
}

func TestSpanRangeIndexNonOverlappingSpans(t *testing.T) {
	t.Parallel()
	test := newSpanRangeIndexTest(t)
	ctx := test.ctx

	spans := []Span{
		{
			Id:         0,
			StartBlock: 0,
			EndBlock:   999,
		},
		{
			Id:         1,
			StartBlock: 1000,
			EndBlock:   1999,
		},
		{
			Id:         2,
			StartBlock: 2000,
			EndBlock:   2999,
		},
		{
			Id:         3,
			StartBlock: 3000,
			EndBlock:   3999,
		},
		{
			Id:         4,
			StartBlock: 4000,
			EndBlock:   4999,
		},
		{
			Id:         5,
			StartBlock: 5000,
			EndBlock:   5999,
		},
		{
			Id:         6,
			StartBlock: 6000,
			EndBlock:   6999,
		},
		{
			Id:         7,
			StartBlock: 7000,
			EndBlock:   7999,
		},
		{
			Id:         8,
			StartBlock: 8000,
			EndBlock:   8999,
		},
		{
			Id:         9,
			StartBlock: 9000,
			EndBlock:   9999,
		},
	}

	for _, span := range spans {
		spanId := span.RawId()
		r := ClosedRange{Start: span.StartBlock, End: span.EndBlock}
		require.NoError(t, test.index.Put(ctx, r, spanId))
	}

	for _, span := range spans {
		blockNumsToTest := []uint64{span.StartBlock, (span.StartBlock + span.EndBlock) / 2, span.EndBlock}
		for _, blockNum := range blockNumsToTest {
			actualId, found, err := test.index.Lookup(ctx, blockNum)
			require.NoError(t, err)
			require.True(t, found)
			assert.Equal(t, span.RawId(), actualId)
		}
	}
}

func TestSpanRangeIndexSpanRotation(t *testing.T) {
	t.Parallel()
	test := newSpanRangeIndexTest(t)
	ctx := test.ctx

	// span data that is irregular, containing possible span rotations
	var spans = []Span{
		{
			Id:         0,
			StartBlock: 0,
			EndBlock:   999,
		},
		{
			Id:         1, // new span announced
			StartBlock: 1000,
			EndBlock:   1999,
		},
		{
			Id:         2, // span rotation
			StartBlock: 5,
			EndBlock:   1999,
		},
	}

	for _, span := range spans {
		spanId := span.RawId()
		r := ClosedRange{Start: span.StartBlock, End: span.EndBlock}
		require.NoError(t, test.index.Put(ctx, r, spanId))
	}

	// expected blockNum -> spanId lookups
	expectedLookupVals := map[uint64]uint64{
		0:    0,
		1:    0,
		4:    0,
		5:    2,
		1000: 2,
	}

	for blockNum, expectedId := range expectedLookupVals {
		actualId, found, err := test.index.Lookup(ctx, blockNum)
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, expectedId, actualId, "Lookup(blockNum=%d) returned %d instead of %d", blockNum, actualId, expectedId)
	}

	// additional test cases for out of range lookups
	_, _, err := test.index.Lookup(ctx, 12000)
	require.Error(t, err)

}

func TestSpanRangeIndexComplicatedSpanRotations(t *testing.T) {
	t.Parallel()
	test := newSpanRangeIndexTest(t)
	ctx := test.ctx

	// span data that is irregular, containing possible span rotations
	var spans = []Span{
		{ // first  span
			Id:         0,
			StartBlock: 0,
			EndBlock:   999,
		},
		{ // new span announced
			Id:         1,
			StartBlock: 1000,
			EndBlock:   1999,
		},
		{ // span rotation
			Id:         2,
			StartBlock: 4,
			EndBlock:   1999,
		},
		{ // span rotation
			Id:         3,
			StartBlock: 5,
			EndBlock:   1999,
		},
		{ // span rotation
			Id:         4,
			StartBlock: 6,
			EndBlock:   1999,
		},
		{ // new span announced
			Id:         5,
			StartBlock: 2000,
			EndBlock:   2999,
		},
		{ // span rotation
			Id:         6,
			StartBlock: 11,
			EndBlock:   1999,
		},
		{ // new span announced, this will have duplicate StartBlock
			Id:         7,
			StartBlock: 2000,
			EndBlock:   2999,
		},
		{ // span rotation
			Id:         8,
			StartBlock: 3100,
			EndBlock:   4999,
		},
		{ // span rotation
			Id:         9,
			StartBlock: 4600,
			EndBlock:   5999,
		},
		{ // span rotation
			Id:         10,
			StartBlock: 5400,
			EndBlock:   6999,
		},
		{ // new span announced
			Id:         11,
			StartBlock: 7000,
			EndBlock:   7999,
		},
	}

	for _, span := range spans {
		spanId := span.RawId()
		r := ClosedRange{Start: span.StartBlock, End: span.EndBlock}
		require.NoError(t, test.index.Put(ctx, r, spanId))
	}

	// expected blockNum -> spanId lookups
	expectedLookupVals := map[uint64]uint64{
		3:    0,
		4:    2,
		5:    3,
		6:    4,
		7:    4,
		12:   6,
		11:   6,
		100:  6,
		1000: 6,
		2000: 7,
		3101: 8,
		4800: 9,
	}

	for blockNum, expectedId := range expectedLookupVals {
		actualId, found, err := test.index.Lookup(ctx, blockNum)
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, expectedId, actualId, "Lookup(blockNum=%d) returned %d instead of %d", blockNum, actualId, expectedId)

	}
}

func TestSpanRangeIndexEvenMoreComplicatedSpanRotations(t *testing.T) {
	t.Parallel()
	test := newSpanRangeIndexTest(t)
	ctx := test.ctx

	// span data that is irregular, containing possible span rotations
	var spans = []Span{
		{
			Id:         7,
			StartBlock: 1000,
			EndBlock:   2999,
		},
		{
			Id:         8,
			StartBlock: 3000, // new span announced
			EndBlock:   4999,
		},
		{
			Id:         9, // span rotation
			StartBlock: 1005,
			EndBlock:   4999,
		},
		{
			Id:         10, // new span announced
			StartBlock: 5000,
			EndBlock:   6999,
		},
		{
			Id:         11, // span rotation
			StartBlock: 4997,
			EndBlock:   6999,
		},
	}

	for _, span := range spans {
		spanId := span.RawId()
		r := ClosedRange{Start: span.StartBlock, End: span.EndBlock}
		require.NoError(t, test.index.Put(ctx, r, spanId))
	}

	// expected blockNum -> spanId lookups
	expectedLookupVals := map[uint64]uint64{
		1000: 7,
		3500: 9,
		4000: 9,
		4996: 9,
		5000: 11,
	}

	for blockNum, expectedId := range expectedLookupVals {
		actualId, found, err := test.index.Lookup(ctx, blockNum)
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, expectedId, actualId, "Lookup(blockNum=%d) returned %d instead of %d", blockNum, actualId, expectedId)

	}
}

func TestSpanRangeIndexSingletonLookup(t *testing.T) {
	t.Parallel()
	test := newSpanRangeIndexTest(t)
	ctx := test.ctx
	span := &Span{Id: 0, StartBlock: 0, EndBlock: 6400}
	spanId := span.RawId()
	r := ClosedRange{Start: span.StartBlock, End: span.EndBlock}
	require.NoError(t, test.index.Put(ctx, r, spanId))

	// Lookup at 0 should be successful
	id, found, err := test.index.Lookup(ctx, 0)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, id, uint64(0))

	// Lookup at 1200 should be successful
	id, found, err = test.index.Lookup(ctx, 1200)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, id, uint64(0))

	// Lookup at 6400 should be successful
	id, found, err = test.index.Lookup(ctx, 6400)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, id, uint64(0))

	// Lookup at 6401 should throw an error
	_, _, err = test.index.Lookup(ctx, 6401)
	require.Error(t, err)

}
