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
		Span{
			Id:         0,
			StartBlock: 0,
			EndBlock:   999,
		},
		Span{
			Id:         1,
			StartBlock: 1000,
			EndBlock:   1999,
		},
		Span{
			Id:         2,
			StartBlock: 2000,
			EndBlock:   2999,
		},
		Span{
			Id:         3,
			StartBlock: 3000,
			EndBlock:   3999,
		},
		Span{
			Id:         4,
			StartBlock: 4000,
			EndBlock:   4999,
		},
		Span{
			Id:         5,
			StartBlock: 5000,
			EndBlock:   5999,
		},
		Span{
			Id:         6,
			StartBlock: 6000,
			EndBlock:   6999,
		},
		Span{
			Id:         7,
			StartBlock: 7000,
			EndBlock:   7999,
		},
		Span{
			Id:         8,
			StartBlock: 8000,
			EndBlock:   8999,
		},
		Span{
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
			assert.Equal(t, actualId, span.RawId())
		}
	}
}

func TestSpanRangeIndexOverlappingSpans(t *testing.T) {
	t.Parallel()
	test := newSpanRangeIndexTest(t)
	ctx := test.ctx

	// span data that is irregular, containing possible span rotations
	var spans = []Span{
		Span{
			Id:         0,
			StartBlock: 0,
			EndBlock:   999,
		},
		Span{
			Id:         1,
			StartBlock: 5,
			EndBlock:   1999,
		},
		Span{
			Id:         2,
			StartBlock: 1988,
			EndBlock:   2999,
		},
		Span{
			Id:         3,
			StartBlock: 3000,
			EndBlock:   3999,
		},
		Span{
			Id:         4,
			StartBlock: 3500,
			EndBlock:   4999,
		},
		Span{
			Id:         5,
			StartBlock: 5000,
			EndBlock:   5999,
		},
		Span{
			Id:         6,
			StartBlock: 5500,
			EndBlock:   6999,
		},
		Span{
			Id:         7,
			StartBlock: 7000,
			EndBlock:   7999,
		},
		Span{
			Id:         8,
			StartBlock: 7001,
			EndBlock:   8999,
		},
		Span{
			Id:         9,
			StartBlock: 7002,
			EndBlock:   9999,
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
		5:    1,
		999:  1,
		100:  1,
		1988: 2,
		1999: 2,
		3200: 3,
		3500: 4,
		3600: 4,
		3988: 4,
		5200: 5,
		5900: 6,
		6501: 6,
		7000: 7,
		7001: 8,
		7002: 9,
		8000: 9,
		8998: 9,
		9000: 9,
		9998: 9,
		9999: 9,
	}

	for blockNum, expectedId := range expectedLookupVals {
		actualId, found, err := test.index.Lookup(ctx, blockNum)
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, actualId, expectedId)
	}

	// additional test cases for out of range lookups
	_, _, err := test.index.Lookup(ctx, 12000)
	require.Error(t, err)

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
