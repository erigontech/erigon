package state

import (
	"context"
	"encoding/binary"
	"math"
	"sync/atomic"
	"testing"

	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
)

func BenchmarkName2(b *testing.B) {
	b.Run("1", func(b *testing.B) {
		j := atomic.Int32{}
		for i := 0; i < b.N; i++ {
			j.Add(1)
		}
	})
	b.Run("2", func(b *testing.B) {
		j := &atomic.Int32{}
		for i := 0; i < b.N; i++ {
			j.Add(1)
		}
	})
}

func TestLocality(t *testing.T) {
	logger := log.New()
	ctx, require := context.Background(), require.New(t)
	const Module uint64 = 31
	path, db, ii, txs := filledInvIndexOfSize(t, 300, 4, Module, logger)
	mergeInverted(t, db, ii, txs)
	ic := ii.MakeContext()
	defer ic.Close()
	li, _ := NewLocalityIndex(path, path, 4, "inv", logger)
	defer li.Close()
	err := li.BuildMissedIndices(ctx, ic)
	require.NoError(err)
	t.Run("locality iterator", func(t *testing.T) {
		ic := ii.MakeContext()
		defer ic.Close()
		it := ic.iterateKeysLocality(math.MaxUint64)
		require.True(it.HasNext())
		key, bitmap := it.Next()
		require.Equal(uint64(2), binary.BigEndian.Uint64(key))
		require.Equal([]uint64{0, 1}, bitmap)
		require.True(it.HasNext())
		key, bitmap = it.Next()
		require.Equal(uint64(3), binary.BigEndian.Uint64(key))
		require.Equal([]uint64{0, 1}, bitmap)

		var last []byte
		for it.HasNext() {
			key, _ = it.Next()
			last = key
		}
		require.Equal(Module, binary.BigEndian.Uint64(last))
	})

	files, err := li.buildFiles(ctx, ic, ii.endTxNumMinimax()/ii.aggregationStep)
	require.NoError(err)
	defer files.Close()
	t.Run("locality index: get full bitamp", func(t *testing.T) {
		res, err := files.bm.At(0)
		require.NoError(err)
		require.Equal([]uint64{0, 1}, res)
		res, err = files.bm.At(1)
		require.NoError(err)
		require.Equal([]uint64{0, 1}, res)
		res, err = files.bm.At(32) //too big, must error
		require.Error(err)
		require.Empty(res)
	})

	t.Run("locality index: search from given position", func(t *testing.T) {
		fst, snd, ok1, ok2, err := files.bm.First2At(0, 1)
		require.NoError(err)
		require.True(ok1)
		require.False(ok2)
		require.Equal(uint64(1), fst)
		require.Zero(snd)
	})
	t.Run("locality index: search from given position in future", func(t *testing.T) {
		fst, snd, ok1, ok2, err := files.bm.First2At(0, 2)
		require.NoError(err)
		require.False(ok1)
		require.False(ok2)
		require.Zero(fst)
		require.Zero(snd)
	})
	t.Run("locality index: lookup", func(t *testing.T) {
		liCtx := li.MakeContext()
		defer liCtx.Close(logger)
		var k [8]byte
		binary.BigEndian.PutUint64(k[:], 1)
		v1, v2, from, ok1, ok2 := li.lookupIdxFiles(liCtx, k[:], 1*li.aggregationStep*StepsInBiggestFile)
		require.True(ok1)
		require.False(ok2)
		require.Equal(uint64(1*StepsInBiggestFile), v1)
		require.Equal(uint64(0*StepsInBiggestFile), v2)
		require.Equal(2*li.aggregationStep*StepsInBiggestFile, from)
	})
}
