package state

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestScanStaticFilesLocality(t *testing.T) {
	logger, baseName := log.New(), "test"

	t.Run("new", func(t *testing.T) {
		ii := &InvertedIndex{filenameBase: baseName, aggregationStep: 1, dir: "", tmpdir: "", logger: logger}
		ii.enableLocalityIndex()
		files := []string{
			"test.0-1.l",
			"test.1-2.l",
			"test.0-4.l",
			"test.2-3.l",
			"test.3-4.l",
			"test.4-5.l",
		}
		ii.warmLocalityIdx.scanStateFiles(files)
		require.Equal(t, 4, int(ii.warmLocalityIdx.file.startTxNum))
		require.Equal(t, 5, int(ii.warmLocalityIdx.file.endTxNum))
		ii.coldLocalityIdx.scanStateFiles(files)
		require.Equal(t, 4, int(ii.coldLocalityIdx.file.startTxNum))
		require.Equal(t, 5, int(ii.coldLocalityIdx.file.endTxNum))
	})
	t.Run("overlap", func(t *testing.T) {
		ii := &InvertedIndex{filenameBase: baseName, aggregationStep: 1, dir: "", tmpdir: "", logger: logger}
		ii.enableLocalityIndex()
		ii.warmLocalityIdx.scanStateFiles([]string{
			"test.0-50.l",
			"test.0-70.l",
			"test.64-70.l",
		})
		require.Equal(t, 64, int(ii.warmLocalityIdx.file.startTxNum))
		require.Equal(t, 70, int(ii.warmLocalityIdx.file.endTxNum))
		ii.coldLocalityIdx.scanStateFiles([]string{
			"test.0-32.l",
			"test.0-64.l",
		})
		require.Equal(t, 0, int(ii.coldLocalityIdx.file.startTxNum))
		require.Equal(t, 64, int(ii.coldLocalityIdx.file.endTxNum))
	})
}

func TestLocality(t *testing.T) {
	logger := log.New()
	ctx, require := context.Background(), require.New(t)
	const Module uint64 = 31
	aggStep := uint64(4)
	coldFiles := uint64(2)
	db, ii, txs := filledInvIndexOfSize(t, 300, aggStep, Module, logger)
	mergeInverted(t, db, ii, txs)

	{ //prepare
		ii.withLocalityIndex = true
		require.NoError(ii.enableLocalityIndex())

		ic := ii.MakeContext()
		g := &errgroup.Group{}
		ii.BuildMissedIndices(ctx, g, background.NewProgressSet())
		require.NoError(g.Wait())
		require.NoError(ic.BuildOptionalMissedIndices(ctx, background.NewProgressSet()))
		ic.Close()
	}

	t.Run("locality iterator", func(t *testing.T) {
		ic := ii.MakeContext()
		defer ic.Close()
		it := ic.iterateKeysLocality(0, coldFiles*StepsInColdFile, nil)
		require.True(it.HasNext())
		key, bitmap := it.Next()
		require.Equal(uint64(1), binary.BigEndian.Uint64(key))
		require.Equal([]uint64{0 * StepsInColdFile, 1 * StepsInColdFile}, bitmap)
		require.True(it.HasNext())
		key, bitmap = it.Next()
		require.Equal(uint64(2), binary.BigEndian.Uint64(key))
		require.Equal([]uint64{0 * StepsInColdFile, 1 * StepsInColdFile}, bitmap)

		var last []byte
		for it.HasNext() {
			key, _ = it.Next()
			last = key
		}
		require.Equal(Module, binary.BigEndian.Uint64(last))
	})

	t.Run("locality index: getBeforeTxNum full bitamp", func(t *testing.T) {
		ic := ii.MakeContext()
		defer ic.Close()

		res, err := ic.coldLocality.bm.At(0)
		require.NoError(err)
		require.Equal([]uint64{0, 1}, res)
		res, err = ic.coldLocality.bm.At(1)
		require.NoError(err)
		require.Equal([]uint64{0, 1}, res)
		res, err = ic.coldLocality.bm.At(32) //too big, must error
		require.Error(err)
		require.Empty(res)
	})

	t.Run("locality index: search from given position", func(t *testing.T) {
		ic := ii.MakeContext()
		defer ic.Close()
		fst, snd, ok1, ok2, err := ic.coldLocality.bm.First2At(0, 1)
		require.NoError(err)
		require.True(ok1)
		require.False(ok2)
		require.Equal(uint64(1), fst)
		require.Zero(snd)
	})
	t.Run("locality index: search from given position in future", func(t *testing.T) {
		ic := ii.MakeContext()
		defer ic.Close()
		fst, snd, ok1, ok2, err := ic.coldLocality.bm.First2At(0, 2)
		require.NoError(err)
		require.False(ok1)
		require.False(ok2)
		require.Zero(fst)
		require.Zero(snd)
	})
	t.Run("locality index: lookup", func(t *testing.T) {
		ic := ii.MakeContext()
		defer ic.Close()
		k := hexutility.EncodeTs(1)
		v1, v2, from, ok1, ok2 := ic.coldLocality.lookupIdxFiles(k, 1*ic.ii.aggregationStep*StepsInColdFile)
		require.True(ok1)
		require.False(ok2)
		require.Equal(uint64(1*StepsInColdFile), v1)
		require.Equal(uint64(0*StepsInColdFile), v2)
		require.Equal(2*ic.ii.aggregationStep*StepsInColdFile, from)
	})
}

func TestLocalityDomain(t *testing.T) {
	logger := log.New()
	ctx, require := context.Background(), require.New(t)
	aggStep := 2
	coldFiles := 3
	coldSteps := coldFiles * StepsInColdFile
	txsInColdFile := aggStep * StepsInColdFile
	keyCount, txCount := uint64(6), coldFiles*txsInColdFile+aggStep*16
	db, dom, data := filledDomainFixedSize(t, keyCount, uint64(txCount), uint64(aggStep), logger)
	collateAndMerge(t, db, nil, dom, uint64(txCount))

	{ //prepare
		dom.withLocalityIndex = true
		require.NoError(dom.enableLocalityIndex())

		dc := dom.MakeContext()
		g := &errgroup.Group{}
		dom.BuildMissedIndices(ctx, g, background.NewProgressSet())
		require.NoError(g.Wait())
		err := dc.BuildOptionalMissedIndices(ctx, background.NewProgressSet())
		require.NoError(err)
		dc.Close()
	}

	_, _ = ctx, data
	t.Run("locality iterator", func(t *testing.T) {
		dc := dom.MakeContext()
		defer dc.Close()
		require.Equal(coldSteps, int(dc.maxColdStep()))
		var last []byte

		it := dc.hc.ic.iterateKeysLocality(0, uint64(coldSteps), nil)
		require.True(it.HasNext())
		key, bitmap := it.Next()
		require.Equal(uint64(0), binary.BigEndian.Uint64(key))
		require.Equal([]uint64{0 * StepsInColdFile}, bitmap)
		require.True(it.HasNext())
		key, bitmap = it.Next()
		require.Equal(uint64(1), binary.BigEndian.Uint64(key))
		require.Equal([]uint64{1 * StepsInColdFile, 2 * StepsInColdFile}, bitmap)

		for it.HasNext() {
			last, _ = it.Next()
		}
		require.Equal(coldFiles-1, int(binary.BigEndian.Uint64(last)))

		it = dc.hc.ic.iterateKeysLocality(dc.hc.ic.maxColdStep(), dc.hc.ic.maxWarmStep()+1, nil)
		require.True(it.HasNext())
		key, bitmap = it.Next()
		require.Equal(2, int(binary.BigEndian.Uint64(key)))
		require.Equal([]uint64{uint64(coldSteps), uint64(coldSteps + 8), uint64(coldSteps + 8 + 4), uint64(coldSteps + 8 + 4 + 2)}, bitmap)
		require.True(it.HasNext())
		key, bitmap = it.Next()
		require.Equal(3, int(binary.BigEndian.Uint64(key)))
		require.Equal([]uint64{uint64(coldSteps), uint64(coldSteps + 8), uint64(coldSteps + 8 + 4), uint64(coldSteps + 8 + 4 + 2)}, bitmap)

		last = nil
		for it.HasNext() {
			last, _ = it.Next()
		}
		require.Equal(int(keyCount-1), int(binary.BigEndian.Uint64(last)))

	})

	t.Run("locality index: bitmap all data check", func(t *testing.T) {
		dc := dom.MakeContext()
		defer dc.Close()
		res, err := dc.hc.ic.coldLocality.bm.At(0)
		require.NoError(err)
		require.Equal([]uint64{0}, res)
		res, err = dc.hc.ic.coldLocality.bm.At(1)
		require.NoError(err)
		require.Equal([]uint64{1, 2}, res)
		res, err = dc.hc.ic.coldLocality.bm.At(keyCount) //too big, must error
		require.Error(err)
		require.Empty(res)
	})

	t.Run("locality index: search from given position", func(t *testing.T) {
		dc := dom.MakeContext()
		defer dc.Close()
		fst, snd, ok1, ok2, err := dc.hc.ic.coldLocality.bm.First2At(1, 1)
		require.NoError(err)
		require.True(ok1)
		require.True(ok2)
		require.Equal(1, int(fst))
		require.Equal(2, int(snd))

		fst, snd, ok1, ok2, err = dc.hc.ic.coldLocality.bm.First2At(1, 2)
		require.NoError(err)
		require.True(ok1)
		require.False(ok2)
		require.Equal(2, int(fst))
		require.Equal(0, int(snd))

		fst, snd, ok1, ok2, err = dc.hc.ic.coldLocality.bm.First2At(2, 1)
		require.NoError(err)
		require.True(ok1)
		require.False(ok2)
		require.Equal(uint64(2), fst)
		require.Zero(snd)

		fst, snd, ok1, ok2, err = dc.hc.ic.coldLocality.bm.First2At(0, 1)
		require.NoError(err)
		require.False(ok1)
		require.False(ok2)
	})
	t.Run("locality index: bitmap operations", func(t *testing.T) {
		dc := dom.MakeContext()
		defer dc.Close()
		_, _, ok1, ok2, err := dc.hc.ic.coldLocality.bm.First2At(0, 2)
		require.NoError(err)
		require.False(ok1)
		require.False(ok2)

		_, _, ok1, ok2, err = dc.hc.ic.coldLocality.bm.First2At(2, 3)
		require.NoError(err)
		require.False(ok1)
		require.False(ok2)

		v1, ok1, err := dc.hc.ic.coldLocality.bm.LastAt(0)
		require.NoError(err)
		require.True(ok1)
		require.Equal(0, int(v1))

		v1, ok1, err = dc.hc.ic.coldLocality.bm.LastAt(1)
		require.NoError(err)
		require.True(ok1)
		require.Equal(2, int(v1))

		_, ok1, err = dc.hc.ic.coldLocality.bm.LastAt(3)
		require.NoError(err)
		require.False(ok1)
	})
	t.Run("locality index: lookup", func(t *testing.T) {
		dc := dom.MakeContext()
		defer dc.Close()
		to := dc.hc.ic.coldLocality.indexedTo()
		require.Equal(coldFiles*txsInColdFile, int(to))

		v1, v2, from, ok1, ok2 := dc.hc.ic.coldLocality.lookupIdxFiles(hexutility.EncodeTs(0), 0)
		require.True(ok1)
		require.False(ok2)
		require.Equal(uint64(0*StepsInColdFile), v1)
		require.Equal(txsInColdFile*coldFiles, int(from))

		v1, v2, from, ok1, ok2 = dc.hc.ic.coldLocality.lookupIdxFiles(hexutility.EncodeTs(1), 0)
		require.True(ok1)
		require.True(ok2)
		require.Equal(uint64(1*StepsInColdFile), v1)
		require.Equal(uint64(2*StepsInColdFile), v2)
		require.Equal(txsInColdFile*coldFiles, int(from))
	})
	t.Run("domain.getLatestFromFiles", func(t *testing.T) {
		dc := dom.MakeContext()
		defer dc.Close()
		fmt.Printf("--case0\n")
		v, ok, err := dc.getLatestFromFiles(hexutility.EncodeTs(0))
		require.NoError(err)
		require.True(ok)
		require.Equal(1*txsInColdFile-1, int(binary.BigEndian.Uint64(v)))

		fmt.Printf("--case1\n")
		v, ok, err = dc.getLatestFromFiles(hexutility.EncodeTs(1))
		require.NoError(err)
		require.True(ok)
		require.Equal(3*txsInColdFile-1, int(binary.BigEndian.Uint64(v)))

		fmt.Printf("--case2\n")
		v, ok, err = dc.getLatestFromFiles(hexutility.EncodeTs(2))
		require.NoError(err)
		require.True(ok)
		require.Equal(221, int(binary.BigEndian.Uint64(v)))

		fmt.Printf("--case5\n")
		v, ok, err = dc.getLatestFromFiles(hexutility.EncodeTs(5))
		require.NoError(err)
		require.True(ok)
		require.Equal(221, int(binary.BigEndian.Uint64(v)))
	})
}
