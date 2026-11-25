// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/mmap"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
)

func regionSlotKey(addr byte, slot int) []byte {
	k := make([]byte, length.Addr+length.Hash)
	k[0], k[length.Addr], k[length.Addr+1] = addr, byte(slot>>8), byte(slot)
	return k
}

func storageDomainTxWithContracts(t *testing.T, contracts byte, slots int) *DomainRoTx {
	t.Helper()
	perContract := map[byte]int{}
	for addr := byte(1); addr <= contracts; addr++ {
		perContract[addr] = slots
	}
	return storageDomainTxOfSteps(t, perContract)
}

func storageDomainTxOfSteps(t *testing.T, slotsPerStep ...map[byte]int) *DomainRoTx {
	t.Helper()
	const stepSize = 16
	db, d := testDbAndDomainOfStep(t, statecfg.Schema.StorageDomain, stepSize, log.New())
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	for step, contracts := range slotsPerStep {
		domainTx := d.beginForTests()
		writer := domainTx.NewWriter(db)
		for addr, slots := range contracts {
			for slot := step * 1000; slot < step*1000+slots; slot++ {
				require.NoError(t, writer.PutWithPrev(regionSlotKey(addr, slot), []byte{addr, byte(slot), 7, 7, 7, 7, 7, 7}, uint64(step*stepSize+1), nil))
			}
		}
		require.NoError(t, writer.Flush(t.Context(), tx))
		writer.Close()
		domainTx.Close()
		require.NoError(t, d.collateBuildIntegrate(t.Context(), kv.Step(step), tx, background.NewProgressSet()))
	}
	dt := d.beginForTests()
	t.Cleanup(dt.Close)
	require.Len(t, dt.files, len(slotsPerStep))
	return dt
}

func recordRegionAdvise(t *testing.T) *[][2]uint64 {
	var advised [][2]uint64
	orig := regionAdvise
	regionAdvise = func(_ *seg.Decompressor, from, to uint64) { advised = append(advised, [2]uint64{from, to}) }
	t.Cleanup(func() { regionAdvise = orig })
	return &advised
}

func contractRegion(t *testing.T, dt *DomainRoTx, addr byte) [2]uint64 {
	from, to, ok := dt.regionWords(0, regionSlotKey(addr, 0)[:length.Addr])
	require.True(t, ok)
	return [2]uint64{from, to}
}

func TestRegionAheadAdvisesAContractOnceAfterEnoughFileHits(t *testing.T) {
	dt := storageDomainTxWithContracts(t, 3, 200)
	dt.resetRegionHits()
	advised := recordRegionAdvise(t)
	read := func(addr byte, from, to int) {
		for slot := from; slot < to; slot++ {
			_, found, _, _, err := dt.getLatestFromFiles(regionSlotKey(addr, slot), nil, kv.NoStepBound)
			require.NoError(t, err)
			require.True(t, found)
		}
	}
	read(2, 0, regionAheadHits-1)
	read(1, 0, regionAheadHits-2)
	dt.regionAhead(0, regionSlotKey(1, 0))
	require.Empty(t, *advised)
	dt.regionAhead(0, regionSlotKey(2, 0))
	dt.regionAhead(0, regionSlotKey(2, 0))
	require.Equal(t, [][2]uint64{contractRegion(t, dt, 2)}, *advised)

	dt.probe = &mmap.Probe{}
	dt.regionAhead(0, regionSlotKey(1, 0))
	require.Len(t, *advised, 1)
	dt.probe = nil
	dt.regionAhead(0, regionSlotKey(1, 0))
	require.Equal(t, [][2]uint64{contractRegion(t, dt, 2), contractRegion(t, dt, 1)}, *advised)

	for file := 1; len(dt.regionHits) < regionAheadTracked; file++ {
		dt.regionAhead(file, regionSlotKey(3, 0))
	}
	for range regionAheadHits {
		dt.regionAhead(0, regionSlotKey(3, 0))
	}
	require.Equal(t, map[regionKey]uint32{
		{addr: [length.Addr]byte(regionSlotKey(3, 0)), file: 0}:  regionAheadHits,
		{addr: [length.Addr]byte(regionSlotKey(3, 0)), file: -1}: 0,
	}, dt.regionHits)
	require.Equal(t, [][2]uint64{contractRegion(t, dt, 2), contractRegion(t, dt, 1), contractRegion(t, dt, 3)}, *advised)
}

func TestRegionAheadSkipsARegionOverTheCap(t *testing.T) {
	dt := storageDomainTxWithContracts(t, 1, 200)
	advised := recordRegionAdvise(t)
	region := contractRegion(t, dt, 1)
	orig := regionAheadMaxBytes
	t.Cleanup(func() { regionAheadMaxBytes = orig })
	for _, c := range []struct {
		maxBytes uint64
		want     int
	}{{region[1] - region[0] - 1, 0}, {region[1] - region[0], 1}} {
		dt.resetRegionHits()
		*advised = nil
		regionAheadMaxBytes = c.maxBytes
		for range regionAheadHits {
			dt.regionAhead(0, regionSlotKey(1, 0))
		}
		require.Len(t, *advised, c.want, c.maxBytes)
	}
}

func TestRegionWordsSplitTheFileByContract(t *testing.T) {
	t.Parallel()
	dt := storageDomainTxWithContracts(t, 3, 200)
	d := dt.files[0].src.decompressor
	words := uint64(d.MakeGetter().Size())

	var end uint64
	for addr := byte(1); addr <= 3; addr++ {
		from, to, ok := dt.regionWords(0, regionSlotKey(addr, 0)[:length.Addr])
		require.True(t, ok)
		require.Equal(t, end, from)
		require.Greater(t, to, from)
		end = to
	}
	require.Equal(t, words, end)

	for _, absent := range []byte{0, 4} {
		_, _, ok := dt.regionWords(0, regionSlotKey(absent, 0)[:length.Addr])
		require.False(t, ok)
	}
}

func TestRegionAheadCountsHitsPerTx(t *testing.T) {
	dt := storageDomainTxWithContracts(t, 1, 200)
	advised := recordRegionAdvise(t)
	hit := func(n int) {
		for range n {
			dt.regionAhead(0, regionSlotKey(1, 0))
		}
	}
	dt.resetRegionHits()
	hit(regionAheadHits - 1)
	dt.resetRegionHits()
	hit(1)
	require.Empty(t, *advised, "hits before the reset must not count")
	hit(regionAheadHits - 1)
	require.Equal(t, [][2]uint64{contractRegion(t, dt, 1)}, *advised)
}

func TestRegionAheadIsOffUntilTheReaderResetsHits(t *testing.T) {
	dt := storageDomainTxWithContracts(t, 1, 200)
	advised := recordRegionAdvise(t)
	read := func() {
		for slot := range regionAheadHits {
			_, found, _, _, err := dt.getLatestFromFiles(regionSlotKey(1, slot), nil, kv.NoStepBound)
			require.NoError(t, err)
			require.True(t, found)
		}
	}
	read()
	require.Empty(t, *advised, "a reader that never reset its hits must not read ahead")
	require.Nil(t, dt.regionHits)
	dt.resetRegionHits()
	read()
	require.Equal(t, [][2]uint64{contractRegion(t, dt, 1)}, *advised)
}

type fileAdvice struct {
	file     string
	from, to uint64
}

func fileRegion(t *testing.T, dt *DomainRoTx, file int, addr byte) fileAdvice {
	from, to, ok := dt.regionWords(file, regionSlotKey(addr, 0)[:length.Addr])
	require.True(t, ok)
	return fileAdvice{dt.files[file].src.decompressor.FileName(), from, to}
}

func TestRegionAheadAlsoAdvisesTheContractInSmallFilesOncePerTx(t *testing.T) {
	dt := storageDomainTxOfSteps(t, map[byte]int{1: 200, 2: 200}, map[byte]int{1: 10, 2: 10}, map[byte]int{2: 5})
	var advised []fileAdvice
	orig := regionAdvise
	regionAdvise = func(d *seg.Decompressor, from, to uint64) {
		advised = append(advised, fileAdvice{d.FileName(), from, to})
	}
	t.Cleanup(func() { regionAdvise = orig })
	hit := func(file int, addr byte) {
		for range regionAheadHits {
			dt.regionAhead(file, regionSlotKey(addr, 0))
		}
	}
	dt.resetRegionHits()

	hit(0, 1)
	require.Equal(t, []fileAdvice{fileRegion(t, dt, 0, 1), fileRegion(t, dt, 1, 1)}, advised)
	hit(1, 1)
	require.Equal(t, []fileAdvice{fileRegion(t, dt, 0, 1), fileRegion(t, dt, 1, 1), fileRegion(t, dt, 1, 1)}, advised)

	advised = nil
	origSmall := regionAheadSmallBytes
	t.Cleanup(func() { regionAheadSmallBytes = origSmall })
	small, large := fileRegion(t, dt, 2, 2), fileRegion(t, dt, 1, 2)
	regionAheadSmallBytes = small.to - small.from
	require.Greater(t, large.to-large.from, regionAheadSmallBytes)
	hit(0, 2)
	require.Equal(t, []fileAdvice{fileRegion(t, dt, 0, 2), small}, advised)
}

func TestRegionAheadKeepsTheHitTableBoundedAfterASmallFilePass(t *testing.T) {
	dt := storageDomainTxWithContracts(t, 2, 200)
	recordRegionAdvise(t)
	dt.resetRegionHits()
	for file := 1; len(dt.regionHits) < regionAheadTracked-1; file++ {
		dt.regionAhead(file, regionSlotKey(2, 0))
	}
	for range regionAheadHits {
		dt.regionAhead(0, regionSlotKey(1, 0))
	}
	require.Len(t, dt.regionHits, regionAheadTracked+1)
	dt.regionAhead(0, regionSlotKey(2, 0))
	require.Len(t, dt.regionHits, 1)
}
