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
	db, d := testDbAndDomainOfStep(t, statecfg.Schema.StorageDomain, 16, log.New())
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	domainTx := d.beginForTests()
	writer := domainTx.NewWriter(db)
	for addr := byte(1); addr <= contracts; addr++ {
		for slot := range slots {
			require.NoError(t, writer.PutWithPrev(regionSlotKey(addr, slot), []byte{addr, byte(slot), 7, 7, 7, 7, 7, 7}, 1, nil))
		}
	}
	require.NoError(t, writer.Flush(t.Context(), tx))
	writer.Close()
	domainTx.Close()
	require.NoError(t, d.collateBuildIntegrate(t.Context(), 0, tx, background.NewProgressSet()))
	dt := d.beginForTests()
	t.Cleanup(dt.Close)
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
	require.Len(t, dt.regionHits, 1)
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
		dt.regionHits, *advised = nil, nil
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
