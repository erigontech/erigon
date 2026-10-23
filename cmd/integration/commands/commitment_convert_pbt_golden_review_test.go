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

package commands

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
)

func TestPBTGoldenLargeStorageIsVisibleToConverter(t *testing.T) {
	configurePBTGoldenSuite(t, commitment.PBinHashBlake3, true)
	source, _ := newPBTConversionSource(t)
	addPBTGoldenStorageBucket(t, source)
	settings, err := dbstate.ReadErigonDBSettings(source.Dirs)
	require.NoError(t, err)
	agg, err := dbstate.NewPBTStateAggregator(source.Dirs, settings, log.New()).Open(t.Context())
	require.NoError(t, err)
	defer agg.Close()
	require.NoError(t, agg.OpenFolder(nil))
	at := agg.BeginFilesRo()
	defer at.Close()
	storageLeaves := 0
	require.NoError(t, dbstate.ForEachPBinLeaf(at, nil, true, func(leaf dbstate.PBinLeaf) error {
		if leaf.Key[0] == eip8297.StorageZone {
			storageLeaves++
		}
		return nil
	}))
	require.Equal(t, 256, storageLeaves, "large-storage golden corpus: converter must see the added nonzero slots in files")
}

func TestPBTGoldenSharedCodeCoversDifferentStampRanges(t *testing.T) {
	source := newPBTGoldenAcceptanceSource(t, commitment.PBinHashBlake3)
	settings, err := dbstate.ReadErigonDBSettings(source.Dirs)
	require.NoError(t, err)
	agg, err := dbstate.NewPBTStateAggregator(source.Dirs, settings, log.New()).Open(t.Context())
	require.NoError(t, err)
	defer agg.Close()
	require.NoError(t, agg.OpenFolder(nil))
	at := agg.BeginFilesRo()
	defer at.Close()
	iter, err := at.DbgDomain(kv.CodeDomain).DebugRangeLatestFromFiles(nil, nil, kv.Unlim)
	require.NoError(t, err)
	defer iter.Close()
	stampsByCode := make(map[string][]uint64)
	for iter.HasNext() {
		_, code, nextErr := iter.Next()
		require.NoError(t, nextErr)
		if len(code) != 0 {
			stampsByCode[string(code)] = append(stampsByCode[string(code)], iter.Stamp())
		}
	}
	var duplicateStamps []uint64
	maxRanges := 0
	for _, stamps := range stampsByCode {
		if len(stamps) < 2 {
			continue
		}
		duplicateStamps = append(duplicateStamps, stamps...)
		ranges := make(map[uint64]bool)
		for _, stamp := range stamps {
			ranges[stamp/settings.StepSize] = true
		}
		maxRanges = max(maxRanges, len(ranges))
	}
	require.Greater(t, maxRanges, 1, "shared-code golden corpus must exercise max-stamp dedup across ranges; duplicate stamps=%v", duplicateStamps)
}
