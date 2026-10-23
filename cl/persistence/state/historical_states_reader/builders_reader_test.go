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

package historical_states_reader

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/db/kv"
)

const readerBuilderLimit = 1024

func TestReadBuildersSSZDumpChangesAndAppends(t *testing.T) {
	dumpSlot := uint64(clparams.SlotsPerDump)
	dump := readerBuilders(10, 20)
	afterChange := readerBuilders(11, 20)
	afterAppend := readerBuilders(11, 20, 30)
	getter := mockGetValFn(map[string]map[string][]byte{
		kv.BuildersDump: {
			string(base_encoding.Encode64ToBytes4(dumpSlot)): compressSSZ(t, dump),
		},
		kv.Builders: {
			string(base_encoding.Encode64ToBytes4(dumpSlot + 1)): encodeBuildersRow(t, dump, afterChange),
			string(base_encoding.Encode64ToBytes4(dumpSlot + 2)): encodeBuildersRow(t, afterChange, afterAppend),
		},
	})

	got := readerBuilders()
	require.NoError(t, ReadBuildersSSZ(getter, dumpSlot+2, 0, readerBuilderLimit, got))
	require.Equal(t, []uint64{11, 20, 30}, readerBuilderBalances(got))
}

func TestReadBuildersSSZDistinguishesHolesFromMissedSlots(t *testing.T) {
	dumpSlot := uint64(clparams.SlotsPerDump)
	dump := readerBuilders(10, 20)
	changed := readerBuilders(11, 20)
	appended := readerBuilders(11, 20, 30)
	rowsAfterDump := map[uint64][]byte{
		dumpSlot + 1: encodeBuildersRow(t, dump, changed),
		dumpSlot + 3: encodeBuildersRow(t, changed, appended),
	}
	tests := []struct {
		name     string
		forkSlot uint64
		target   uint64
		withDump bool
		rows     map[uint64][]byte
		slotData []uint64
		hole     uint64
		want     []uint64
	}{
		{name: "processed block hole", forkSlot: dumpSlot, target: dumpSlot, slotData: []uint64{dumpSlot}, hole: dumpSlot},
		{name: "missed slot", forkSlot: dumpSlot, target: dumpSlot, want: []uint64{}},
		{name: "pre-fork processed slot", forkSlot: dumpSlot + 1, target: dumpSlot, slotData: []uint64{dumpSlot}, want: []uint64{}},
		{
			name: "processed block hole after dump", target: dumpSlot + 3, withDump: true, rows: rowsAfterDump,
			slotData: []uint64{dumpSlot + 1, dumpSlot + 2, dumpSlot + 3}, hole: dumpSlot + 2,
		},
		{
			name: "missed slot after dump", target: dumpSlot + 3, withDump: true, rows: rowsAfterDump,
			slotData: []uint64{dumpSlot + 1, dumpSlot + 3}, want: []uint64{11, 20, 30},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data := map[string]map[string][]byte{kv.SlotData: {}, kv.Builders: {}}
			if test.withDump {
				data[kv.BuildersDump] = map[string][]byte{string(base_encoding.Encode64ToBytes4(dumpSlot)): compressSSZ(t, dump)}
			}
			for slot, row := range test.rows {
				data[kv.Builders][string(base_encoding.Encode64ToBytes4(slot))] = row
			}
			for _, slot := range test.slotData {
				data[kv.SlotData][string(base_encoding.Encode64ToBytes4(slot))] = []byte{1}
			}
			got := readerBuilders()
			err := ReadBuildersSSZ(mockGetValFn(data), test.target, test.forkSlot, readerBuilderLimit, got)
			if test.hole != 0 {
				require.ErrorIs(t, err, state_accessors.ErrHistoryRebuildRequired)
				require.ErrorContains(t, err, kv.Builders)
				require.ErrorContains(t, err, fmt.Sprintf("slot %d", test.hole))
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, readerBuilderBalances(got))
		})
	}
}

func TestReadBuildersSSZChecksTheRowAtTheDumpSlot(t *testing.T) {
	dumpSlot := uint64(2 * clparams.SlotsPerDump)
	dump := readerBuilders(10, 20)
	legacyWriter := base_encoding.NewSSZQueueEncoder[*cltypes.Builder](func(a, b *cltypes.Builder) bool { return *a == *b })
	var legacy bytes.Buffer
	require.NoError(t, legacyWriter.WriteDiff(&legacy, dump))
	tests := []struct {
		name     string
		forkSlot uint64
		row      []byte
		missed   bool
		rebuild  bool
	}{
		{name: "legacy row after the fork", forkSlot: clparams.SlotsPerDump, row: legacy.Bytes(), rebuild: true},
		{name: "legacy row at the fork slot", forkSlot: dumpSlot, row: legacy.Bytes(), rebuild: true},
		{name: "current row is not applied twice", forkSlot: clparams.SlotsPerDump, row: encodeBuildersRow(t, readerBuilders(10), dump)},
		{name: "processed block without a row", forkSlot: clparams.SlotsPerDump, rebuild: true},
		{name: "missed block", forkSlot: clparams.SlotsPerDump, missed: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			slotData := map[string][]byte{}
			if !test.missed {
				slotData[string(base_encoding.Encode64ToBytes4(dumpSlot))] = []byte{1}
			}
			getter := mockGetValFn(map[string]map[string][]byte{
				kv.BuildersDump: {string(base_encoding.Encode64ToBytes4(dumpSlot)): compressSSZ(t, dump)},
				kv.Builders:     {string(base_encoding.Encode64ToBytes4(dumpSlot)): test.row},
				kv.SlotData:     slotData,
			})
			got := readerBuilders()
			err := ReadBuildersSSZ(getter, dumpSlot, test.forkSlot, readerBuilderLimit, got)
			if test.rebuild {
				require.ErrorIs(t, err, state_accessors.ErrHistoryRebuildRequired)
				require.ErrorContains(t, err, fmt.Sprintf("slot %d", dumpSlot))
				return
			}
			require.NoError(t, err)
			require.Equal(t, []uint64{10, 20}, readerBuilderBalances(got))
		})
	}
}

func TestReadBuildersSSZRejectsLegacyRowWithContext(t *testing.T) {
	slot := uint64(clparams.SlotsPerDump)
	legacyWriter := base_encoding.NewSSZQueueEncoder[*cltypes.Builder](func(a, b *cltypes.Builder) bool { return *a == *b })
	var legacy bytes.Buffer
	require.NoError(t, legacyWriter.WriteDiff(&legacy, readerBuilders(1)))
	getter := mockGetValFn(map[string]map[string][]byte{
		kv.Builders: {string(base_encoding.Encode64ToBytes4(slot)): legacy.Bytes()},
	})

	err := ReadBuildersSSZ(getter, slot, slot, readerBuilderLimit, readerBuilders())
	require.ErrorIs(t, err, state_accessors.ErrHistoryRebuildRequired)
	require.ErrorContains(t, err, kv.Builders)
	require.ErrorContains(t, err, fmt.Sprint(slot))
}

func readerBuilders(values ...uint64) *solid.ListSSZ[*cltypes.Builder] {
	builders := solid.NewStaticListSSZ[*cltypes.Builder](readerBuilderLimit, new(cltypes.Builder).EncodingSizeSSZ())
	for _, value := range values {
		builders.Append(&cltypes.Builder{Balance: value})
	}
	return builders
}

func encodeBuildersRow(t *testing.T, pre, post *solid.ListSSZ[*cltypes.Builder]) []byte {
	t.Helper()
	writer := new(state_accessors.BuildersDiffWriter)
	writer.Initialize(pre)
	var row bytes.Buffer
	require.NoError(t, writer.WriteDiff(&row, post))
	return bytes.Clone(row.Bytes())
}

func readerBuilderBalances(builders *solid.ListSSZ[*cltypes.Builder]) []uint64 {
	balances := make([]uint64, builders.Len())
	for i := range balances {
		balances[i] = builders.Get(i).Balance
	}
	return balances
}
