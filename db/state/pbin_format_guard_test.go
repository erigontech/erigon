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

package state_test

import (
	"encoding/binary"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

func TestOpenFolderRejectsLegacyPBinStateFormats(t *testing.T) {
	oldBin := statecfg.ExperimentalBinCommitment
	oldSchema := statecfg.Schema
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = oldBin
		statecfg.Schema = oldSchema
	})
	statecfg.ExperimentalBinCommitment = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)

	for format := byte(0); format <= 7; format++ {
		t.Run("legacy flags "+strconv.Itoa(int(format)), func(t *testing.T) {
			temporalDB, agg := testDbAndAggregatorv3(t, 1)
			db := temporalDB
			putPBinOpenState(t, temporalDB, []byte{commitment.PBinStateMarker, format, 0, 0})
			err := agg.OpenFolder(db)
			require.ErrorContains(t, err, "OpenFolder")
			require.ErrorContains(t, err, "commitment convert-pbt")
		})
	}

	t.Run("legacy record format", func(t *testing.T) {
		temporalDB, agg := testDbAndAggregatorv3(t, 1)
		db := temporalDB
		state := []byte{commitment.PBinStateMarker, 0x10, 0, 0}
		putPBinOpenState(t, temporalDB, pbinOpenStateEnvelope(state))
		err := agg.OpenFolder(db)
		require.ErrorContains(t, err, "OpenFolder")
		require.ErrorContains(t, err, "commitment convert-pbt")
	})
}

func TestOpenFolderAcceptsCurrentPBinState(t *testing.T) {
	oldBin := statecfg.ExperimentalBinCommitment
	oldSchema := statecfg.Schema
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = oldBin
		statecfg.Schema = oldSchema
	})
	statecfg.ExperimentalBinCommitment = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)

	temporalDB, agg := testDbAndAggregatorv3(t, 1)
	db := temporalDB
	putPBinOpenState(t, temporalDB, []byte{commitment.PBinStateMarker, commitment.PBinRowStateFormat, 0, 0, 0})
	require.NoError(t, agg.OpenFolder(db))
	require.NoError(t, agg.OpenFolder(db))
}

func TestOpenFolderRejectsTruncatedCurrentPBinState(t *testing.T) {
	oldBin := statecfg.ExperimentalBinCommitment
	oldSchema := statecfg.Schema
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = oldBin
		statecfg.Schema = oldSchema
	})
	statecfg.ExperimentalBinCommitment = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)

	for length := 2; length < 5; length++ {
		t.Run(strconv.Itoa(length), func(t *testing.T) {
			temporalDB, agg := testDbAndAggregatorv3(t, 1)
			putPBinOpenState(t, temporalDB, []byte{commitment.PBinStateMarker, commitment.PBinRowStateFormat, 0, 0, 0}[:length])
			err := agg.OpenFolder(temporalDB)
			require.ErrorContains(t, err, "OpenFolder")
			require.ErrorContains(t, err, "commitment convert-pbt")
		})
	}
}

func TestOpenFolderAcceptsFreshPBinDatadir(t *testing.T) {
	oldBin := statecfg.ExperimentalBinCommitment
	oldSchema := statecfg.Schema
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = oldBin
		statecfg.Schema = oldSchema
	})
	statecfg.ExperimentalBinCommitment = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)

	temporalDB, agg := testDbAndAggregatorv3(t, 1)
	require.NoError(t, agg.OpenFolder(temporalDB))
}

func TestOpenFolderRejectsLegacyPBinStateInFiles(t *testing.T) {
	oldBin := statecfg.ExperimentalBinCommitment
	oldSchema := statecfg.Schema
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = oldBin
		statecfg.Schema = oldSchema
	})
	statecfg.ExperimentalBinCommitment = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)

	dirs := datadir.New(t.TempDir())
	variant, hash := state.TrieVariantBin, commitment.PBinHashBlake3
	settings := &state.ErigonDBSettings{StepSize: 1, StepsInFrozenFile: 1, TrieVariant: &variant, TrieHash: &hash}
	initialSettings := &state.ErigonDBSettings{StepSize: 1, StepsInFrozenFile: 1}
	require.NoError(t, state.WriteErigonDBSettings(dirs, initialSettings))
	path := filepath.Join(dirs.SnapDomain, "v1.0-commitment.0-1.kv")
	compressor, err := seg.NewCompressor(t.Context(), "legacy-pbin-state", path, dirs.Tmp, seg.DefaultCfg, log.LvlDebug, log.New())
	require.NoError(t, err)
	require.NoError(t, compressor.AddWord(commitmentdb.KeyCommitmentState))
	require.NoError(t, compressor.AddWord(pbinOpenStateEnvelope([]byte{commitment.PBinStateMarker, 0x10, 0, 0})))
	require.NoError(t, compressor.Compress())
	compressor.Close()

	output := state.NewTest(dirs).StepSize(1).WithErigonDBSettings(initialSettings).Logger(log.New()).MustOpen(t.Context())
	t.Cleanup(output.Close)
	require.NoError(t, output.OpenFolder(nil))
	require.NoError(t, output.BuildMissedAccessors(t.Context(), nil, 2))
	output.Close()
	require.NoError(t, state.WriteErigonDBSettings(dirs, settings))
	output = state.NewTest(dirs).StepSize(1).WithErigonDBSettings(settings).Logger(log.New()).MustOpen(t.Context())
	err = output.OpenFolder(nil)
	require.ErrorContains(t, err, "OpenFolder")
	require.ErrorContains(t, err, "commitment convert-pbt")
}

func TestOpenFolderRejectsLegacyPBinStateInDualDomain(t *testing.T) {
	oldBin := statecfg.ExperimentalBinCommitment
	oldHexBin := statecfg.ExperimentalHexBinCommitment
	oldV3 := statecfg.ExperimentalCommitmentV3
	oldSchema := statecfg.Schema
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = oldBin
		statecfg.ExperimentalHexBinCommitment = oldHexBin
		statecfg.ExperimentalCommitmentV3 = oldV3
		statecfg.Schema = oldSchema
	})
	statecfg.ExperimentalBinCommitment = true
	statecfg.ExperimentalHexBinCommitment = true
	statecfg.ExperimentalCommitmentV3 = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentBinDomain)

	db, agg := testDbAndAggregatorv3(t, 1)
	putPBinOpenStateForDomain(t, db, kv.CommitmentBinDomain, []byte{commitment.PBinStateMarker, 0x10, 0, 0})
	err := agg.OpenFolder(db)
	require.ErrorContains(t, err, "OpenFolder")
	require.ErrorContains(t, err, "commitment convert-pbt")
}

func putPBinOpenState(t *testing.T, db kv.TemporalRwDB, value []byte) {
	putPBinOpenStateForDomain(t, db, kv.CommitmentDomain, value)
}

func putPBinOpenStateForDomain(t *testing.T, db kv.TemporalRwDB, domain kv.Domain, value []byte) {
	t.Helper()
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	sd, err := execctx.NewSharedDomains(t.Context(), tx, log.New(), execctx.WithoutCommitmentSeek())
	require.NoError(t, err)
	require.NoError(t, sd.DomainPut(domain, tx, commitmentdb.KeyCommitmentState, value, 1, nil))
	require.NoError(t, sd.Flush(t.Context(), tx))
	require.NoError(t, tx.Commit())
	sd.Close()
}

func pbinOpenStateEnvelope(state []byte) []byte {
	value := make([]byte, 18+len(state))
	binary.BigEndian.PutUint16(value[16:18], uint16(len(state)))
	copy(value[18:], state)
	return value
}
