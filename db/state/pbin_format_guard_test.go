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
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
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
			require.ErrorContains(t, err, "rebuild the bin commitment domain")
		})
	}

	t.Run("legacy record format", func(t *testing.T) {
		temporalDB, agg := testDbAndAggregatorv3(t, 1)
		db := temporalDB
		state := []byte{commitment.PBinStateMarker, 0x10, 0, 0}
		putPBinOpenState(t, temporalDB, pbinOpenStateEnvelope(state))
		err := agg.OpenFolder(db)
		require.ErrorContains(t, err, "OpenFolder")
		require.ErrorContains(t, err, "rebuild the bin commitment domain")
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
			require.ErrorContains(t, err, "rebuild the bin commitment domain")
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
	fixture := newPBinOutputFixture(t, true, false)
	fixture.output.Close()
	entries, err := os.ReadDir(filepath.Dir(fixture.outputPath))
	require.NoError(t, err)
	for _, entry := range entries {
		if strings.Contains(entry.Name(), "-commitment.") && !strings.HasSuffix(entry.Name(), ".kv") {
			require.NoError(t, dir.RemoveFile(filepath.Join(filepath.Dir(fixture.outputPath), entry.Name())))
		}
	}

	settings, err := state.ReadErigonDBSettings(fixture.output.Dirs())
	require.NoError(t, err)
	prepared := state.NewTest(fixture.output.Dirs()).
		StepSize(fixture.output.StepSize()).
		WithErigonDBSettings(settings).
		Logger(log.New()).
		MustOpen(t.Context())
	t.Cleanup(prepared.Close)
	require.NoError(t, prepared.OpenFolder(fixture.db))
	require.NoError(t, prepared.BuildMissedAccessors(t.Context(), fixture.db, 2))
	prepared.Close()

	variant, hash := state.TrieVariantBin, commitment.PBinHashBlake3
	settings.TrieVariant = &variant
	settings.TrieHash = &hash
	require.NoError(t, state.WriteErigonDBSettings(fixture.output.Dirs(), settings))

	output := state.NewTest(fixture.output.Dirs()).
		StepSize(fixture.output.StepSize()).
		WithErigonDBSettings(settings).
		Logger(log.New()).
		MustOpen(t.Context())
	t.Cleanup(output.Close)
	err = output.OpenFolder(nil)
	require.ErrorContains(t, err, "OpenFolder")
	require.ErrorContains(t, err, "rebuild the bin commitment domain")
}

func TestOpenFolderRejectsLegacyPBinStateInDualDomain(t *testing.T) {
	oldBin := statecfg.ExperimentalBinCommitment
	oldHexBin := statecfg.ExperimentalHexBinCommitment
	oldSchema := statecfg.Schema
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = oldBin
		statecfg.ExperimentalHexBinCommitment = oldHexBin
		statecfg.Schema = oldSchema
	})
	statecfg.ExperimentalBinCommitment = true
	statecfg.ExperimentalHexBinCommitment = true
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentDomain)
	statecfg.EnableCommitmentV3Records(&statecfg.Schema.CommitmentBinDomain)

	db, agg := testDbAndAggregatorv3(t, 1)
	putPBinOpenStateForDomain(t, db, kv.CommitmentBinDomain, []byte{commitment.PBinStateMarker, 0x10, 0, 0})
	err := agg.OpenFolder(db)
	require.ErrorContains(t, err, "OpenFolder")
	require.ErrorContains(t, err, "rebuild the bin commitment domain")
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
