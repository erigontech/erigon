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
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/rawdb"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/stretchr/testify/require"
)

func TestPBTR16LegacyAttachMarkerCanResume(t *testing.T) {
	selectPBTCommandSuite(t)
	source, _ := newPBTConversionSource(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, published, true, "", log.New()))
	setExecutionProgress(t, source.Chaindata, 1)
	hooks := pbtAttachHooks{genesis: pbtAttachNoGenesis, step: func(step string) error {
		if step == "marker" {
			return errors.New("interrupted attach")
		}
		return nil
	}}
	require.ErrorContains(t, attachPBTWithHooks(t.Context(), source.DataDir, published, "", log.New(), hooks), "interrupted attach")
	marker, err := dbstate.ReadPBTAttachMarker(source.Dirs)
	require.NoError(t, err)
	require.NotNil(t, marker)
	marker.NodeHexRoot, marker.HeaderRoot = "", ""
	require.NoError(t, dbstate.WritePBTAttachMarker(source.Dirs, marker))
	err = attachPBTWithHooks(t.Context(), source.DataDir, published, "", log.New(), pbtAttachHooks{genesis: pbtAttachNoGenesis})
	require.NoError(t, err, "same-manifest retry with a legacy marker must finish while the original node state is intact")
}

func TestPBTR16ResumeRejectsOutputMDBXRows(t *testing.T) {
	source, output := pbtR15ResumeFixture(t, 1, false, false)
	dirs := datadir.Open(output)
	db := mdbx.New(dbcfg.ChainDB, log.New()).Path(dirs.Chaindata).MustOpen()
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		value := binary.BigEndian.AppendUint64(nil, ^uint64(0))
		return tx.Put(kv.TblCommitmentBinVals, []byte("stray"), value)
	}))
	db.Close()
	before := snapshotTreeDigest(t, output)
	err := convertPBTWithOptions(t.Context(), source.Tester.Dirs.DataDir, output, true, "", log.New(), pbtConvertHooks{resume: true, workers: 1})
	require.ErrorContains(t, err, "MDBX rows", "resume must refuse output commitment MDBX rows before changing files; got %v", err)
	after := snapshotTreeDigest(t, output)
	require.Equal(t, before, after, "MDBX-base refusal must leave output file list and bytes unchanged")
}

func TestPBTR16EmptyOutputRefusalPreservesDirectory(t *testing.T) {
	selectPBTCommandSuite(t)
	source, _ := newPBTConversionSource(t)
	output := filepath.Join(t.TempDir(), "output")
	require.NoError(t, os.MkdirAll(output, 0o755))
	err := convertPBTWithOptions(t.Context(), source.DataDir, output, true, "", log.New(), pbtConvertHooks{resume: true, workers: 1})
	require.ErrorContains(t, err, "empty resume output requires a source binary commitment domain")
	_, err = os.Stat(output)
	require.NoError(t, err, "resume refusal must preserve an existing empty output directory")
}

func TestPBTR16BytecodeRewriteRefusalPreservesOutput(t *testing.T) {
	source, output := pbtR15ResumeFixture(t, 1, false, false)
	db := dbCfg(dbcfg.ChainDB, source.Tester.Dirs.Chaindata).MustOpen()
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		hash, err := rawdb.ReadCanonicalHash(tx, 0)
		if err != nil {
			return err
		}
		config, err := rawdb.ReadChainConfig(tx, hash)
		if err != nil {
			return err
		}
		config.ChainName = "gnosis"
		return rawdb.WriteChainConfig(tx, hash, config)
	}))
	db.Close()
	before := domainFileNames(t, datadir.Open(output).SnapDomain)
	err := convertPBTWithOptions(t.Context(), source.Tester.Dirs.DataDir, output, true, "", log.New(), pbtConvertHooks{resume: true, workers: 1})
	require.ErrorContains(t, err, "bytecode rewrite")
	after := domainFileNames(t, datadir.Open(output).SnapDomain)
	require.Equal(t, before, after, "bytecode-rewrite refusal changed the output domain file list: before=%d after=%d", len(before), len(after))
}
