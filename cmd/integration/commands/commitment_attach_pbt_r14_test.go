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
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/state"
)

func TestAttachPBTR14ResumeAfterCommitmentRemoval(t *testing.T) {
	selectPBTCommandSuite(t)
	source, _ := newPBTConversionSource(t)
	published := filepath.Join(t.TempDir(), "published")
	require.NoError(t, convertPBT(t.Context(), source.DataDir, published, true, "", log.New()))
	setExecutionProgress(t, source.Chaindata, 1)
	db := dbCfg(dbcfg.ChainDB, source.Chaindata).MustOpen()
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error { return tx.ClearTable(kv.TblCommitmentVals) }))
	db.Close()
	hooks := pbtAttachHooks{genesis: pbtAttachNoGenesis, step: func(step string) error {
		if step == "marker" {
			return errors.New("interrupted attach")
		}
		return nil
	}}
	require.ErrorContains(t, attachPBTWithHooks(t.Context(), source.DataDir, published, "", log.New(), hooks), "interrupted attach")
	marker, err := state.ReadPBTAttachMarker(source.Dirs)
	require.NoError(t, err)
	require.NotNil(t, marker)
	digest, err := state.PBTManifestDigest(filepath.Join(datadir.Open(published).Snap, "pbt-manifest.toml"))
	require.NoError(t, err)
	require.Equal(t, digest, marker.ManifestBlake3)
	require.NoError(t, removePBTFiles(source.Dirs, pbtAttachAdoptsFile))
	err = attachPBTWithHooks(t.Context(), source.DataDir, published, "", log.New(), pbtAttachHooks{genesis: pbtAttachNoGenesis})
	require.NoError(t, err, "same-manifest resume after interrupted file removal must finish: got %v", err)
}
