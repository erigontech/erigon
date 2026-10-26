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
package eez

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/eez/networks"
	"github.com/erigontech/erigon/eez/protocol"
	eezrules "github.com/erigontech/erigon/eez/rules"
	"github.com/erigontech/erigon/execution/chain"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/rulesconfig"
)

func TestRegister(t *testing.T) {
	_, err := chainspec.ChainSpecByName(networks.Devnet.Name)
	require.ErrorIs(t, err, chainspec.ErrChainSpecUnknown)
	require.False(t, types.IsRegisteredTxType(protocol.SystemTxType))

	Register()
	Register()

	spec, err := chainspec.ChainSpecByName(networks.Devnet.Name)
	require.NoError(t, err)
	require.True(t, types.IsRegisteredTxType(protocol.SystemTxType))
	require.IsType(t, &eezrules.Engine{}, rulesconfig.CreateRulesEngineBareBones(t.Context(), spec.Config, log.New()))

	l2, err := json.Marshal(spec.Config.L2)
	require.NoError(t, err)
	stored := chain.Config{ChainID: spec.Config.ChainID, L2JSON: l2}
	require.NoError(t, stored.ResolveL2())
	require.Equal(t, spec.Config.L2, stored.L2)
}

func TestDirs(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	got := Dirs(dirs)
	require.Equal(t, filepath.Join(dirs.DataDir, "eez", "chaindata"), got.Chaindata)
	require.Equal(t, filepath.Join(dirs.DataDir, "snapshots", "eez"), got.Snap)
	require.Equal(t, filepath.Join(got.Snap, "domain"), got.SnapDomain)
	require.Equal(t, dirs.Tmp, got.Tmp)
	require.Equal(t, dirs.Log, got.Log)
	require.DirExists(t, got.Chaindata)
	require.DirExists(t, got.SnapAccessors)
}
