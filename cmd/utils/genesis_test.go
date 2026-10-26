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

package utils

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func writeGenesis(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "genesis.json")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func TestReadGenesisFile(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		g, err := ReadGenesisFile(writeGenesis(t, `{"config":{"chainId":1337},"gasLimit":"0x1c9c380","difficulty":"0x0","alloc":{}}`))
		require.NoError(t, err)
		require.Equal(t, uint64(1337), g.Config.ChainID.Uint64())
		require.Equal(t, uint64(30_000_000), g.GasLimit)
	})
	t.Run("missing file", func(t *testing.T) {
		_, err := ReadGenesisFile(filepath.Join(t.TempDir(), "missing.json"))
		require.ErrorContains(t, err, "failed to read genesis file")
	})
	t.Run("invalid json", func(t *testing.T) {
		_, err := ReadGenesisFile(writeGenesis(t, `{`))
		require.ErrorContains(t, err, "invalid genesis file")
	})
	t.Run("missing config", func(t *testing.T) {
		_, err := ReadGenesisFile(writeGenesis(t, `{"gasLimit":"0x1c9c380","difficulty":"0x0","alloc":{}}`))
		require.ErrorContains(t, err, "missing 'config'")
	})
	t.Run("bor config", func(t *testing.T) {
		_, err := ReadGenesisFile(writeGenesis(t, `{"config":{"chainId":137,"bor":{"period":{"0":2}}},"gasLimit":"0x1c9c380","difficulty":"0x0","alloc":{}}`))
		require.ErrorContains(t, err, "Polygon is not supported")
	})
}
