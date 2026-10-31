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
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/node/ethconfig"
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
	t.Run("missing chain id", func(t *testing.T) {
		_, err := ReadGenesisFile(writeGenesis(t, `{"config":{},"gasLimit":"0x1c9c380","difficulty":"0x0","alloc":{}}`))
		require.ErrorContains(t, err, "missing 'config.chainId'")
		_, err = ReadGenesisFile(writeGenesis(t, `{"config":{"chainId":null},"gasLimit":"0x1c9c380","difficulty":"0x0","alloc":{}}`))
		require.ErrorContains(t, err, "missing 'config.chainId'")
	})
	t.Run("bor config", func(t *testing.T) {
		_, err := ReadGenesisFile(writeGenesis(t, `{"config":{"chainId":137,"bor":{"period":{"0":2}}},"gasLimit":"0x1c9c380","difficulty":"0x0","alloc":{}}`))
		require.ErrorContains(t, err, "Polygon is not supported")
	})
}

func TestSetOverrideGenesis(t *testing.T) {
	path := writeGenesis(t, `{"config":{"chainId":1337},"gasLimit":"0x1c9c380","difficulty":"0x0","alloc":{}}`)
	mainnetDNS := []string{"enrtree://mainnet"}
	tests := []struct {
		name          string
		args          []string
		networkID     uint64 // as set by SetEthConfig before setOverrideGenesis
		wantNetworkID uint64
		wantDNS       []string
	}{
		{"no --networkid → genesis chain id", nil, 1, 1337, nil},
		{"explicit --networkid wins", []string{"--networkid=7"}, 7, 7, nil},
		{"--nodiscover keeps discovery config", []string{"--nodiscover"}, 1, 1337, mainnetDNS},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// urfave/cli v3 flags carry parse state (hasBeenSet), so use fresh
			// instances per run instead of the shared package-global flags.
			overrideGenesisFlag := OverrideGenesisFlag
			chainFlag := ChainFlag
			networkIdFlag := NetworkIdFlag
			dnsDiscoveryFlag := DNSDiscoveryFlag
			noDiscoverFlag := NoDiscoverFlag
			app := &cli.Command{}
			app.Flags = []cli.Flag{&overrideGenesisFlag, &chainFlag, &networkIdFlag, &dnsDiscoveryFlag, &noDiscoverFlag}
			app.Action = func(_ context.Context, ctx *cli.Command) error {
				cfg := &ethconfig.Config{NetworkID: tt.networkID, EthDiscoveryURLs: mainnetDNS}
				setOverrideGenesis(ctx, cfg)
				require.Equal(t, uint64(1337), cfg.Genesis.Config.ChainID.Uint64())
				require.Equal(t, tt.wantNetworkID, cfg.NetworkID)
				require.Equal(t, tt.wantDNS, cfg.EthDiscoveryURLs)
				return nil
			}
			args := append([]string{"test", "--override.genesis=" + path}, tt.args...)
			require.NoError(t, app.Run(context.Background(), args))
		})
	}
}
