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

func runOverrideGenesis(args []string) (*ethconfig.Config, error) {
	// CLI flags retain parse state, so each run needs fresh instances.
	overrideGenesisFlag, chainFlag, networkIDFlag := OverrideGenesisFlag, ChainFlag, NetworkIdFlag
	dnsDiscoveryFlag, noDiscoverFlag := DNSDiscoveryFlag, NoDiscoverFlag
	cfg := &ethconfig.Config{EthDiscoveryURLs: []string{"enrtree://mainnet"}}
	app := &cli.Command{
		Flags: []cli.Flag{&overrideGenesisFlag, &chainFlag, &networkIDFlag, &dnsDiscoveryFlag, &noDiscoverFlag},
		Action: func(_ context.Context, ctx *cli.Command) error {
			cfg.NetworkID = ctx.Uint64(NetworkIdFlag.Name)
			return setOverrideGenesis(ctx, cfg)
		},
	}
	err := app.Run(context.Background(), append([]string{"test"}, args...))
	return cfg, err
}

func TestSetOverrideGenesis(t *testing.T) {
	path := writeGenesis(t, `{"config":{"chainId":1337},"gasLimit":"0x1c9c380","difficulty":"0x0","alloc":{}}`)
	mainnetDNS := []string{"enrtree://mainnet"}
	tests := []struct {
		name          string
		args          []string
		wantNetworkID uint64
		wantDNS       []string
	}{
		{
			name:          "no --networkid → genesis chain id",
			wantNetworkID: 1337,
		},
		{
			name:          "explicit --networkid wins",
			args:          []string{"--networkid=7"},
			wantNetworkID: 7,
		},
		{
			name:          "--nodiscover keeps discovery config",
			args:          []string{"--nodiscover"},
			wantNetworkID: 1337,
			wantDNS:       mainnetDNS,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := append([]string{"--override.genesis=" + path}, tt.args...)
			cfg, err := runOverrideGenesis(args)
			require.NoError(t, err)
			require.True(t, cfg.GenesisFromFile)
			require.Equal(t, uint64(1337), cfg.Genesis.Config.ChainID.Uint64())
			require.Equal(t, tt.wantNetworkID, cfg.NetworkID)
			require.Equal(t, tt.wantDNS, cfg.EthDiscoveryURLs)
		})
	}
}

func TestSetOverrideGenesisErrors(t *testing.T) {
	path := writeGenesis(t, `{"config":{"chainId":1337},"gasLimit":"0x1c9c380","difficulty":"0x0","alloc":{}}`)
	zeroChainID := writeGenesis(t, `{"config":{"chainId":0},"gasLimit":"0x1c9c380","difficulty":"0x0","alloc":{}}`)
	largeChainID := writeGenesis(t, `{"config":{"chainId":18446744073709551616},"gasLimit":"0x1c9c380","difficulty":"0x0","alloc":{}}`)
	tests := []struct {
		name          string
		args          []string
		wantErr       string
		wantNetworkID uint64
	}{
		{
			name:    "empty path",
			args:    []string{"--override.genesis="},
			wantErr: "failed to read genesis file",
		},
		{
			name:    "with --chain",
			args:    []string{"--override.genesis=" + path, "--chain=sepolia"},
			wantErr: "mutually exclusive",
		},
		{
			name:    "chain id 0",
			args:    []string{"--override.genesis=" + zeroChainID},
			wantErr: "must be set to a nonzero network id",
		},
		{
			name:          "chain id 0 with --networkid",
			args:          []string{"--override.genesis=" + zeroChainID, "--networkid=7"},
			wantNetworkID: 7,
		},
		{
			name:    "chain id 2^64",
			args:    []string{"--override.genesis=" + largeChainID},
			wantErr: "must be set to a nonzero network id",
		},
		{
			name:          "chain id 2^64 with --networkid",
			args:          []string{"--override.genesis=" + largeChainID, "--networkid=7"},
			wantNetworkID: 7,
		},
		{
			name:          "flag not given",
			wantNetworkID: ethconfig.Defaults.NetworkID,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := runOverrideGenesis(tt.args)
			if tt.wantErr == "" {
				require.NoError(t, err)
				require.Equal(t, tt.wantNetworkID, cfg.NetworkID)
			} else {
				require.ErrorContains(t, err, tt.wantErr)
				require.Nil(t, cfg.Genesis)
			}
		})
	}
}
