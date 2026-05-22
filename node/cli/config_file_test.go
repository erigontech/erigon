// Copyright 2024 The Erigon Authors
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

package cli

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
)

func TestSetFlagsFromConfigFile_StaticPeers(t *testing.T) {
	staticPeersFlag := cli.StringFlag{
		Name:  "staticpeers",
		Usage: "Comma separated enode URLs to connect to",
		Value: "",
	}
	sentinelStaticPeersFlag := cli.StringSliceFlag{
		Name:  "sentinel.staticpeers",
		Usage: "connect to comma-separated Consensus static peers",
	}

	tests := []struct {
		name                    string
		toml                    string
		wantStaticPeers         string
		wantSentinelStaticPeers []string
	}{
		{
			name:            "EL staticpeers as TOML array",
			toml:            "staticpeers = [\"enode://abc@1.2.3.4:30303\", \"enode://def@5.6.7.8:30303\"]\n",
			wantStaticPeers: "enode://abc@1.2.3.4:30303,enode://def@5.6.7.8:30303",
		},
		{
			name:            "EL staticpeers as TOML string",
			toml:            "staticpeers = \"enode://abc@1.2.3.4:30303,enode://def@5.6.7.8:30303\"\n",
			wantStaticPeers: "enode://abc@1.2.3.4:30303,enode://def@5.6.7.8:30303",
		},
		{
			name:                    "CL sentinel.staticpeers as TOML dotted key",
			toml:                    "sentinel.staticpeers = [\"enode://abc@1.2.3.4:9000\", \"enode://def@5.6.7.8:9000\"]\n",
			wantSentinelStaticPeers: []string{"enode://abc@1.2.3.4:9000", "enode://def@5.6.7.8:9000"},
		},
		{
			name:                    "CL sentinel.staticpeers as TOML nested table",
			toml:                    "[sentinel]\nstaticpeers = [\"enode://abc@1.2.3.4:9000\", \"enode://def@5.6.7.8:9000\"]\n",
			wantSentinelStaticPeers: []string{"enode://abc@1.2.3.4:9000", "enode://def@5.6.7.8:9000"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			cfgFile := filepath.Join(tmpDir, "erigon.toml")
			require.NoError(t, os.WriteFile(cfgFile, []byte(tt.toml), 0o600))

			app := cli.NewApp()
			app.Flags = []cli.Flag{&staticPeersFlag, &sentinelStaticPeersFlag}
			app.Action = func(ctx *cli.Context) error {
				err := SetFlagsFromConfigFile(ctx, cfgFile)
				require.NoError(t, err)

				if tt.wantStaticPeers != "" {
					require.True(t, ctx.IsSet("staticpeers"), "ctx.IsSet(staticpeers) should be true")
					require.Equal(t, tt.wantStaticPeers, ctx.String("staticpeers"))
				}
				if tt.wantSentinelStaticPeers != nil {
					require.True(t, ctx.IsSet("sentinel.staticpeers"), "ctx.IsSet(sentinel.staticpeers) should be true")
					require.Equal(t, tt.wantSentinelStaticPeers, ctx.StringSlice("sentinel.staticpeers"))
				}
				return nil
			}

			err := app.Run([]string{"erigon"})
			require.NoError(t, err)
		})
	}
}
