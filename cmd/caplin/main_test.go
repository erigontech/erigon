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

package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/cmd/caplin/caplincli"
	"github.com/erigontech/erigon/cmd/sentinel/sentinelflags"
	"github.com/erigontech/erigon/node/logging"
)

func TestCaplinSentinelBindFlags(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
		host string
		port uint64
	}{
		{name: "defaults", host: "localhost", port: 7777},
		{name: "custom", args: []string{"--sentinel.addr=127.0.0.2", "--sentinel.port=8888"}, host: "127.0.0.2", port: 8888},
		{name: "ephemeral", args: []string{"--sentinel.addr=127.0.0.1", "--sentinel.port=0"}, host: "127.0.0.1", port: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			addr := sentinelflags.SentinelServerAddr
			port := sentinelflags.SentinelServerPort
			verbosity := logging.LogVerbosityFlag
			cmd := &cli.Command{
				Flags: []cli.Flag{&addr, &port, &verbosity},
				Action: func(_ context.Context, cmd *cli.Command) error {
					cfg, err := caplincli.SetupCaplinCli(cmd)
					require.NoError(t, err)
					config := newCaplinConfig(cmd, cfg)
					require.Equal(t, test.host, config.SentinelAddr)
					require.Equal(t, test.port, config.SentinelPort)
					return nil
				},
			}
			require.NoError(t, cmd.Run(t.Context(), append([]string{"caplin"}, test.args...)))
		})
	}
}
