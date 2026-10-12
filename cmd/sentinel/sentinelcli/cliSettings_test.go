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

package sentinelcli

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"

	"github.com/erigontech/erigon/cmd/sentinel/sentinelflags"
	"github.com/erigontech/erigon/node/logging"
)

func TestSetupSentinelCliUsesIndependentQUICPort(t *testing.T) {
	discoveryPort := sentinelflags.SentinelDiscoveryPort
	tcpPort := sentinelflags.SentinelTcpPort
	quicPort := sentinelflags.SentinelQUICPort
	verbosity := logging.LogVerbosityFlag
	cmd := &cli.Command{Flags: []cli.Flag{&discoveryPort, &tcpPort, &quicPort, &verbosity}}
	cmd.Action = func(_ context.Context, cmd *cli.Command) error {
		cfg, err := SetupSentinelCli(cmd)
		require.NoError(t, err)
		require.Equal(t, uint(9000), cfg.Port)
		require.Equal(t, uint(9000), cfg.ServerTcpPort)
		require.Equal(t, uint(9001), cfg.ServerQUICPort)
		return nil
	}

	require.NoError(t, cmd.Run(t.Context(), []string{
		"caplin",
		"--discovery.port=9000",
		"--sentinel.tcp.port=9000",
		"--sentinel.quic.port=9001",
	}))
}
