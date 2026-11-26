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

package backtester

import (
	"io"
	"testing"

	"github.com/felixge/fgprof"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/db/state/statecfg"
)

func TestBacktesterMatchesHeaderRootsOnCommitmentV3(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(true))
	statecfg.EnableHistoricalCommitment()
	m, chain, _ := rpcdaemontest.CreateTestExecModule(t)

	bt := New(m.Log, m.DB, m.BlockReader, t.TempDir())
	require.NoError(t, bt.Run(t.Context(), 1, uint64(chain.Length())))
}

func TestStopFgprofProfileWithoutSamples(t *testing.T) {
	stop := fgprof.Start(io.Discard, fgprof.FormatPprof)
	require.NoError(t, stopFgprofProfile(stop))
}
