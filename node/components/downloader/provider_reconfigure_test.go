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

package downloader

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/downloader/downloadercfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

var _ rpchelper.ChainConfigReconfigurable = (*Provider)(nil)

// TestReconfigure_UpdatesChainName covers the current Reconfigure
// surface: it swaps p.cfg.ChainName when the new chain.Config carries
// one, and errors on nil. Full torrent-client rebuild for a fork
// transition lands with Phase 2 wiring.
func TestReconfigure_UpdatesChainName(t *testing.T) {
	p := &Provider{cfg: &downloadercfg.Cfg{ChainName: "hoodi"}}
	require.NoError(t, p.Reconfigure(context.Background(), &chain.Config{ChainName: "hoodi-fork-42"}))
	require.Equal(t, "hoodi-fork-42", p.cfg.ChainName)

	require.Error(t, p.Reconfigure(context.Background(), nil))
}
