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

package downloadercfg

import (
	"testing"

	g "github.com/anacrolix/generics"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
)

func newTestCfg(t *testing.T, opts NewCfgOpts) *Cfg {
	t.Helper()
	cfg, err := New(
		t.Context(),
		datadir.New(t.TempDir()),
		"",
		log.LvlInfo,
		0, 0,
		nil,
		"testnet",
		false,
		opts,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, cfg.CloseTorrentLogFile())
	})
	return cfg
}

func TestDisableTCPOpt(t *testing.T) {
	t.Run("unset leaves TCP enabled", func(t *testing.T) {
		cfg := newTestCfg(t, NewCfgOpts{})
		require.False(t, cfg.ClientConfig.DisableTCP)
	})
	t.Run("true disables TCP", func(t *testing.T) {
		cfg := newTestCfg(t, NewCfgOpts{DisableTCP: g.Some(true)})
		require.True(t, cfg.ClientConfig.DisableTCP)
	})
	t.Run("false leaves TCP enabled", func(t *testing.T) {
		cfg := newTestCfg(t, NewCfgOpts{DisableTCP: g.Some(false)})
		require.False(t, cfg.ClientConfig.DisableTCP)
	})
	t.Run("does not affect uTP", func(t *testing.T) {
		cfg := newTestCfg(t, NewCfgOpts{DisableTCP: g.Some(true)})
		require.False(t, cfg.ClientConfig.DisableUTP)
	})
}
