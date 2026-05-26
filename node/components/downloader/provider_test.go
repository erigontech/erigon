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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/node/ethconfig"
)

func TestProviderDisabled(t *testing.T) {
	p := &Provider{}
	p.Configure(
		nil, // no downloader cfg
		ethconfig.BlocksFreezing{NoDownloader: true},
		datadir.New(t.TempDir()),
		log.Root(),
		nil,
	)

	err := p.Initialize(t.Context())
	require.NoError(t, err)
	require.False(t, p.IsEnabled())
	require.Nil(t, p.Client)
	require.Nil(t, p.Downloader)

	// Close is safe on disabled provider
	p.Close()
}

func TestProviderNoConfig(t *testing.T) {
	p := &Provider{}
	p.Configure(
		nil, // no downloader cfg
		ethconfig.BlocksFreezing{},
		datadir.New(t.TempDir()),
		log.Root(),
		nil,
	)

	err := p.Initialize(t.Context())
	require.NoError(t, err)
	// No config → no client, but no error
	require.Nil(t, p.Client)
}

func TestProviderCloseIdempotent(t *testing.T) {
	p := &Provider{}
	// Close without Initialize should not panic
	p.Close()
	p.Close()
}

// TestActivateOnNilProvider pins the nil-safety guard so a misconfigured
// caller gets a clear error rather than a nil-deref panic.
func TestActivateOnNilProvider(t *testing.T) {
	var p *Provider
	err := p.Activate(t.Context())
	require.Error(t, err)
	require.Contains(t, err.Error(), "nil provider")
}

// TestActivateNoDownloader pins that the activate phase is a clean no-op
// (returns nil, marks activated) when the Provider has no underlying
// Downloader (NoDownloader build, external mode, or no chain config).
func TestActivateNoDownloader(t *testing.T) {
	p := &Provider{}
	p.Configure(
		nil,
		ethconfig.BlocksFreezing{NoDownloader: true},
		datadir.New(t.TempDir()),
		log.Root(),
		nil,
	)
	require.NoError(t, p.Initialize(t.Context()))
	require.Nil(t, p.Downloader, "NoDownloader: no underlying downloader constructed")

	require.NoError(t, p.Activate(t.Context()))
	require.True(t, p.activated, "Activate must record the lifecycle edge even on no-op path")

	// Idempotent: second Activate is a no-op + still returns nil.
	require.NoError(t, p.Activate(t.Context()))
}
