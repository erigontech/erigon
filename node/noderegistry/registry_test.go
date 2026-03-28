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

package noderegistry_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/noderegistry"
)

func TestNewAllocatesAllProviders(t *testing.T) {
	r := noderegistry.New()
	require.NotNil(t, r.Downloader, "Downloader provider must be allocated")
	require.NotNil(t, r.TxPool, "TxPool provider must be allocated")
	require.NotNil(t, r.Shutter, "Shutter provider must be allocated")
}

func TestCloseIsIdempotent(t *testing.T) {
	r := noderegistry.New()
	// Close on a freshly-allocated registry (no underlying resources) must not panic.
	require.NotPanics(t, func() { r.Close() })
	require.NotPanics(t, func() { r.Close() })
}

func TestStartNoopWhenNotInitialized(t *testing.T) {
	r := noderegistry.New()
	// Start on a registry whose providers have not been initialized must not panic.
	// (TxPool.Pool == nil and Shutter.Pool == nil; both Start methods are no-ops.)
	require.NotPanics(t, func() {
		r.Start(t.Context(), &noopEg{})
	})
}

type noopEg struct{}

func (*noopEg) Go(_ func() error) {}
