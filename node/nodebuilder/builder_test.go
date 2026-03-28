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

package nodebuilder_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/nodebuilder"
)

func TestNewAllocatesAllProviders(t *testing.T) {
	b := nodebuilder.New()
	require.NotNil(t, b.Downloader, "Downloader provider must be allocated")
}

func TestCloseIsIdempotent(t *testing.T) {
	b := nodebuilder.New()
	require.NotPanics(t, func() { b.Close() })
	require.NotPanics(t, func() { b.Close() })
}

func TestStartNoopWhenNotInitialized(t *testing.T) {
	b := nodebuilder.New()
	require.NotPanics(t, func() {
		b.Start(t.Context(), &noopEg{})
	})
}

type noopEg struct{}

func (*noopEg) Go(_ func() error) {}
