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

package caplin1

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
)

// TestOpenCaplinDatabase_CloseReleasesBothEnvs pins that closing releases the
// blob env as well as the index env, so the same paths can be opened again.
//
// CaplinService.Restart tears the instance down and immediately reopens these
// paths. An env still held by the previous generation fails the reopen with
// MDBX "resource temporarily unavailable", which kills Caplin — and with it
// the only thing driving the node forward after a SetHead.
func TestOpenCaplinDatabase_CloseReleasesBothEnvs(t *testing.T) {
	dir := t.TempDir()
	idxPath := filepath.Join(dir, "caplin-indexing")
	blobPath := filepath.Join(dir, "caplin-blobs")
	_, cfg, _, err := clparams.GetConfigsByNetworkName("mainnet")
	require.NoError(t, err)

	_, _, closeFn, err := OpenCaplinDatabase(t.Context(), cfg, idxPath, blobPath, nil, false)
	require.NoError(t, err)
	require.NotNil(t, closeFn, "caller needs a synchronous close before the next open")
	closeFn()

	require.NotPanics(t, func() {
		_, _, closeAgain, err := OpenCaplinDatabase(t.Context(), cfg, idxPath, blobPath, nil, false)
		require.NoError(t, err)
		closeAgain()
	}, "reopening after close must succeed — a leaked env makes Restart fail with MDBX busy")
}
