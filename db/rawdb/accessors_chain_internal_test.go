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

package rawdb

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
)

type forEachErrorTx struct {
	kv.Tx
	err error
}

func (tx forEachErrorTx) ForEach(string, []byte, func([]byte, []byte) error) error {
	return tx.err
}

func TestGetLatestBadBlocksPropagatesCacheInitError(t *testing.T) {
	wantErr := errors.New("cache read failed")

	bheapMu.Lock()
	previousCache := bheapCache
	bheapCache = nil
	bheapMu.Unlock()
	t.Cleanup(func() {
		bheapMu.Lock()
		bheapCache = previousCache
		bheapMu.Unlock()
	})

	blocks, err := GetLatestBadBlocks(forEachErrorTx{err: wantErr})
	require.ErrorIs(t, err, wantErr)
	require.Nil(t, blocks)
}
