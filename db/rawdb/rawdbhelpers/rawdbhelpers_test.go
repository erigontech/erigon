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

package rawdbhelpers

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
)

type cursorTx struct {
	kv.Tx
	cursor kv.Cursor
	err    error
}

func (tx cursorTx) Cursor(string) (kv.Cursor, error) {
	return tx.cursor, tx.err
}

type errorCursor struct {
	kv.Cursor
	first    []byte
	last     []byte
	firstErr error
	lastErr  error
}

func (c errorCursor) First() ([]byte, []byte, error) {
	return c.first, nil, c.firstErr
}

func (c errorCursor) Last() ([]byte, []byte, error) {
	return c.last, nil, c.lastErr
}

func (errorCursor) Close() {}

func TestIdxStepsInDBPropagatesFirstKeyError(t *testing.T) {
	wantErr := errors.New("first key failed")

	steps, err := IdxStepsInDB(cursorTx{cursor: errorCursor{firstErr: wantErr}}, "table", 10)
	require.ErrorIs(t, err, wantErr)
	require.Zero(t, steps)
}

func TestIdxStepsInDBPropagatesLastKeyError(t *testing.T) {
	wantErr := errors.New("last key failed")

	steps, err := IdxStepsInDB(cursorTx{cursor: errorCursor{first: make([]byte, 8), lastErr: wantErr}}, "table", 10)
	require.ErrorIs(t, err, wantErr)
	require.Zero(t, steps)
}

func TestIdxStepsInDB(t *testing.T) {
	first := make([]byte, 8)
	last := make([]byte, 8)
	binary.BigEndian.PutUint64(first, 20)
	binary.BigEndian.PutUint64(last, 70)

	steps, err := IdxStepsInDB(cursorTx{cursor: errorCursor{first: first, last: last}}, "table", 10)
	require.NoError(t, err)
	require.Equal(t, 5.0, steps)
}
