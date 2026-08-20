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

package fromdb

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
)

type errorViewDB struct {
	kv.RoDB
	err error
}

func (db errorViewDB) View(context.Context, func(kv.Tx) error) error {
	return db.err
}

type callbackViewDB struct {
	kv.RoDB
	tx kv.Tx
}

func (db callbackViewDB) View(_ context.Context, f func(kv.Tx) error) error {
	return f(db.tx)
}

type getErrorTx struct {
	kv.Tx
	err error
}

func (tx getErrorTx) GetOne(string, []byte) ([]byte, error) {
	return nil, tx.err
}

func TestPruneModeReturnsViewError(t *testing.T) {
	wantErr := errors.New("view failed")

	_, err := PruneMode(errorViewDB{err: wantErr})
	require.ErrorIs(t, err, wantErr)
}

func TestPruneModeReturnsReadError(t *testing.T) {
	wantErr := errors.New("prune read failed")

	_, err := PruneMode(callbackViewDB{tx: getErrorTx{err: wantErr}})
	require.ErrorIs(t, err, wantErr)
}
