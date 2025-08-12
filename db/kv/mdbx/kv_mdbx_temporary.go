// Copyright 2021 The Erigon Authors
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

package mdbx

import (
	"context"
	"os"
	"unsafe"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

type TemporaryMdbx struct {
	db   kv.RwDB
	path string
}

func NewTemporaryMdbx(ctx context.Context, tempdir string) (kv.RwDB, error) {
	path, err := os.MkdirTemp(tempdir, "mdbx-temp")
	if err != nil {
		return &TemporaryMdbx{}, err
	}

	db, err := New(kv.ChainDB, log.Root()).InMem(path).Open(ctx)
	if err != nil {
		return &TemporaryMdbx{}, err
	}

	return &TemporaryMdbx{
		db:   db,
		path: path,
	}, nil
}

func NewUnboundedTemporaryMdbx(ctx context.Context, tempdir string) (kv.RwDB, error) {
	path, err := os.MkdirTemp(tempdir, "mdbx-temp")
	if err != nil {
		return &TemporaryMdbx{}, err
	}

	db, err := New(kv.ChainDB, log.Root()).InMem(path).MapSize(32 * datasize.TB).PageSize(16 * datasize.KB).Open(ctx)
	if err != nil {
		return &TemporaryMdbx{}, err
	}

	return &TemporaryMdbx{
		db:   db,
		path: path,
	}, nil
}

func (t *TemporaryMdbx) ReadOnly() bool { return t.db.ReadOnly() }
func (t *TemporaryMdbx) Update(ctx context.Context, f func(kv.RwTx) error) error {
	return t.db.Update(ctx, f)
}

func (t *TemporaryMdbx) UpdateNosync(ctx context.Context, f func(kv.RwTx) error) error {
	return t.db.UpdateNosync(ctx, f)
}

func (t *TemporaryMdbx) BeginRw(ctx context.Context) (kv.RwTx, error) {
	return t.db.BeginRw(ctx)
}
func (t *TemporaryMdbx) BeginRwNosync(ctx context.Context) (kv.RwTx, error) {
	return t.db.BeginRwNosync(ctx)
}

func (t *TemporaryMdbx) View(ctx context.Context, f func(kv.Tx) error) error {
	return t.db.View(ctx, f)
}

func (t *TemporaryMdbx) BeginRo(ctx context.Context) (kv.Tx, error) {
	return t.db.BeginRo(ctx)
}

func (t *TemporaryMdbx) AllTables() kv.TableCfg {
	return t.db.AllTables()
}

func (t *TemporaryMdbx) PageSize() datasize.ByteSize {
	return t.db.PageSize()
}

func (t *TemporaryMdbx) Close() {
	t.db.Close()
	dir.RemoveAll(t.path)
}

func (t *TemporaryMdbx) CHandle() unsafe.Pointer {
	panic("CHandle not implemented")
}
