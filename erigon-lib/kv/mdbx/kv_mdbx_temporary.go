/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package mdbx

import (
	"context"
	"os"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"
)

type TemporaryMdbx struct {
	db   kv.RwDB
	path string
}

func NewTemporaryMdbx(tempdir string) (kv.RwDB, error) {
	path, err := os.MkdirTemp(tempdir, "mdbx-temp")
	if err != nil {
		return &TemporaryMdbx{}, err
	}

	db, err := Open(path, log.Root(), false)
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

func (t *TemporaryMdbx) PageSize() uint64 {
	return t.db.PageSize()
}

func (t *TemporaryMdbx) Close() {
	t.db.Close()
	os.RemoveAll(t.path)
}
