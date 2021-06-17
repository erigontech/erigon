// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package ethdb

import (
	"context"
	"testing"
)

func NewMemDatabase() *ObjectDatabase {
	return NewObjectDatabase(NewMemKV())
}

func NewTestDB(t testing.TB) *ObjectDatabase {
	return NewObjectDatabase(NewTestKV(t))
}

func NewMemKV() RwKV {
	return NewMDBX().InMem().MustOpen()
}

func NewTestKV(t testing.TB) RwKV {
	kv := NewMemKV()
	switch tt := t.(type) {
	case *testing.T:
		if tt != nil {
			tt.Cleanup(kv.Close)
		}
	}
	return kv
}

func NewTestTx(t testing.TB) (RwKV, RwTx) {
	kv := NewMemKV()
	switch tt := t.(type) {
	case *testing.T:
		if tt != nil {
			tt.Cleanup(kv.Close)
		}
	}
	tx, err := kv.BeginRw(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	switch tt := t.(type) {
	case *testing.T:
		if tt != nil {
			tt.Cleanup(tx.Rollback)
		}
	}
	return kv, tx
}
