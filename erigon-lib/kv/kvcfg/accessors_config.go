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

package kvcfg

import (
	"context"

	"github.com/erigontech/erigon-lib/kv"
)

type ConfigKey []byte

func (k ConfigKey) Enabled(tx kv.Tx) (bool, error) { return kv.GetBool(tx, kv.DatabaseInfo, k) }
func (k ConfigKey) FromDB(db kv.RoDB) (enabled bool) {
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		var err error
		enabled, err = k.Enabled(tx)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		panic(err)
	}
	return enabled
}
func (k ConfigKey) WriteOnce(tx kv.RwTx, v bool) (bool, error) {
	_, enabled, err := kv.EnsureNotChangedBool(tx, kv.DatabaseInfo, k, v)
	return enabled, err
}
func (k ConfigKey) EnsureNotChanged(tx kv.RwTx, value bool) (ok, enabled bool, err error) {
	return kv.EnsureNotChangedBool(tx, kv.DatabaseInfo, k, value)
}
func (k ConfigKey) ForceWrite(tx kv.RwTx, enabled bool) error {
	if enabled {
		if err := tx.Put(kv.DatabaseInfo, k, []byte{1}); err != nil {
			return err
		}
	} else {
		if err := tx.Put(kv.DatabaseInfo, k, []byte{0}); err != nil {
			return err
		}
	}
	return nil
}
