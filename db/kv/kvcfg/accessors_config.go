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
	"errors"

	"github.com/erigontech/erigon/db/kv"
)

type ConfigKey []byte

var (
	PersistReceipts   = ConfigKey("persist.receipts")
	CommitmentHistory = ConfigKey("commitment.history")
)

func (k ConfigKey) Enabled(tx kv.Tx) (bool, error) { return kv.GetBool(tx, kv.DatabaseInfo, k) }
func (k ConfigKey) WriteOnce(tx kv.RwTx, v bool) (bool, error) {
	_, enabled, err := kv.EnsureNotChangedBool(tx, kv.DatabaseInfo, k, v)
	return enabled, err
}
func (k ConfigKey) EnsureNotChanged(tx kv.RwTx, value bool) (notChanged, enabled bool, err error) {
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
func (k ConfigKey) MustBeEnabled(tx kv.Tx, msg string) error {
	enabled, err := k.Enabled(tx)
	if err != nil {
		return err
	}
	if !enabled {
		return errors.New(msg)
	}
	return nil
}
