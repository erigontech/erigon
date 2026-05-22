// Copyright 2025 The Erigon Authors
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
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/version"
)

func SetErigonVersion(tx kv.RwTx, versionKey string) error {
	versionKeyByte := []byte(versionKey)
	hasVersion, err := tx.Has(kv.DatabaseInfo, versionKeyByte)
	if err != nil {
		return err
	}
	if hasVersion {
		return nil
	}
	// Save version if it does not exist
	if err := tx.Put(kv.DatabaseInfo, versionKeyByte, []byte(version.VersionNoMeta)); err != nil {
		return err
	}
	return nil
}
