// Copyright 2024 The Erigon Authors
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

package clique

import (
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv/dbutils"
)

// SnapshotFullKey = SnapshotBucket + num (uint64 big endian) + hash
func SnapshotFullKey(number uint64, hash common.Hash) []byte {
	return append(dbutils.EncodeBlockNumber(number), hash.Bytes()...)
}

// SnapshotKey = SnapshotBucket + num (uint64 big endian)
func SnapshotKey(number uint64) []byte {
	return dbutils.EncodeBlockNumber(number)
}

// SnapshotKey = SnapshotBucket + '0'
func LastSnapshotKey() []byte {
	return []byte{0}
}
