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

package cache

import (
	"unsafe"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
)

// KECCAK_CACHE_MODE=lru backs the keccak memo with a ByteLRU sized for the fixed table's 2^17 entries.
func init() {
	if dbg.EnvString("KECCAK_CACHE_MODE", "") != "lru" {
		return
	}
	entryBytes := int64(unsafe.Sizeof(crypto.KeccakCacheEntry{})) + ByteLRUEntryOverheadBytes
	crypto.SetKeccakLRU(NewByteLRU[*crypto.KeccakCacheEntry](datasize.ByteSize(entryBytes)<<17, func(uint64, *crypto.KeccakCacheEntry) int64 { return entryBytes }))
}
