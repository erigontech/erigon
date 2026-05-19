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

package state

import (
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/cache"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// PrewarmBlockStateCacheFromBAL pre-populates the per-block committed cache
// from in-memory cache.StateCache hits for addresses + slots referenced in
// the block's BAL. Misses are skipped — the lazy CachedReaderV3 fill handles
// cold reads as usual, so this is purely additive. RAM-to-RAM, no MDBX, no
// snapshot probes; safe to call concurrently with the async BlockReadAheader
// warm that populates cache.StateCache.
//
// The win: EVM first-touch read of a BAL-listed addr/slot is a single
// BlockStateCache map probe instead of the fallthrough chain
// (BlockStateCache miss → CachedReaderV3 → cache.StateCache hit → backfill).
// One indirection saved per address, and the saved path avoids the
// fallthrough's atomic counter / metric overhead.
func PrewarmBlockStateCacheFromBAL(bsc *BlockStateCache, bal types.BlockAccessList, sc *cache.StateCache) {
	if bsc == nil || sc == nil || len(bal) == 0 {
		return
	}
	var slotKey [52]byte
	for i := range bal {
		entry := bal[i]
		addr := entry.Address.Value()
		// AccountsDomain holds the serialized V3 encoding. Empty bytes are
		// a valid negative answer (account doesn't exist) — propagate that
		// through as PutCommittedAccount(nil) so CachedReaderV3 short-circuits
		// instead of fall-through-and-re-cache.
		if accBytes, ok := sc.Get(kv.AccountsDomain, addr[:]); ok {
			if len(accBytes) == 0 {
				bsc.PutCommittedAccount(entry.Address, nil)
			} else {
				var acc accounts.Account
				if err := accounts.DeserialiseV3(&acc, accBytes); err == nil {
					bsc.PutCommittedAccount(entry.Address, &acc)
				}
			}
		}
		copy(slotKey[:20], addr[:])
		for j := range entry.StorageChanges {
			slot := entry.StorageChanges[j].Slot.Value()
			copy(slotKey[20:], slot[:])
			if val, ok := sc.Get(kv.StorageDomain, slotKey[:]); ok {
				bsc.PutCommittedStorage(entry.Address, entry.StorageChanges[j].Slot, val)
			}
		}
		for j := range entry.StorageReads {
			slot := entry.StorageReads[j].Value()
			copy(slotKey[20:], slot[:])
			if val, ok := sc.Get(kv.StorageDomain, slotKey[:]); ok {
				bsc.PutCommittedStorage(entry.Address, entry.StorageReads[j], val)
			}
		}
	}
}
