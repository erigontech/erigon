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
	"encoding/binary"
	"fmt"

	"github.com/erigontech/erigon/db/kv"
)

// putShadowHistorySynth writes a paired history entry (K → txN in
// KeysTable, (K, txN, prevVal) in ValuesTable) alongside a shadow
// domain-value write. Without this pairing, retire's History.collate
// misses K when building .ef for the step and produces a .kv without
// a matching .ef entry — the mode-D wrong-root failure shape.
//
// Callers pass prevVal = the shadow write's value, so HistorySeek
// returns the same value the shadow row carries. Called AFTER the
// paired shadow-value Put AND AFTER any pre-write history prune, so
// the entry survives prune's [txFrom, ∞) removal.
//
// No-op when the domain disables history (commitment, rcache).
func putShadowHistorySynth(rwTx kv.RwTx, d *Domain, k, prevVal []byte, txN uint64) error {
	if d.HistoryDisabled {
		return nil
	}
	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], txN)

	if err := rwTx.Put(d.History.KeysTable, txKey[:], k); err != nil {
		return fmt.Errorf("putShadowHistorySynth keys(%s, txN=%d, k=%x): %w",
			d.History.KeysTable, txN, k, err)
	}

	if d.History.HistoryLargeValues {
		vk := make([]byte, 0, len(k)+8)
		vk = append(vk, k...)
		vk = append(vk, txKey[:]...)
		if err := rwTx.Put(d.History.ValuesTable, vk, prevVal); err != nil {
			return fmt.Errorf("putShadowHistorySynth vals-large(%s, k=%x, txN=%d): %w",
				d.History.ValuesTable, k, txN, err)
		}
		return nil
	}

	val := make([]byte, 0, 8+len(prevVal))
	val = append(val, txKey[:]...)
	val = append(val, prevVal...)
	if err := rwTx.Put(d.History.ValuesTable, k, val); err != nil {
		return fmt.Errorf("putShadowHistorySynth vals-dup(%s, k=%x, txN=%d): %w",
			d.History.ValuesTable, k, txN, err)
	}
	return nil
}
