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

package commitment

import (
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// VerifyBranchHashes checks that stateHash in each cell of a branch
// matches the hash recomputed from the provided domain values.
//
// branchKey: compacted trie path from commitment.kv
// branchData: raw branch data (with PLAIN keys, already dereferenced)
// accountValues: string(plainKey) → serialised account value (V3 format)
// storageValues: string(plainKey) → raw storage value
//
// Returns nil if all hashes match, or an error listing mismatches.
func VerifyBranchHashes(
	branchKey []byte,
	branchData BranchData,
	accountValues map[string][]byte,
	storageValues map[string][]byte,
) error {
	_, _, row, err := branchData.decodeCells()
	if err != nil {
		return fmt.Errorf("decodeCells: %w", err)
	}

	// Derive depth from branchKey (compacted nibble path).
	// The branch node identified by N nibbles was folded at depth = N + 1,
	// because the branch stores children one level deeper than its prefix path.
	// E.g., root branch (0 nibbles) → depth=1; branch at "3a" (2 nibbles) → depth=3.
	nibbles := uncompactNibbles(branchKey)
	if HasTerm(nibbles) {
		nibbles = nibbles[:len(nibbles)-1]
	}
	depth := int16(len(nibbles)) + 1

	var mismatches []string

	for nibble := 0; nibble < 16; nibble++ {
		c := row[nibble]
		if c == nil {
			continue
		}
		if c.stateHashLen == 0 {
			continue // no stored hash to verify
		}

		// Save the original stateHash for comparison.
		var origHash common.Hash
		copy(origHash[:], c.stateHash[:c.stateHashLen])
		origLen := c.stateHashLen

		// Load domain values into cell fields and mark as loaded.
		// If a value is missing or empty (deletion), skip this cell — it's not verifiable.
		canVerify := true
		if c.storageAddrLen > 0 {
			stoKeyBytes := c.storageAddr[:c.storageAddrLen]
			stoVal, ok := storageValues[string(stoKeyBytes)]
			if !ok || len(stoVal) == 0 {
				canVerify = false
			} else {
				// Storage value is stored raw in the cell's Storage field.
				copy(c.Storage[:], stoVal)
				c.StorageLen = int8(len(stoVal))
				c.loaded = c.loaded.addFlag(cellLoadStorage)
			}
		}
		if c.accountAddrLen > 0 && canVerify {
			accKeyBytes := c.accountAddr[:c.accountAddrLen]
			accVal, ok := accountValues[string(accKeyBytes)]
			if !ok || len(accVal) == 0 {
				canVerify = false
			} else {
				var acc accounts.Account
				if err := accounts.DeserialiseV3(&acc, accVal); err != nil {
					mismatches = append(mismatches, fmt.Sprintf(
						"nibble %x: failed to deserialise account %x: %v", nibble, accKeyBytes, err))
					continue
				}
				c.Nonce = acc.Nonce
				c.Balance.Set(&acc.Balance)
				c.CodeHash = acc.CodeHash.Value()
				if c.CodeHash == (common.Hash{}) {
					c.CodeHash = empty.CodeHash
				}
				c.Flags = BalanceUpdate | NonceUpdate | CodeUpdate
				c.loaded = c.loaded.addFlag(cellLoadAccount)
			}
		}
		if !canVerify {
			continue
		}

		// Clear stateHash to force recomputation.
		c.stateHashLen = 0

		// Create a fresh HexPatriciaHashed for each cell to avoid any shared state issues.
		hph := NewHexPatriciaHashed(length.Addr, nil)
		hph.memoizationOff = true

		computed, err := hph.computeCellHash(c, depth, nil)
		if err != nil {
			mismatches = append(mismatches, fmt.Sprintf(
				"nibble %x: computeCellHash error: %v", nibble, err))
			continue
		}

		// computed is [0xa0, hash...] (33 bytes) for non-embedded leaves.
		// origHash is just the 32-byte hash (without the 0xa0 prefix).
		if len(computed) >= 33 && computed[0] == 0xa0 {
			computed = computed[1:]
		}

		if origLen != int16(len(computed)) || common.Hash(computed[:origLen]) != origHash {
			mismatches = append(mismatches, fmt.Sprintf(
				"nibble %x: stateHash mismatch: stored=%x computed=%x (branchKey=%x)",
				nibble, origHash[:origLen], computed[:min(int(origLen), len(computed))], branchKey))
		}
	}

	if len(mismatches) > 0 {
		msg := fmt.Sprintf("hash verification failed with %d mismatch(es) at branchKey=%x:", len(mismatches), branchKey)
		for _, m := range mismatches {
			msg += "\n  " + m
		}
		return fmt.Errorf("%s", msg)
	}
	return nil
}
