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
	"errors"
	"fmt"
	"strings"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
	"github.com/erigontech/erigon/execution/types/accounts"
)

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

	nib := nibbles.CompactToHex(branchKey)
	if nibbles.HasTerm(nib) {
		nib = nib[:len(nib)-1]
	}
	// +1: cells in this branch are one nibble deeper than the branch's own prefix.
	depth := int16(len(nib)) + 1

	var mismatches []string

	for nibble := range 16 {
		c := row[nibble]
		if c == nil {
			continue
		}
		if c.stateHashLen == 0 {
			continue // no stored hash to verify
		}

		var origHash common.Hash
		copy(origHash[:], c.stateHash[:c.stateHashLen])
		origLen := c.stateHashLen

		canVerify := true
		if c.storageAddrLen > 0 {
			stoKeyBytes := c.storageAddr[:c.storageAddrLen]
			stoVal, ok := storageValues[string(stoKeyBytes)]
			if !ok || len(stoVal) == 0 {
				canVerify = false
			} else {
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

		c.stateHashLen = 0

		verifyCfg := DefaultTrieConfig()
		verifyCfg.MemoizationOff = true
		hph := NewHexPatriciaHashed(length.Addr, nil, verifyCfg)

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
		var sb strings.Builder
		fmt.Fprintf(&sb, "hash verification failed with %d mismatch(es) at branchKey=%x:", len(mismatches), branchKey)
		for _, m := range mismatches {
			sb.WriteString("\n  ")
			sb.WriteString(m)
		}
		return errors.New(sb.String())
	}
	return nil
}
