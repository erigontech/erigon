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

package eip8297

import (
	"encoding/binary"
	"fmt"

	keccak "github.com/erigontech/fastkeccak"
	"lukechampine.com/blake3"

	"github.com/erigontech/erigon/common"
)

const (
	LeafTag   = 0x00
	BranchTag = 0x01

	HashKeccak = "keccak"
	HashBlake3 = "blake3"
)

var EmptyTreeHash common.Hash

type HashFn func([]byte) common.Hash

var selectedHash HashFn

func SetHashSuite(name string) error {
	switch name {
	case "", HashKeccak:
		selectedHash = nil
	case HashBlake3:
		selectedHash = func(b []byte) common.Hash { return common.Hash(blake3.Sum256(b)) }
	default:
		return fmt.Errorf("unknown bin commitment hash %q, want %q or %q", name, HashKeccak, HashBlake3)
	}
	return nil
}

func HashSuiteName() string {
	if selectedHash == nil {
		return HashKeccak
	}
	return HashBlake3
}

func SelectedHash() HashFn { return selectedHash }

func HashBytes(preimage []byte) common.Hash {
	if selectedHash != nil {
		return selectedHash(preimage)
	}
	return keccak.Sum256(preimage)
}

func AppendBitPrefix(dst []byte, p *Bitpath) []byte {
	return p.AppendPackedBits(binary.BigEndian.AppendUint16(dst, uint16(p.BitLen)))
}

func LeafPreimage(dst, key, value []byte) []byte {
	dst = append(dst, LeafTag)
	dst = append(dst, key...)
	return append(dst, value...)
}

func BranchPreimage(dst []byte, prefix *Bitpath, left, right *common.Hash) []byte {
	dst = append(dst, BranchTag)
	dst = AppendBitPrefix(dst, prefix)
	dst = append(dst, left[:]...)
	return append(dst, right[:]...)
}
