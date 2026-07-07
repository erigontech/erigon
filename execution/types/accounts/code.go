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

package accounts

import "github.com/erigontech/erigon/common/crypto"

// Code pairs EVM bytecode with its interned CodeHash.
//
// INVARIANT: Hash == Keccak256(Bytes), computed once at construction. Bytes is
// a non-mutating borrow (often of a cache-owned slice) and must not be
// modified, so equality reduces to comparing the interned Hash without
// re-hashing. Code has no mutating method by design.
type Code struct {
	Hash  CodeHash
	Bytes []byte
}

// EmptyCode is the canonical empty bytecode value (len 0, Hash = EmptyCodeHash).
var EmptyCode = Code{Hash: EmptyCodeHash}

// NewCode builds a Code from raw bytecode, hashing once so the
// Hash == Keccak256(Bytes) invariant holds by construction.
func NewCode(bytes []byte) Code {
	if len(bytes) == 0 {
		return EmptyCode
	}
	return Code{Hash: InternCodeHash(crypto.HashData(bytes)), Bytes: bytes}
}

func (c Code) Len() int      { return len(c.Bytes) }
func (c Code) IsEmpty() bool { return len(c.Bytes) == 0 }
