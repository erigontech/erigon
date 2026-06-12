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

// Code is the canonical representation of EVM bytecode: an interned CodeHash
// paired with the bytes the hash was computed over. Constructed only by
// cache.StateCache.PutCode at write boundaries (CREATE/CREATE2 finalize,
// EIP-7702 SetCodeTx, genesis writes, RPC state overrides, chain-rule
// rewrites). Downstream consumers read code.Bytes as a non-mutating borrow
// of the cache-owned slice; the Hash field is the interned codeHash so
// equality is pointer-compare and no consumer needs to re-Keccak the bytes.
//
// INVARIANT: Hash must equal Keccak256(Bytes). PutCode trusts the caller
// to compute the hash once; downstream paths must NOT mutate Bytes. There
// is no mutating method on Code by design.
type Code struct {
	Hash  CodeHash
	Bytes []byte
}

// EmptyCode is the canonical empty bytecode value (len 0, Hash = EmptyCodeHash).
var EmptyCode = Code{Hash: EmptyCodeHash}

func (c Code) Len() int      { return len(c.Bytes) }
func (c Code) IsEmpty() bool { return len(c.Bytes) == 0 }
