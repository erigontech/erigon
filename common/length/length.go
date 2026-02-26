// Copyright 2021 The Erigon Authors
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

package length

// Lengths of hashes and addresses in bytes.
const (
	PeerID = 64
	// Hash is the expected length of the hash (in bytes)
	Hash = 32
	// expected length of Bytes96 (signature)
	Bytes96 = 96
	// expected length of Bytes48 (bls public key and such)
	Bytes48 = 48
	// expected length of Bytes64 (sync committee bits)
	Bytes64 = 64
	// expected length of Bytes48 (beacon domain and such)
	Bytes4 = 4
	// Addr is the expected length of the address (in bytes)
	Addr = 20
	// BlockNumberLen length of uint64 big endian
	BlockNum = 8
	// Ts TimeStamp (BlockNum, TxNum or any other uint64 equivalent of Time)
	Ts = 8
	// Incarnation length of uint64 for contract incarnations
	Incarnation = 8
)
