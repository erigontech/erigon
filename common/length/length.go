/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

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
