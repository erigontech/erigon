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

package peerdasstate

import "math/big"

// EncodeCgc renders a custody group count for the ENR: big endian with no leading zero
// bytes, so zero is the empty byte string. Shared with the seed written at ENR setup so
// both sites agree on the format.
//
// Spec: fulu/p2p-interface.md, "Custody group count".
func EncodeCgc(cgc uint64) []byte {
	return new(big.Int).SetUint64(cgc).Bytes()
}
