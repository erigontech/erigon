// Copyright 2024 The Erigon Authors
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

package secp256r1

import (
	"crypto/ecdsa"
	"math/big"
)

// Verify verifies the given signature (r, s) for the given hash and public key (x, y).
func Verify(hash []byte, r, s, x, y *big.Int) bool {
	// Create the public key format
	publicKey := newPublicKey(x, y)

	// Check if they are invalid public key coordinates
	if publicKey == nil {
		return false
	}

	// Verify the signature with the public key,
	// then return true if it's valid, false otherwise
	return ecdsa.Verify(publicKey, hash, r, s)
}
