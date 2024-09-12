// Copyright 2022 The Erigon Authors
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

package crypto

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common/hexutility"
)

var (
	secp256k1N     = new(uint256.Int).SetBytes(hexutility.MustDecodeHex("fffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141"))
	Secp256k1halfN = new(uint256.Int).Rsh(secp256k1N, 1)
)

// See Appendix F "Signing Transactions" of the Yellow Paper
func TransactionSignatureIsValid(v byte, r, s *uint256.Int, allowPreEip2s bool) bool {
	if r.IsZero() || s.IsZero() {
		return false
	}

	// See EIP-2: Homestead Hard-fork Changes
	if !allowPreEip2s && s.Gt(Secp256k1halfN) {
		return false
	}

	return r.Lt(secp256k1N) && s.Lt(secp256k1N) && (v == 0 || v == 1)
}
