/*
   Copyright 2022 The Erigon contributors

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

package crypto

import (
	"github.com/holiman/uint256"

	"github.com/ledgerwatch/erigon-lib/common/hexutility"
)

var (
	secp256k1N     = new(uint256.Int).SetBytes(hexutility.MustDecodeHex("fffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141"))
	secp256k1halfN = new(uint256.Int).Rsh(secp256k1N, 1)
)

// See Appendix F "Signing Transactions" of the Yellow Paper
func TransactionSignatureIsValid(v byte, r, s *uint256.Int, allowPreEip2s bool) bool {
	if r.IsZero() || s.IsZero() {
		return false
	}

	// See EIP-2: Homestead Hard-fork Changes
	if !allowPreEip2s && s.Gt(secp256k1halfN) {
		return false
	}

	return r.Lt(secp256k1N) && s.Lt(secp256k1N) && (v == 0 || v == 1)
}
