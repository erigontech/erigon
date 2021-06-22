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

package txpool

import (
	"encoding/hex"
	"fmt"
	"testing"
)

var txParseTests = []struct {
	payloadStr string
}{
	{"f86a808459682f0082520894fe3b557e8fb62b89f4916b721be55ceb828dbd73872386f26fc10000801ca0d22fc3eed9b9b9dbef9eec230aa3fb849eff60356c6b34e86155dca5c03554c7a05e3903d7375337f103cb9583d97a59dcca7472908c31614ae240c6a8311b02d6"},
	{"b86d02f86a7b80843b9aca00843b9aca0082520894e80d2a018c813577f33f9e69387dc621206fb3a48080c001a02c73a04cd144e5a84ceb6da942f83763c2682896b51f7922e2e2f9a524dd90b7a0235adda5f87a1d098e2739e40e83129ff82837c9042e6ad61d0481334dcb6f1a"},
	{"b86e01f86b7b018203e882520894236ff1e97419ae93ad80cafbaa21220c5d78fb7d880de0b6b3a764000080c080a0987e3d8d0dcd86107b041e1dca2e0583118ff466ad71ad36a8465dd2a166ca2da02361c5018e63beea520321b290097cd749febc2f437c7cb41fdd085816742060"},
}

func TestParseTransactionRLP(t *testing.T) {
	for i, tt := range txParseTests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			var payload []byte
			var err error
			var tx *TxSlot
			if payload, err = hex.DecodeString(tt.payloadStr); err != nil {
				t.Fatal(err)
			}
			if tx, err = ParseTransaction(payload); err != nil {
				t.Fatal(err)
			}
			fmt.Printf("tx nonce: %d\n", tx.nonce)
		})
	}
}
