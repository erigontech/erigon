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
	"container/heap"
	"math/rand"
	"testing"
)

func BenchmarkName(b *testing.B) {
	txs := make([]*metaTx, 10_000)
	p := NewSubPool(BaseFeeSubPool, 1024)
	for i := 0; i < len(txs); i++ {
		txs[i] = &metaTx{Tx: &TxSlot{}}
	}
	for i := 0; i < len(txs); i++ {
		p.UnsafeAdd(txs[i])
	}
	p.EnforceInvariants()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txs[0].timestamp = 1
		heap.Fix(p.best, txs[0].bestIndex)
		heap.Fix(p.worst, txs[0].worstIndex)
	}
}

func BenchmarkName2(b *testing.B) {

	var (
		a = rand.Uint64()
		c = rand.Uint64()
		d = rand.Uint64()
	)
	b.ResetTimer()
	var min1 uint64
	var min2 uint64
	var r uint64

	for i := 0; i < b.N; i++ {
		min1 = min(min1, a)
		min2 = min(min2, c)
		if d <= min1 {
			r = min(min1-d, min2)
		} else {
			r = 0
		}
		//
		//// 4. Dynamic fee requirement. Set to 1 if feeCap of the transaction is no less than
		//// baseFee of the currently pending block. Set to 0 otherwise.
		//mt.subPool &^= EnoughFeeCapBlock
		//if mt.Tx.feeCap >= pendingBaseFee {
		//	mt.subPool |= EnoughFeeCapBlock
		//}
	}
	_ = r
}
