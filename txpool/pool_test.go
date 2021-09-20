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
	"testing"
)

func BenchmarkName(b *testing.B) {
	txs := make([]*metaTx, 10_000)
	p := NewSubPool(BaseFeeSubPool)
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
	txs := make([]*metaTx, 10_000)
	p := NewSubPool(BaseFeeSubPool)
	for i := 0; i < len(txs); i++ {
		txs[i] = &metaTx{Tx: &TxSlot{}}
	}
	for i := 0; i < len(txs); i++ {
		p.UnsafeAdd(txs[i])
	}
	p.EnforceInvariants()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
	}
}
