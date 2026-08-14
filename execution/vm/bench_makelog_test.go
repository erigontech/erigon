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

package vm

import (
	"testing"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

// benchmarkOpLog isolates the LOG opcode handler (makeLog) emit path: pop
// topics + mem range, copy log data, and hand the log to the IntraBlockState.
// Reset each iteration keeps the log buffer at steady-state capacity so the
// measured allocs are makeLog's own (topics slice + data copy), not buffer growth.
func benchmarkOpLog(b *testing.B, numTopics, dataLen int) {
	ibs := state.New(state.NewNoopReader())
	defer ibs.Release(false)
	evm := NewEVM(evmtypes.BlockContext{BlockNumber: 1}, evmtypes.TxContext{}, ibs, chain.AllProtocolChanges, Config{})
	to := accounts.InternAddress(common.Address{0x01})
	cc := &CallContext{Contract: *NewContract(accounts.ZeroAddress, accounts.ZeroAddress, to, uint256.Int{})}
	cc.Memory.Resize(uint64(dataLen))
	logFn := makeLog(numTopics)
	pc := uint64(0)
	topic := *uint256.NewInt(0x42)
	mStart := uint256.Int{}
	mSize := *uint256.NewInt(uint64(dataLen))

	emit := func() {
		for range numTopics {
			cc.Stack.push(topic)
		}
		cc.Stack.push(mSize)
		cc.Stack.push(mStart)
		if _, _, err := logFn(pc, evm, cc); err != nil {
			b.Fatalf("makeLog(%d): %v", numTopics, err)
		}
		ibs.Reset()
	}

	b.ReportAllocs()
	ibs.SetTxContext(1, 0)
	emit() // grow the log buffer before measuring, so the loop reports steady state
	for b.Loop() {
		emit()
	}
}

func BenchmarkOpLog0(b *testing.B) { benchmarkOpLog(b, 0, 96) }
func BenchmarkOpLog2(b *testing.B) { benchmarkOpLog(b, 2, 96) }
func BenchmarkOpLog3(b *testing.B) { benchmarkOpLog(b, 3, 96) }
func BenchmarkOpLog4(b *testing.B) { benchmarkOpLog(b, 4, 96) }
