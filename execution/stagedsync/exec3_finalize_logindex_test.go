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

package stagedsync

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/stream"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

const logIndexFinalTxNum = uint64(101)

var (
	logIndexContract = common.HexToAddress("0x00000000000000000000000000000000000c0de0")
	logIndexTopic    = common.Hash{31: 0x42}
)

// An empty block still has a start and an end virtual task. Their txNums must
// differ from each other and from the block number so the index assertions
// detect using the wrong identifier.
func newLogIndexTestTasks(config *chain.Config) []exec.Task {
	header := &types.Header{Number: *uint256.NewInt(7), GasLimit: 10_000_000, ReceiptHash: empty.RootHash}
	tasks := make([]exec.Task, 2)
	for i := range tasks {
		tasks[i] = &exec.TxTask{
			Header:          header,
			TxNum:           logIndexFinalTxNum - 1 + uint64(i),
			TxIndex:         i - 1,
			Config:          config,
			EvmBlockContext: evmtypes.BlockContext{BlockNumber: header.Number.Uint64()},
		}
	}
	return tasks
}

// logEmittingSyscallEngine drives a fixed number of block-end system calls at one
// contract, each of which emits a log.
type logEmittingSyscallEngine struct {
	rules.Engine
	contract accounts.Address
	calls    int
}

func (e *logEmittingSyscallEngine) Finalize(config *chain.Config, header *types.Header, ibs *state.IntraBlockState,
	uncles []*types.Header, receipts types.Receipts, withdrawals []*types.Withdrawal, chain rules.ChainReader,
	syscall rules.SystemCall, skipReceiptsEval bool, logger log.Logger,
) (types.FlatRequests, error) {
	for range e.calls {
		if _, err := syscall(e.contract, nil); err != nil {
			return nil, err
		}
	}
	return nil, nil
}

// putLogEmittingContract writes LOG1 bytecode at addr so every system call to it
// appends exactly one log to the caller's IntraBlockState.
func putLogEmittingContract(putter kv.TemporalPutDel, addr common.Address) error {
	code := []byte{byte(vm.PUSH1), logIndexTopic[31], byte(vm.PUSH1), 0, byte(vm.PUSH1), 0, byte(vm.LOG1), byte(vm.STOP)}
	acc := accounts.NewAccount()
	acc.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(code))
	if err := putter.DomainPut(kv.CodeDomain, addr[:], code, 0, nil); err != nil {
		return err
	}
	return putter.DomainPut(kv.AccountsDomain, addr[:], accounts.SerialiseV3(&acc), 0, nil)
}

func indexedTxNums(t *testing.T, tx kv.TemporalTx, idx kv.InvertedIdx, key []byte) []uint64 {
	t.Helper()
	it, err := tx.IndexRange(idx, key, 0, -1, order.Asc, kv.Unlim)
	require.NoError(t, err)
	txNums, err := stream.ToArrayU64(it)
	require.NoError(t, err)
	return txNums
}

func seedLogEmittingContract(t *testing.T, db kv.TemporalRwDB, addr common.Address) {
	seedResumeTestDB(t, db, func(putter kv.TemporalPutDel) error {
		return putLogEmittingContract(putter, addr)
	})
}
