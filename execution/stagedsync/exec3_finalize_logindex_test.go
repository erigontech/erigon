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
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

// logIndexContract is the address the block-end system call emits its log from.
var logIndexContract = common.HexToAddress("0x00000000000000000000000000000000000c0de0")

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
	code := []byte{byte(vm.PUSH1), 0x42, byte(vm.PUSH1), 0, byte(vm.PUSH1), 0, byte(vm.LOG1), byte(vm.STOP)}
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

// A block-end system call emits its logs outside any transaction receipt, at the
// block's final txNum. The serial executor must index them: the log index files it
// builds are queried via IndexRange. A chain whose consensus emits logs at block end
// (e.g. Gnosis) has one in nearly every block.
func TestSerialBlockEndLogsReachLogIndex(t *testing.T) {
	engine := &logEmittingSyscallEngine{
		Engine:   ethash.NewFaker(),
		contract: accounts.InternAddress(logIndexContract),
		calls:    1,
	}
	se, task := newSerialFinalizeTestExec(t, engine)
	task.Header.ReceiptHash = empty.RootHash
	rwTx := se.applyTx.(kv.TemporalRwTx)
	require.NoError(t, putLogEmittingContract(se.doms.AsPutDel(rwTx), logIndexContract))

	_, err := se.executeBlock(t.Context(), []exec.Task{task}, true, false)
	require.NoError(t, err)
	require.NoError(t, se.doms.Flush(t.Context(), rwTx))

	require.Equal(t, []uint64{task.TxNum}, indexedTxNums(t, rwTx, kv.LogAddrIdx, logIndexContract[:]))
	topic := common.Hash{31: 0x42}
	require.Equal(t, []uint64{task.TxNum}, indexedTxNums(t, rwTx, kv.LogTopicIdx, topic[:]))
}
