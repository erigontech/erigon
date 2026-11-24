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
	"context"
	"errors"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

func TestParallelResumedBlockEndLeavesInclusionListUnchecked(t *testing.T) {
	ctx := context.Background()
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	config := chain.TestChainBerlinConfig

	pe, roTx := newResumeTestExec(t, db, config)
	pe.cfg.vmConfig = &vm.Config{}
	pe.cfg.readAheader = exec.NewBlockReadAheader()

	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	signer := *types.LatestSignerForChainID(config.ChainID)
	included, err := types.SignTx(types.NewTransaction(0, common.Address{}, uint256.NewInt(0), params.TxGas, uint256.NewInt(0), nil), signer, key)
	require.NoError(t, err)
	omitted, err := types.SignTx(types.NewTransaction(1, common.Address{}, uint256.NewInt(0), params.TxGas, uint256.NewInt(0), nil), signer, key)
	require.NoError(t, err)

	header := &types.Header{Number: *uint256.NewInt(1), GasLimit: 10_000_000}
	blockHash := header.Hash()
	pe.cfg.readAheader.AddInclusionList(blockHash, types.Transactions{omitted})

	gasPool := new(protocol.GasPool).AddGas(10_000_000)
	be := newBlockExec(1, blockHash, gasPool, nil, newResultStream(), false, nil)
	endTask := &exec.TxTask{Header: header, Txs: types.Transactions{included}, TxNum: 2, TxIndex: 1, Config: config}
	be.tasks = []*execTask{{Task: endTask, index: 0}}
	reader := state.NewReaderV3(pe.domainsRead().AsGetter(roTx))
	require.NoError(t, pe.checkInclusionList(ctx, roTx, be, endTask, reader, 1))

	_, ilHash, ok := pe.cfg.readAheader.ReadInclusionList(blockHash)
	require.True(t, ok)
	_, checked := pe.cfg.readAheader.ReadInclusionListResult(blockHash, ilHash)
	require.False(t, checked)
}

func TestParallelInclusionListCheckResolvesBlockHashWithoutProducerTx(t *testing.T) {
	ctx := context.Background()
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	config := chain.AllProtocolChanges.Copy()
	config.BogotaTime = common.NewUint64(0)

	grandparent := &types.Header{Number: *uint256.NewInt(1)}
	parent := &types.Header{Number: *uint256.NewInt(2), ParentHash: grandparent.Hash()}
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		return rawdb.WriteHeader(tx, parent)
	}))

	pe, roTx := newResumeTestExec(t, db, config)
	pe.cfg.vmConfig = &vm.Config{NoBaseFee: true}
	pe.cfg.blockReader = freezeblocks.NewBlockReader(nil)
	pe.cfg.readAheader = exec.NewBlockReadAheader()

	// Approves only when BLOCKHASH(1) resolves to the grandparent's hash.
	grandparentHash := grandparent.Hash()
	code := append([]byte{0x7f}, grandparentHash[:]...)
	code = append(code,
		0x60, 0x01, 0x40, 0x14, // PUSH1 1, BLOCKHASH, EQ
		0x60, 43, 0x57, // PUSH1 43, JUMPI
		0x5f, 0x5f, 0xfd, // PUSH0, PUSH0, REVERT
		0x5b, 0x60, 0x03, 0x5f, 0x5f, 0xaa, // JUMPDEST, PUSH1 3, PUSH0, PUSH0, APPROVE
	)
	sender := common.HexToAddress("0x1000")
	acc := accounts.Account{Balance: *uint256.NewInt(10_000_000), CodeHash: accounts.InternCodeHash(crypto.Keccak256Hash(code))}
	require.NoError(t, pe.doms.DomainPut(kv.AccountsDomain, roTx, sender[:], accounts.SerialiseV3(&acc), 1, nil))
	require.NoError(t, pe.doms.DomainPut(kv.CodeDomain, roTx, sender[:], code, 1, nil))

	omitted := &types.FrameTransaction{
		ChainID: *config.ChainID, SenderAddress: sender,
		Frames: []types.Frame{{Mode: types.FrameVerify, Flags: types.FrameApproveBoth, Limits: mdgas.MdGas{Execution: 50_000}}},
	}

	header := &types.Header{Number: *uint256.NewInt(3), ParentHash: parent.Hash(), GasLimit: 10_000_000}
	blockHash := header.Hash()
	pe.cfg.readAheader.AddInclusionList(blockHash, types.Transactions{omitted})

	var producerLookups int
	producerGetHash := func(uint64) (common.Hash, error) {
		producerLookups++
		return common.Hash{}, errors.New("producer read tx used outside its goroutine")
	}
	blockContext := protocol.NewEVMBlockContext(header, producerGetHash, pe.cfg.engine, accounts.NilAddress, config)

	gasPool := new(protocol.GasPool).AddGas(10_000_000)
	be := newBlockExec(3, blockHash, gasPool, nil, newResultStream(), false, nil)
	endTask := &exec.TxTask{Header: header, TxNum: 2, TxIndex: 0, Config: config, EvmBlockContext: blockContext}
	be.tasks = []*execTask{{Task: endTask, index: 0}}
	reader := state.NewReaderV3(pe.domainsRead().AsGetter(roTx))
	require.NoError(t, pe.checkInclusionList(ctx, roTx, be, endTask, reader, 0))

	require.Zero(t, producerLookups)
	_, ilHash, ok := pe.cfg.readAheader.ReadInclusionList(blockHash)
	require.True(t, ok)
	satisfied, checked := pe.cfg.readAheader.ReadInclusionListResult(blockHash, ilHash)
	require.True(t, checked)
	require.False(t, satisfied)
}
