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

package executiontests

import (
	"context"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

// storeUnexecutedBlocks executes the first block of chainPack and stores the
// others without executing them.
func storeUnexecutedBlocks(t *testing.T, m *execmoduletester.ExecModuleTester, chainPack *blockgen.ChainPack) {
	t.Helper()
	require.NoError(t, m.InsertChain(chainPack.Slice(0, 1)))
	require.NoError(t, m.DB.UpdateTemporal(context.Background(), func(tx kv.TemporalRwTx) error {
		_, lastTxNum, err := m.BlockReader.TxnumReader().Last(tx)
		if err != nil {
			return err
		}
		signer := types.LatestSignerForChainID(m.ChainConfig.ChainID)
		for _, b := range chainPack.Blocks[1:] {
			if err := rawdb.WriteHeader(tx, b.Header()); err != nil {
				return err
			}
			if err := rawdb.WriteBody(tx, b.Hash(), b.NumberU64(), b.Body()); err != nil {
				return err
			}
			if err := rawdb.WriteCanonicalHash(tx, b.Hash(), b.NumberU64()); err != nil {
				return err
			}
			senders := make([]common.Address, 0, len(b.Transactions()))
			for _, txn := range b.Transactions() {
				sender, err := txn.Sender(*signer)
				if err != nil {
					return err
				}
				senders = append(senders, sender.Value())
			}
			if err := rawdb.WriteSenders(tx, b.Hash(), b.NumberU64(), senders); err != nil {
				return err
			}
			lastTxNum += uint64(len(b.Transactions())) + 2
			if err := rawdbv3.TxNums.Append(tx, b.NumberU64(), lastTxNum); err != nil {
				return err
			}
		}
		return nil
	}))
}

// corruptAccountRecord stores an account record that cannot be decoded.
func corruptAccountRecord(t *testing.T, m *execmoduletester.ExecModuleTester, addr common.Address) {
	t.Helper()
	require.NoError(t, m.DB.UpdateTemporal(context.Background(), func(tx kv.TemporalRwTx) error {
		doms, err := execctx.NewSharedDomains(context.Background(), tx, log.New())
		if err != nil {
			return err
		}
		defer doms.Close()
		if err := doms.AsPutDel(tx).DomainPut(kv.AccountsDomain, addr[:], []byte{0, 0, 0}, 0, nil); err != nil {
			return err
		}
		return doms.Flush(context.Background(), tx)
	}))
}

// executeStoredBlocks runs the execution stage up to block toBlock.
func executeStoredBlocks(t *testing.T, m *execmoduletester.ExecModuleTester, engine rules.Engine, parallel bool, toBlock uint64) error {
	t.Helper()
	prevParallel := dbg.Exec3Parallel
	defer func() { dbg.Exec3Parallel = prevParallel }()
	dbg.Exec3Parallel = parallel

	ctx := context.Background()
	tx, err := m.DB.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	syncCfg := m.Cfg().Sync
	syncCfg.ChaosMonkey = false
	cfg := stagedsync.StageExecuteBlocksCfg(m.DB, m.Cfg().Prune, m.Cfg().BatchSize, m.ChainConfig, engine, &vm.Config{},
		m.Notifications, m.Cfg().StateStream, false, m.Dirs, m.BlockReader, m.Cfg().Genesis, syncCfg, false, exec.NewBlockReadAheader())
	require.NoError(t, stages.SaveStageProgress(tx, stages.Senders, toBlock))
	doms, err := execctx.NewSharedDomains(ctx, tx, m.Log)
	require.NoError(t, err)
	defer doms.Close()
	s := &stagedsync.StageState{State: m.Sync, ID: stages.Execution, BlockNumber: 1, CurrentSyncCycle: stagedsync.CurrentSyncCycleInfo{IsInitialCycle: true}}
	return stagedsync.SpawnExecuteBlocksStage(s, nil, doms, tx, toBlock, ctx, cfg, m.Log)
}

// transferInBlock2 builds two blocks; block 2 carries one value transfer to to.
func transferInBlock2(t *testing.T, m *execmoduletester.ExecModuleTester, to common.Address) *blockgen.ChainPack {
	t.Helper()
	signer := types.LatestSignerForChainID(m.ChainConfig.ChainID)
	chainPack, err := m.GenerateChain(2, func(i int, b *blockgen.BlockGen) {
		if i == 1 {
			txn, err := types.SignTx(types.NewTransaction(b.TxNonce(m.Address), to, uint256.NewInt(1), 21_000, uint256.NewInt(1), nil), *signer, m.Key)
			require.NoError(t, err)
			b.AddTx(txn)
		}
	})
	require.NoError(t, err)
	return chainPack
}

// creditingEngine credits beneficiary when it finalizes block 2.
type creditingEngine struct {
	rules.Engine
	beneficiary common.Address
}

func (e creditingEngine) Finalize(config *chain.Config, header *types.Header, ibs *state.IntraBlockState,
	uncles []*types.Header, receipts types.Receipts, withdrawals []*types.Withdrawal,
	chainReader rules.ChainReader, syscall rules.SystemCall, skipReceiptsEval bool, logger log.Logger,
) (types.FlatRequests, error) {
	requests, err := e.Engine.Finalize(config, header, ibs, uncles, receipts, withdrawals, chainReader, syscall, skipReceiptsEval, logger)
	if err != nil || header.Number.Uint64() != 2 {
		return requests, err
	}
	return requests, ibs.AddBalance(accounts.InternAddress(e.beneficiary), *uint256.NewInt(1), tracing.BalanceIncreaseWithdrawal)
}

// A state read that fails says nothing about the block: execution must stop
// with the read error, not reject the block as invalid.
func TestStateReadErrorIsNotInvalidBlock(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		t.Run(map[bool]string{false: "serial", true: "parallel"}[parallel], func(t *testing.T) {
			t.Run("tx", func(t *testing.T) {
				m := execmoduletester.New(t)
				to := common.Address{0xbb, 19: 1}
				storeUnexecutedBlocks(t, m, transferInBlock2(t, m, to))
				corruptAccountRecord(t, m, to)
				err := executeStoredBlocks(t, m, m.Engine, parallel, 2)
				require.ErrorContains(t, err, "deserialse2")
				require.NotErrorIs(t, err, rules.ErrInvalidBlock)
			})
			t.Run("finalize", func(t *testing.T) {
				m := execmoduletester.New(t)
				storeUnexecutedBlocks(t, m, transferInBlock2(t, m, common.Address{0xaa}))
				beneficiary := common.Address{19: 0x42}
				corruptAccountRecord(t, m, beneficiary)
				err := executeStoredBlocks(t, m, creditingEngine{Engine: m.Engine, beneficiary: beneficiary}, parallel, 2)
				require.ErrorContains(t, err, "deserialse2")
				require.NotErrorIs(t, err, rules.ErrInvalidBlock)
			})
		})
	}
}

// The corrupt record is first read through Exist and Empty, by a value CALL
// from a contract, not through a balance read.
func TestStateReadErrorInCallIsNotInvalidBlock(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		t.Run(map[bool]string{false: "serial", true: "parallel"}[parallel], func(t *testing.T) {
			corrupt := common.Address{0xcc, 19: 1}
			payer := common.HexToAddress("0xc0de")
			alloc := types.GenesisAlloc{payer: {Code: callCode(corrupt, 1), Balance: big.NewInt(1)}}
			keys := revivalKeys(t, alloc, 1)
			gspec := &types.Genesis{Config: chain.TestChainOsakaConfig, GasLimit: 30_000_000, Alloc: alloc}
			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(keys[0]))
			signer := types.LatestSignerForChainID(gspec.Config.ChainID)
			chainPack, err := m.GenerateChain(2, func(i int, b *blockgen.BlockGen) {
				if i == 1 {
					txn, err := types.SignTx(types.NewTransaction(b.TxNonce(m.Address), payer, uint256.NewInt(0), 200_000, uint256.NewInt(10_000_000_000), nil), *signer, m.Key)
					require.NoError(t, err)
					b.AddTx(txn)
				}
			})
			require.NoError(t, err)
			storeUnexecutedBlocks(t, m, chainPack)
			corruptAccountRecord(t, m, corrupt)
			err = executeStoredBlocks(t, m, m.Engine, parallel, 2)
			require.ErrorContains(t, err, "deserialse2")
			require.NotErrorIs(t, err, rules.ErrInvalidBlock)
		})
	}
}
