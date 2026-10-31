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
	"bytes"
	"context"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/execution/blockreplay"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/execfinality"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/node/ethconfig"
)

// Genesis occupies txNums 0 and 1; block 1 then spans 2 (block start), 3 (tx0),
// 4 (tx1), 5 (tx2) and 6 (block end). In the access list, index 0 is the block
// start and transactions start at 1, so access index i lives at txNum 2+i.
const (
	resumeBALGenesisTxNum    = uint64(1)
	resumeBALBlockStartTxNum = uint64(2)
	// resumeBALStepSize makes the first step, txNums [0, 4), end right after tx0.
	resumeBALStepSize = uint64(4)
)

// resumePoint is how far into block 1 the state is committed when execution
// starts: the number of access indices already applied, or -1 for a run from
// the block's start.
type resumePoint int

const (
	fromBlockStart resumePoint = -1
	afterBlockInit resumePoint = 0
	afterTx0       resumePoint = 1
	beforeBlockEnd resumePoint = 3
)

// resumeBALFixture is a three-transaction Amsterdam block over a tiny genesis.
// The block-start system call writes storage, tx0 stores the target's balance,
// tx1 tops the target up and tx2 stores the balance again, so every resumed
// prefix writes storage and the suffix changes a slot the prefix wrote.
type resumeBALFixture struct {
	config  *chain.Config
	engine  rules.Engine
	alloc   types.GenesisAlloc
	genesis *types.Block
	block   *types.Block
	senders []common.Address
	// wipeHistory drops the account, storage and code history after seeding.
	wipeHistory bool
	// fromStateFiles moves the seeded state into the files of the first step and
	// empties the state tables, as a snapshot sync leaves them.
	fromStateFiles bool
}

func newResumeBALFixture(t *testing.T) *resumeBALFixture {
	t.Helper()
	config := chain.TestChainOsakaConfig.Copy()
	config.AmsterdamTime = common.NewUint64(10)

	sender := senderIsCoinbaseKey.rawAddress
	target := common.HexToAddress("0x0000000000000000000000000000000000000a11")
	observer := common.HexToAddress("0x0000000000000000000000000000000000000b22")
	observerCode := append([]byte{byte(vm.PUSH20)}, target[:]...)
	observerCode = append(observerCode, byte(vm.BALANCE), byte(vm.PUSH1), 0, byte(vm.SSTORE), byte(vm.STOP))
	// Stands in for the EIP-4788 contract: the block-start system call stores 1 in slot 0.
	beaconRootsCode := []byte{byte(vm.PUSH1), 1, byte(vm.PUSH1), 0, byte(vm.SSTORE), byte(vm.STOP)}
	alloc := types.GenesisAlloc{
		sender:                            {Balance: big.NewInt(1_000_000_000_000_000_000)},
		target:                            {Balance: big.NewInt(7)},
		observer:                          {Balance: new(big.Int), Nonce: 1, Code: observerCode},
		params.BeaconRootsAddress.Value(): {Balance: new(big.Int), Nonce: 1, Code: beaconRootsCode},
	}
	for _, contract := range []accounts.Address{
		config.GetWithdrawalRequestContract(), config.GetConsolidationRequestContract(),
		config.GetBuilderDepositContract(), config.GetBuilderExitContract(),
	} {
		alloc[contract.Value()] = types.GenesisAccount{Balance: new(big.Int), Nonce: 1, Code: []byte{byte(vm.STOP)}}
	}
	genesis, genesisState, err := genesiswrite.GenesisToBlock(&types.Genesis{Config: config, GasLimit: 30_000_000, Alloc: alloc}, datadir.New(t.TempDir()), log.New())
	require.NoError(t, err)
	genesisState.Close()

	engine := merge.New(ethash.NewFaker())
	t.Cleanup(func() { engine.Close() })
	fx := &resumeBALFixture{config: config, engine: engine, alloc: alloc, genesis: genesis, senders: []common.Address{sender, sender, sender}}

	signer := types.LatestSignerForChainID(config.ChainID)
	gasPrice := uint256.NewInt(1_000_000_000)
	var txs types.Transactions
	for _, txn := range []*types.LegacyTx{
		{CommonTx: types.CommonTx{Nonce: 0, To: &observer, GasLimit: 1_000_000}, GasPrice: *gasPrice},
		{CommonTx: types.CommonTx{Nonce: 1, To: &target, Value: *uint256.NewInt(1), GasLimit: 21_000}, GasPrice: *gasPrice},
		{CommonTx: types.CommonTx{Nonce: 2, To: &observer, GasLimit: 1_000_000}, GasPrice: *gasPrice},
	} {
		signed, err := types.SignTx(txn, *signer, senderIsCoinbaseKey.key)
		require.NoError(t, err)
		txs = append(txs, signed)
	}
	fx.block = fx.assembleBlock(t, txs, common.HexToAddress("0x000000000000000000000000000000000000c01b"))
	require.True(t, hasStorageChangeAt(fx.block.BlockAccessList(), 0), "the block start must write storage")
	require.True(t, hasStorageChangeAt(fx.block.BlockAccessList(), 1), "tx0 must write storage")
	return fx
}

func hasStorageChangeAt(bal types.BlockAccessList, index uint32) bool {
	for i := range bal {
		for _, slot := range bal[i].StorageChanges {
			for _, change := range slot.Changes {
				if change.Index == index {
					return true
				}
			}
		}
	}
	return false
}

func allocAccount(ga types.GenesisAccount) accounts.Account {
	acc := accounts.NewAccount()
	acc.Nonce = ga.Nonce
	acc.Balance = *uint256.MustFromBig(ga.Balance)
	if len(ga.Code) > 0 {
		acc.Incarnation = state.FirstContractIncarnation
		acc.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(ga.Code))
	}
	return acc
}

// seed writes the genesis allocation as block 0 and, for a resumed run, the
// state the prefix left behind, each access index at its own txNum, and
// checkpoints the commitment where execution resumes.
func (fx *resumeBALFixture) seed(t *testing.T, db kv.TemporalRwDB, point resumePoint) {
	t.Helper()
	ctx := context.Background()
	err := db.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
		domains, err := execctx.NewSharedDomains(ctx, tx, log.New(), execctx.WithParaTrieDB(db))
		if err != nil {
			return err
		}
		defer domains.Close()
		commitBlock, commitTxNum := uint64(0), resumeBALGenesisTxNum
		if point != fromBlockStart {
			commitBlock, commitTxNum = 1, resumeBALBlockStartTxNum+uint64(point)
		}
		domains.SetTxNum(commitTxNum)
		putAccount := func(addr common.Address, acc accounts.Account, code []byte, txNum uint64) error {
			if len(code) > 0 {
				if err := domains.DomainPut(kv.CodeDomain, tx, addr[:], code, txNum, nil); err != nil {
					return err
				}
			}
			return domains.DomainPut(kv.AccountsDomain, tx, addr[:], accounts.SerialiseV3(&acc), txNum, nil)
		}
		for addr, ga := range fx.alloc {
			if err := putAccount(addr, allocAccount(ga), ga.Code, resumeBALGenesisTxNum); err != nil {
				return err
			}
		}
		if err := rawdbv3.TxNums.Append(tx, 0, resumeBALGenesisTxNum); err != nil {
			return err
		}
		if point != fromBlockStart {
			if err := rawdbv3.TxNums.Append(tx, 1, resumeBALBlockStartTxNum+uint64(len(fx.block.Transactions()))+1); err != nil {
				return err
			}
			bal := fx.block.BlockAccessList()
			for i := range bal {
				changes := &bal[i]
				acc := accounts.NewAccount()
				if ga, ok := fx.alloc[changes.Address]; ok {
					acc = allocAccount(ga)
				}
				for index := uint32(0); index <= uint32(point); index++ {
					txNum := resumeBALBlockStartTxNum + uint64(index)
					changed := false
					for _, bc := range changes.BalanceChanges {
						if bc.Index == index {
							acc.Balance = bc.Value
							changed = true
						}
					}
					for _, nc := range changes.NonceChanges {
						if nc.Index == index {
							acc.Nonce = nc.Value
							changed = true
						}
					}
					for _, cc := range changes.CodeChanges {
						require.NotEqual(t, index, cc.Index, "the prefix must not change code")
					}
					if changed {
						if err := putAccount(changes.Address, acc, nil, txNum); err != nil {
							return err
						}
					}
					for _, slot := range changes.StorageChanges {
						for _, change := range slot.Changes {
							if change.Index != index {
								continue
							}
							if err := domains.DomainPut(kv.StorageDomain, tx, dbutils.GenerateStoragePlainKey(changes.Address, slot.Slot.Value()), change.Value.Bytes(), txNum, nil); err != nil {
								return err
							}
						}
					}
				}
			}
		}
		root, err := domains.ComputeCommitment(ctx, tx, true, commitBlock, commitTxNum, "seed", nil)
		if err != nil {
			return err
		}
		if point == fromBlockStart {
			require.Equal(t, fx.genesis.Root(), common.BytesToHash(root))
		}
		return domains.Flush(ctx, tx)
	})
	require.NoError(t, err)
}

// assembleBlock builds block 1 with the production block assembler, so the
// header carries the real state root, receipts and block access list hash.
func (fx *resumeBALFixture) assembleBlock(t *testing.T, txs types.Transactions, coinbase common.Address) *types.Block {
	t.Helper()
	ctx := context.Background()
	logger := log.New()
	db := newResumeTestDB(t)
	fx.seed(t, db, fromBlockStart)
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		return rawdb.WriteTd(tx, fx.genesis.Hash(), 0, *uint256.NewInt(0))
	}))

	parent := fx.genesis.Header()
	header := types.NewEmptyHeaderForAssembling()
	header.ParentHash = fx.genesis.Hash()
	header.Coinbase = coinbase
	header.Root = parent.Root
	header.Number = *uint256.NewInt(1)
	header.GasLimit = parent.GasLimit
	header.Time = *fx.config.AmsterdamTime
	header.BaseFee = misc.CalcBaseFee(fx.config, parent)
	excessBlobGas := misc.CalcExcessBlobGas(fx.config, parent, header.Time)
	header.ExcessBlobGas = &excessBlobGas
	header.BlobGasUsed = new(uint64)
	header.ParentBeaconBlockRoot = &common.Hash{}
	slot := header.Number.Uint64()
	header.SlotNumber = &slot

	tx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	domains, err := execctx.NewSharedDomains(ctx, tx, logger, execctx.WithParaTrieDB(db))
	require.NoError(t, err)
	defer domains.Close()
	txNum, _, err := domains.SeekCommitment(ctx, tx)
	require.NoError(t, err)
	domains.SetTxNum(txNum)
	stateReader := state.NewReaderV3(domains.AsStateGetter(tx, execctxapi.StateGetterOptions{}))
	ibs := state.New(stateReader)
	defer ibs.Close()
	ibs.SetVersionMap(state.NewVersionMap(nil))
	ibs.SetTxContext(header.Number.Uint64(), -1)

	ba := exec.NewBlockAssembler(exec.AssemblerCfg{ChainConfig: fx.config, Engine: fx.engine}, &exec.AssembledBlock{
		Header:      header,
		Uncles:      []*types.Header{},
		Txns:        types.Transactions{},
		Receipts:    types.Receipts{},
		Withdrawals: []*types.Withdrawal{},
	})
	require.True(t, ba.HasBAL())
	require.NoError(t, ba.Initialize(ibs, tx, logger))
	_, _, err = ba.AddTransactions(ctx, nil, txs, accounts.InternAddress(coinbase), &vm.Config{}, ibs, nil, "resume-bal", logger)
	require.NoError(t, err)
	require.Len(t, ba.Txns, len(txs))
	assembled, err := ba.AssembleBlock(ibs, tx, logger)
	require.NoError(t, err)

	blockContext := protocol.NewEVMBlockContext(header, protocol.GetHashFn(header, nil), fx.engine, accounts.NilAddress, fx.config)
	blockRules := blockContext.Rules(fx.config)
	for _, ws := range ba.BalIO().Outputs() {
		if ws == nil || ws.IsEmpty() {
			continue
		}
		require.NoError(t, state.ApplyWrites(ws, domains, tx, 1, txNum, nil, blockRules, false))
	}
	root, err := domains.ComputeCommitment(ctx, tx, false, 1, txNum, "resume-bal", nil)
	require.NoError(t, err)

	finalHeader := types.CopyHeader(assembled.HeaderNoCopy())
	finalHeader.Root = common.BytesToHash(root)
	block := types.NewBlockForAsembling(finalHeader, ba.Txns, ba.Uncles, ba.Receipts, ba.Withdrawals, types.NewBlockAccessListSidecar(ba.BlockAccessList))
	block.SendersToTxs(fx.senders)
	require.True(t, fx.config.IsAmsterdam(block.Time()))
	require.NotEmpty(t, block.BlockAccessList())
	return block
}

// withAccessList commits block 1 to bal: transactions and state root stay
// unchanged, so only the access list check can tell the block apart.
func (fx *resumeBALFixture) withAccessList(t *testing.T, bal types.BlockAccessList) *types.Block {
	t.Helper()
	require.NotEqual(t, fx.block.BlockAccessList().Hash(), bal.Hash())
	header := types.CopyHeader(fx.block.HeaderNoCopy())
	balHash := bal.Hash()
	header.BlockAccessListHash = &balHash
	return fx.block.WithSeal(header).WithBlockAccessListSidecar(types.NewBlockAccessListSidecar(bal))
}

// phantomAccountBlock lists an extra untouched account.
func (fx *resumeBALFixture) phantomAccountBlock(t *testing.T) *types.Block {
	t.Helper()
	bal := append(fx.block.BlockAccessList().Copy(), types.AccountChanges{Address: common.HexToAddress("0x0000000000000000000000000000000000000c33")})
	slices.SortFunc(bal, func(a, b types.AccountChanges) int { return bytes.Compare(a.Address[:], b.Address[:]) })
	return fx.withAccessList(t, bal)
}

// droppedInitWritesBlock presents the block-start system call's storage writes
// as mere reads, which is what a replay without history would compute.
func (fx *resumeBALFixture) droppedInitWritesBlock(t *testing.T) *types.Block {
	t.Helper()
	bal := fx.block.BlockAccessList().Copy()
	dropped := false
	for i := range bal {
		if bal[i].Address != params.BeaconRootsAddress.Value() {
			continue
		}
		for _, slot := range bal[i].StorageChanges {
			bal[i].StorageReads = append(bal[i].StorageReads, slot.Slot.Value())
		}
		bal[i].StorageChanges = nil
		dropped = true
	}
	require.True(t, dropped)
	require.NoError(t, bal.Validate())
	return fx.withAccessList(t, bal)
}

// moveToStateFiles builds the files of the first step and empties the state
// tables. With wipeHistory it removes the account, storage and code history
// files instead.
func (fx *resumeBALFixture) moveToStateFiles(t *testing.T, db kv.TemporalRwDB, dirs datadir.Dirs) {
	t.Helper()
	ctx := context.Background()
	agg := db.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
	finality := execfinality.NewContext(^uint64(0), ^uint64(0), 0, false, rawdbv3.TxNums)
	require.NoError(t, agg.BuildFiles2(ctx, db, 0, 1, finality, false))
	agg.WaitForFiles()
	if fx.wipeHistory {
		for _, snapDir := range []string{dirs.SnapHistory, dirs.SnapIdx, dirs.SnapAccessors} {
			entries, err := os.ReadDir(snapDir)
			require.NoError(t, err)
			for _, e := range entries {
				name := e.Name()
				stateDomain := strings.Contains(name, "-accounts.") || strings.Contains(name, "-storage.") || strings.Contains(name, "-code.")
				historyFile := slices.ContainsFunc([]string{".v", ".vi", ".vei", ".ef", ".efi", ".efei"}, func(ext string) bool { return strings.HasSuffix(name, ext) })
				if stateDomain && historyFile {
					require.NoError(t, dir.RemoveFile(filepath.Join(snapDir, name)))
				}
			}
		}
		require.NoError(t, agg.ReloadFiles())
	}
	require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
		for _, table := range agg.DomainTables(kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain, kv.CommitmentDomain) {
			if err := tx.ClearTable(table); err != nil {
				return err
			}
		}
		return nil
	}))
	tx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	require.Equal(t, resumeBALStepSize, tx.Debug().TxNumsInFiles(kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain))
}

// exec runs block through the parallel executor from point and returns the
// invalid-block verdict and the operational error, at most one of them set.
func (fx *resumeBALFixture) exec(t *testing.T, block *types.Block, point resumePoint) (*blockVerdict, error) {
	t.Helper()
	ctx := context.Background()
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	var db kv.TemporalRwDB
	if fx.fromStateFiles {
		db = temporaltest.NewTestDB(t, dirs, temporaltest.WithStepSize(resumeBALStepSize))
	} else {
		db = newResumeTestDB(t)
	}
	fx.seed(t, db, point)
	if fx.fromStateFiles {
		fx.moveToStateFiles(t, db, dirs)
	} else if fx.wipeHistory {
		require.NoError(t, db.Update(ctx, func(tx kv.RwTx) error {
			for _, table := range []string{
				kv.TblAccountHistoryKeys, kv.TblAccountHistoryVals, kv.TblAccountIdx,
				kv.TblStorageHistoryKeys, kv.TblStorageHistoryVals, kv.TblStorageIdx,
				kv.TblCodeHistoryKeys, kv.TblCodeHistoryVals, kv.TblCodeIdx,
			} {
				if err := tx.ClearTable(table); err != nil {
					return err
				}
			}
			return nil
		}))
	}

	blockRLP, err := rlp.EncodeToBytes(block)
	require.NoError(t, err)
	parentRLP, err := rlp.EncodeToBytes(fx.genesis.Header())
	require.NoError(t, err)
	senders := make([][20]byte, len(fx.senders))
	for i, sender := range fx.senders {
		senders[i] = sender
	}
	blockReader, err := blockreplay.NewMemBlockReader(&blockreplay.Fixture{
		BlockRLP: blockRLP, ParentHeaderRLP: parentRLP, Senders: senders,
	})
	require.NoError(t, err)

	syncCfg := ethconfig.Defaults.Sync
	syncCfg.ExecWorkerCount = 2
	cfg := StageExecuteBlocksCfg(db, prune.DefaultMode, 512*datasize.MB, fx.config, fx.engine, &vm.Config{}, nil, false, false, dirs, blockReader, nil, syncCfg, false, nil)

	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	t.Cleanup(rwTx.Rollback)
	domains, err := execctx.NewSharedDomains(ctx, rwTx, logger, execctx.WithParaTrieDB(db))
	require.NoError(t, err)
	t.Cleanup(domains.Close)
	if fx.fromStateFiles {
		txNum, blockNum, err := domains.SeekCommitment(ctx, rwTx)
		require.NoError(t, err)
		require.Equal(t, resumeBALStepSize-1, txNum, "the commitment in the files must sit at the step edge")
		require.Equal(t, uint64(1), blockNum)
	}
	domains.SetTxNum(resumeBALBlockStartTxNum)

	rng := execRange{blockNum: 1, initialTxNum: resumeBALGenesisTxNum, inputTxNum: resumeBALBlockStartTxNum, maxBlockNum: 1}
	if point != fromBlockStart {
		rng.initialTxNum = resumeBALBlockStartTxNum + uint64(point)
	}
	src := &singleBlockSource{block: block, num: 1, parent: fx.genesis.Header(), bal: block.BlockAccessList()}
	out, err := ExecV3(ctx, cfg, domains, rwTx, stages.ModeForkValidation, false, "resume-bal", rng, src, logger)
	if err != nil {
		require.Nil(t, out.verdict)
		return nil, err
	}
	return out.verdict, nil
}

// TestResumedBlockValidatesBlockAccessList pins that a block resumed after a
// mid-block commit still has its EIP-7928 block access list checked against
// the header, exactly like a block executed from its start.
func TestResumedBlockValidatesBlockAccessList(t *testing.T) {
	fx := newResumeBALFixture(t)
	phantom := fx.phantomAccountBlock(t)
	droppedInit := fx.droppedInitWritesBlock(t)
	for _, tc := range []struct {
		name         string
		block        *types.Block
		point        resumePoint
		wantMismatch bool
	}{
		{"full/valid", fx.block, fromBlockStart, false},
		{"full/phantom account", phantom, fromBlockStart, true},
		{"full/dropped block-start writes", droppedInit, fromBlockStart, true},
		{"after tx0/valid", fx.block, afterTx0, false},
		{"after tx0/phantom account", phantom, afterTx0, true},
		{"after block start/valid", fx.block, afterBlockInit, false},
		{"after block start/dropped block-start writes", droppedInit, afterBlockInit, true},
		{"before block end/valid", fx.block, beforeBlockEnd, false},
		{"before block end/phantom account", phantom, beforeBlockEnd, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			verdict, err := fx.exec(t, tc.block, tc.point)
			require.NoError(t, err)
			if !tc.wantMismatch {
				if verdict != nil {
					t.Fatal(verdict.err)
				}
				return
			}
			require.NotNil(t, verdict)
			require.ErrorContains(t, verdict.err, "block access list mismatch")
		})
	}
}

// TestResumedBlockFromStateFilesValidatesBlockAccessList pins the resume after a
// snapshot sync: the state files end inside block 1, right after tx0, and the
// state tables are empty, so the prefix's history comes from the history files.
func TestResumedBlockFromStateFilesValidatesBlockAccessList(t *testing.T) {
	fx := newResumeBALFixture(t)
	fx.fromStateFiles = true
	phantom := fx.phantomAccountBlock(t)
	droppedInit := fx.droppedInitWritesBlock(t)
	for _, tc := range []struct {
		name         string
		block        *types.Block
		wantMismatch bool
	}{
		{"valid", fx.block, false},
		{"phantom account", phantom, true},
		{"dropped block-start writes", droppedInit, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			verdict, err := fx.exec(t, tc.block, afterTx0)
			require.NoError(t, err)
			if !tc.wantMismatch {
				if verdict != nil {
					t.Fatal(verdict.err)
				}
				return
			}
			require.NotNil(t, verdict)
			require.ErrorContains(t, verdict.err, "block access list mismatch")
		})
	}
}

// TestResumedBlockWithoutPrefixHistoryIsOperational pins that a resumed block
// whose prefix is missing from the state history is neither accepted nor marked
// invalid: a history read silently answers from the latest state, which would
// turn the prefix's writes into no-ops.
func TestResumedBlockWithoutPrefixHistoryIsOperational(t *testing.T) {
	fx := newResumeBALFixture(t)
	fx.wipeHistory = true
	droppedInit := fx.droppedInitWritesBlock(t)

	for _, point := range []resumePoint{afterBlockInit, afterTx0, beforeBlockEnd} {
		verdict, err := fx.exec(t, fx.block, point)
		require.ErrorIs(t, err, state.ErrPruned, "the real block must not be judged without history")
		require.Nil(t, verdict)
		verdict, err = fx.exec(t, droppedInit, point)
		require.ErrorIs(t, err, state.ErrPruned, "the forged block must not be judged without history")
		require.Nil(t, verdict)
	}

	files := *fx
	files.fromStateFiles = true
	for _, block := range []*types.Block{fx.block, droppedInit} {
		verdict, err := files.exec(t, block, afterTx0)
		require.ErrorIs(t, err, state.ErrPruned, "a block must not be judged without the history files")
		require.Nil(t, verdict)
	}
}

// TestResumedBlockWithWrongRequestsHashIsInvalid pins that a block failing
// finalization is rejected when resumed mid-block, as when executed from its
// start, although its access list matches.
func TestResumedBlockWithWrongRequestsHashIsInvalid(t *testing.T) {
	fx := newResumeBALFixture(t)
	header := types.CopyHeader(fx.block.HeaderNoCopy())
	header.RequestsHash = &common.Hash{1}
	block := fx.block.WithSeal(header)

	for _, point := range []resumePoint{fromBlockStart, afterBlockInit, afterTx0, beforeBlockEnd} {
		verdict, err := fx.exec(t, block, point)
		require.NoError(t, err, "resume point %d", point)
		require.NotNil(t, verdict, "resume point %d", point)
		require.ErrorIs(t, verdict.err, rules.ErrInvalidBlock, "resume point %d", point)
	}
}
