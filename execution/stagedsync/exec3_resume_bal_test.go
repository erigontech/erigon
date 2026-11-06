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
	"errors"
	"fmt"
	"maps"
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
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
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
	resumeBlockTime   = uint64(10)
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

// resumeBALFixture is block 1 over a tiny genesis. newResumeBALFixture's is a
// three-transaction Amsterdam block: the block-start system call writes
// storage, tx0 stores the target's balance, tx1 tops the target up and tx2
// stores the balance again, so every resumed prefix writes storage and the
// suffix changes a slot the prefix wrote.
type resumeBALFixture struct {
	config   *chain.Config
	engine   rules.Engine
	alloc    types.GenesisAlloc
	genesis  *types.Block
	block    *types.Block
	receipts types.Receipts
	senders  []common.Address
	// wipeHistory drops the account, storage and code history after seeding.
	wipeHistory bool
	// smallSteps runs exec on steps of resumeBALStepSize, whose first edge is tx0.
	smallSteps bool
	// fromStateFiles moves the seeded state into the files of the first step and
	// empties the state tables, as a snapshot sync leaves them. It implies
	// smallSteps.
	fromStateFiles bool
	// noPrefixReceipts leaves the prefix's receipts out of the seed.
	noPrefixReceipts bool
	// inspect, when set, sees the state each exec and execSerial run leaves.
	inspect func(t *testing.T, sd *execctx.SharedDomains, tx kv.TemporalTx)
}

func newResumeBALFixture(t *testing.T) *resumeBALFixture {
	t.Helper()
	config := chain.TestChainOsakaConfig.Copy()
	config.AmsterdamTime = common.NewUint64(resumeBlockTime)
	return newResumeBlockFixture(t, config)
}

// newResumeBlockFixture returns the fixture of newResumeBALFixture under config.
func newResumeBlockFixture(t *testing.T, config *chain.Config) *resumeBALFixture {
	t.Helper()
	fx := newResumeFixture(t, config, nil)
	target := common.HexToAddress("0x0000000000000000000000000000000000000a11")
	observer := common.HexToAddress("0x0000000000000000000000000000000000000b22")

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
	fx.block, fx.receipts = fx.assembleBlock(t, txs, common.HexToAddress("0x000000000000000000000000000000000000c01b"))
	require.True(t, hasStorageChangeAt(fx.block.BlockAccessList(), 0), "the block start must write storage")
	require.True(t, hasStorageChangeAt(fx.block.BlockAccessList(), 1), "tx0 must write storage")
	return fx
}

// newResumeFixture returns a fixture under config without a block, over a
// genesis of a funded sender, the target and observer, stand-ins for the
// system contracts, and extra.
func newResumeFixture(t *testing.T, config *chain.Config, extra types.GenesisAlloc) *resumeBALFixture {
	t.Helper()
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
	maps.Copy(alloc, extra)
	genesis, genesisState, err := genesiswrite.GenesisToBlock(&types.Genesis{Config: config, GasLimit: 30_000_000, Alloc: alloc}, datadir.New(t.TempDir()), log.New())
	require.NoError(t, err)
	genesisState.Close()

	engine := merge.New(ethash.NewFaker())
	t.Cleanup(func() { engine.Close() })
	return &resumeBALFixture{config: config, engine: engine, alloc: alloc, genesis: genesis, senders: []common.Address{sender, sender, sender}}
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
// state and receipts the prefix left behind, each access index at its own
// txNum, and checkpoints the commitment where execution resumes.
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
			var logIndex uint32
			var blobGas uint64
			for i, receipt := range fx.receipts[:point] {
				if fx.noPrefixReceipts {
					break
				}
				logIndex += uint32(len(receipt.Logs))
				blobGas += fx.block.Transactions()[i].GetBlobGas()
				txNum := resumeBALBlockStartTxNum + uint64(i) + 1
				if err := rawtemporaldb.AppendReceiptMetadata(domains.AsPutDel(tx), logIndex, receipt.CumulativeGasUsed, blobGas, txNum); err != nil {
					return err
				}
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

// assembleBlock builds block 1 and its receipts with the production block
// assembler, so the header carries the real state root, receipts and, from
// Amsterdam, block access list hash. The block carries its access list
// before Amsterdam too, for seed.
func (fx *resumeBALFixture) assembleBlock(t *testing.T, txs types.Transactions, coinbase common.Address) (*types.Block, types.Receipts) {
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
	header.Time = resumeBlockTime
	header.BaseFee = misc.CalcBaseFee(fx.config, parent)
	excessBlobGas := misc.CalcExcessBlobGas(fx.config, parent, header.Time)
	header.ExcessBlobGas = &excessBlobGas
	header.BlobGasUsed = new(uint64)
	header.ParentBeaconBlockRoot = &common.Hash{}
	if fx.config.IsAmsterdam(header.Time) {
		slot := header.Number.Uint64()
		header.SlotNumber = &slot
	}

	tx, err := db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	domains, err := execctx.NewSharedDomains(ctx, tx, logger)
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

	ba := exec.NewBlockAssembler(exec.AssemblerCfg{ChainConfig: fx.config, Engine: fx.engine, ExperimentalBAL: true}, &exec.AssembledBlock{
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
	require.NotEmpty(t, block.BlockAccessList())
	return block, ba.Receipts
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

// wipeHistoryTables empties the account, storage and code history tables.
func wipeHistoryTables(t *testing.T, db kv.TemporalRwDB) {
	t.Helper()
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
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

// exec runs block through the parallel executor from point and returns the
// invalid-block verdict and the operational error, at most one of them set.
func (fx *resumeBALFixture) exec(t *testing.T, block *types.Block, point resumePoint) (*blockVerdict, error) {
	t.Helper()
	ctx := context.Background()
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	var db kv.TemporalRwDB
	if fx.smallSteps || fx.fromStateFiles {
		db = temporaltest.NewTestDB(t, dirs, temporaltest.WithStepSize(resumeBALStepSize))
	} else {
		db = newResumeTestDB(t)
	}
	fx.seed(t, db, point)
	if fx.fromStateFiles {
		fx.moveToStateFiles(t, db, dirs)
	} else if fx.wipeHistory {
		wipeHistoryTables(t, db)
	}

	cfg := fx.execCfg(t, db, dirs, block)
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	t.Cleanup(rwTx.Rollback)
	domains, err := execctx.NewSharedDomains(ctx, rwTx, logger)
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
	src := &singleBlockSource{block: block, num: 1, parent: fx.genesis.Header()}
	if fx.config.IsAmsterdam(block.Time()) {
		src.bal = block.BlockAccessList()
	}
	out, err := ExecV3(ctx, cfg, domains, rwTx, stages.ModeForkValidation, false, "resume-bal", rng, src, logger)
	if err != nil {
		require.Nil(t, out.verdict)
		return nil, err
	}
	if fx.inspect != nil {
		fx.inspect(t, domains, out.applyTx)
	}
	return out.verdict, nil
}

// run executes block from point with the serial or the parallel executor and
// returns the block's rejection, an invalid-block verdict or an operational
// error.
func (fx *resumeBALFixture) run(t *testing.T, block *types.Block, point resumePoint, serial bool) error {
	t.Helper()
	if serial {
		return fx.execSerial(t, block, point)
	}
	verdict, err := fx.exec(t, block, point)
	if verdict != nil {
		return verdict.err
	}
	return err
}

// execCfg returns the stage configuration for executing block over db.
func (fx *resumeBALFixture) execCfg(t *testing.T, db kv.TemporalRwDB, dirs datadir.Dirs, block *types.Block) ExecuteBlockCfg {
	t.Helper()
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
	return StageExecuteBlocksCfg(db, prune.DefaultMode, 512*datasize.MB, fx.config, fx.engine, &vm.Config{}, nil, false, false, dirs, blockReader, nil, syncCfg, false, nil)
}

// execSerial runs block through the serial executor from point and checks the
// state root it leaves against the header's.
func (fx *resumeBALFixture) execSerial(t *testing.T, block *types.Block, point resumePoint) error {
	t.Helper()
	ctx := context.Background()
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	db := newResumeTestDB(t)
	fx.seed(t, db, point)
	if fx.wipeHistory {
		wipeHistoryTables(t, db)
	}
	cfg := fx.execCfg(t, db, dirs, block)
	rwTx, err := db.BeginTemporalRw(ctx)
	require.NoError(t, err)
	t.Cleanup(rwTx.Rollback)
	domains, err := execctx.NewSharedDomains(ctx, rwTx, logger)
	require.NoError(t, err)
	t.Cleanup(domains.Close)
	rs := state.NewStateV3Buffered(state.NewStateV3(domains, false, logger))
	se := &serialExecutor{txExecutor: txExecutor{cfg: cfg, rs: rs, doms: domains, applyTx: rwTx, logger: logger}}
	require.NoError(t, se.resetWorkers(ctx, rs, rwTx))
	t.Cleanup(func() { _ = se.worker.ResetTx(nil) })

	header := block.HeaderNoCopy()
	blockContext := protocol.NewEVMBlockContext(header, protocol.GetHashFn(header, nil), fx.engine, accounts.NilAddress, fx.config)
	var tasks []exec.Task
	txNum := resumeBALBlockStartTxNum
	for txIndex := -1; txIndex <= len(block.Transactions()); txIndex++ {
		if txIndex >= int(point) {
			tasks = append(tasks, &exec.TxTask{
				TxNum: txNum, TxIndex: txIndex, Header: header, Txs: block.Transactions(), Uncles: block.Uncles(),
				Withdrawals: block.Withdrawals(), EvmBlockContext: blockContext, Logger: logger,
			})
		}
		txNum++
	}
	if _, err := se.executeBlock(ctx, tasks, true, false); err != nil {
		return err
	}
	if fx.inspect != nil {
		fx.inspect(t, domains, rwTx)
	}
	root, err := domains.ComputeCommitment(ctx, rwTx, false, 1, txNum-1, "resume-serial", nil)
	require.NoError(t, err)
	if have := common.BytesToHash(root); have != block.Root() {
		return fmt.Errorf("%w: %x, want %x", ErrWrongTrieRoot, have, block.Root())
	}
	return nil
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

// TestResumedBlockChecksStateRoot pins that a block resumed mid-block has its
// state root checked against the header, as when executed from its start, also
// when it resumes at a step edge or crosses one.
func TestResumedBlockChecksStateRoot(t *testing.T) {
	base := newResumeBALFixture(t)
	header := types.CopyHeader(base.block.HeaderNoCopy())
	header.Root[0] ^= 0xff
	wrongRoot := base.block.WithSeal(header)

	for _, tc := range []struct {
		name           string
		point          resumePoint
		smallSteps     bool
		fromStateFiles bool
	}{
		{"full", fromBlockStart, false, false},
		{"after block start", afterBlockInit, false, false},
		{"after tx0", afterTx0, false, false},
		{"before block end", beforeBlockEnd, false, false},
		{"across a step edge", afterBlockInit, true, false},
		{"at a step edge", afterTx0, true, false},
		{"at the state files' step edge", afterTx0, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := *base
			fx.smallSteps, fx.fromStateFiles = tc.smallSteps, tc.fromStateFiles
			verdict, err := fx.exec(t, fx.block, tc.point)
			require.NoError(t, err)
			if verdict != nil {
				t.Fatal(verdict.err)
			}
			verdict, err = fx.exec(t, wrongRoot, tc.point)
			require.NoError(t, err)
			require.NotNil(t, verdict, "a block with a wrong state root must be rejected")
			require.ErrorIs(t, verdict.err, ErrWrongTrieRoot)
		})
	}
}

// gasLeftAt records the gas its block admits at the transaction mark.
type gasLeftAt struct {
	rules.Engine
	mark common.Hash
	seen *uint64
}

func (b gasLeftAt) BindTxGas(txn types.Transaction, gasLeft uint64) (types.Transaction, error) {
	if txn.Hash() == b.mark {
		*b.seen = gasLeft
	}
	return txn, nil
}

// boundAt binds the transaction mark to the gas its block admits less offset.
type boundAt struct {
	rules.Engine
	mark   common.Hash
	offset uint64
}

func (b boundAt) BindTxGas(txn types.Transaction, gasLeft uint64) (types.Transaction, error) {
	if txn.Hash() != b.mark || gasLeft-b.offset == txn.GetGasLimit() {
		return txn, nil
	}
	return &types.LegacyTx{CommonTx: types.CommonTx{GasLimit: gasLeft - b.offset}}, nil
}

// TestResumedBlockTxGas checks that a gas limit bound to its position is
// checked in a block executed from its start or resumed before Amsterdam, and
// left alone in a block resumed from Amsterdam or after transactions without
// receipts, whose gas its pool then lacks: no message takes its gas from the
// block, so the transactions are not executed again.
func TestResumedBlockTxGas(t *testing.T) {
	amsterdam := chain.TestChainOsakaConfig.Copy()
	amsterdam.AmsterdamTime = common.NewUint64(resumeBlockTime)
	for _, fork := range []struct {
		name   string
		config *chain.Config
	}{
		{"osaka", chain.TestChainOsakaConfig},
		{"amsterdam", amsterdam},
	} {
		t.Run(fork.name, func(t *testing.T) {
			testResumedBlockTxGas(t, fork.config)
		})
	}
}

func testResumedBlockTxGas(t *testing.T, config *chain.Config) {
	fx := newResumeBlockFixture(t, config)
	isAmsterdam := config.IsAmsterdam(resumeBlockTime)
	base := fx.engine
	target := common.HexToAddress("0x0000000000000000000000000000000000000a11")
	observer := common.HexToAddress("0x0000000000000000000000000000000000000b22")
	coinbase := common.HexToAddress("0x000000000000000000000000000000000000c01b")
	signer := types.LatestSignerForChainID(fx.config.ChainID)
	gasPrice := uint256.NewInt(1_000_000_000)
	txsWith := func(gas uint64) types.Transactions {
		var txs types.Transactions
		for _, txn := range []*types.LegacyTx{
			{CommonTx: types.CommonTx{Nonce: 0, To: &observer, GasLimit: 1_000_000}, GasPrice: *gasPrice},
			{CommonTx: types.CommonTx{Nonce: 1, To: &target, Value: *uint256.NewInt(1), GasLimit: 21_000}, GasPrice: *gasPrice},
			{CommonTx: types.CommonTx{Nonce: 2, To: &observer, GasLimit: gas}, GasPrice: *gasPrice},
		} {
			signed, err := types.SignTx(txn, *signer, senderIsCoinbaseKey.key)
			require.NoError(t, err)
			txs = append(txs, signed)
		}
		return txs
	}

	// tx2's limit has to be what tx0 and tx1 leave of the block, less offset.
	const offset = 29_000_000
	var left uint64
	txs := txsWith(1_000_000)
	fx.engine = gasLeftAt{base, txs[2].Hash(), &left}
	fx.assembleBlock(t, txs, coinbase)
	require.Greater(t, left, uint64(offset+100_000))
	bound, unbound := txsWith(left-offset), txsWith(left-offset-1)
	fx.engine = boundAt{base, bound[2].Hash(), offset}
	valid, _ := fx.assembleBlock(t, bound, coinbase)
	fx.engine = base
	invalid, _ := fx.assembleBlock(t, unbound, coinbase)

	type testCase struct {
		name             string
		block            *types.Block
		mark             common.Hash
		offset           uint64
		point            resumePoint
		noPrefixReceipts bool
		err              error
	}
	cases := []testCase{
		{"full/bound", valid, bound[2].Hash(), offset, fromBlockStart, false, nil},
		{"full/unbound", invalid, unbound[2].Hash(), offset, fromBlockStart, false, protocol.ErrGasLimitPosition},
		{"full/first unbound", valid, bound[0].Hash(), 0, fromBlockStart, false, protocol.ErrGasLimitPosition},
		{"after block start/unbound", invalid, unbound[2].Hash(), offset, afterBlockInit, false, protocol.ErrGasLimitPosition},
		{"after tx0/bound", valid, bound[2].Hash(), offset, afterTx0, false, nil},
		{"after tx0 without receipts/bound", valid, bound[2].Hash(), offset, afterTx0, true, nil},
		{"after tx0 without receipts/unbound", invalid, unbound[2].Hash(), offset, afterTx0, true, nil},
	}
	if !isAmsterdam {
		cases = append(cases, testCase{"after tx0/unbound", invalid, unbound[2].Hash(), offset, afterTx0, false, protocol.ErrGasLimitPosition})
	}
	for _, tc := range cases {
		for _, serial := range []bool{false, true} {
			// The serial executor refuses Amsterdam blocks.
			if serial && isAmsterdam {
				continue
			}
			name := "parallel/" + tc.name
			if serial {
				name = "serial/" + tc.name
			}
			t.Run(name, func(t *testing.T) {
				fx.engine = boundAt{base, tc.mark, tc.offset}
				fx.noPrefixReceipts = tc.noPrefixReceipts
				err := fx.run(t, tc.block, tc.point, serial)
				if tc.err == nil {
					require.NoError(t, err)
					return
				}
				require.ErrorIs(t, err, tc.err)
			})
		}
	}
}

// fromBlockTxn is a transaction whose message takes its gas from the block, at
// most its gas limit, which its position does not bind.
type fromBlockTxn struct{ types.Transaction }

func (txn fromBlockTxn) AsMessage(s types.Signer, baseFee *uint256.Int, rules *chain.Rules) (*types.Message, error) {
	msg, err := txn.Transaction.AsMessage(s, baseFee, rules)
	if err != nil {
		return nil, err
	}
	msg.SetGasFromBlock(true)
	return msg, nil
}

var (
	// resumeBurner burns all the gas it gets: JUMPDEST PUSH1 0 JUMP.
	resumeBurner = common.HexToAddress("0x000000000000000000000000000000000000b0e4")
	// resumeRecorder stores the gas it has left: GAS PUSH1 0 SSTORE.
	resumeRecorder = common.HexToAddress("0x000000000000000000000000000000000000ea51")
)

// newRecorderFixture returns an Osaka fixture without a block, whose genesis
// adds resumeBurner and resumeRecorder, and the transactions of a block where
// a transaction per burns entry burns that gas and the last one calls
// resumeRecorder with gasLimit, its message taking its gas from the block if
// fromBlock.
func newRecorderFixture(t *testing.T, burns []uint64, gasLimit uint64, fromBlock bool) (*resumeBALFixture, types.Transactions) {
	t.Helper()
	fx := newResumeFixture(t, chain.TestChainOsakaConfig, types.GenesisAlloc{
		resumeBurner:   {Balance: new(big.Int), Nonce: 1, Code: []byte{byte(vm.JUMPDEST), byte(vm.PUSH1), 0, byte(vm.JUMP)}},
		resumeRecorder: {Balance: new(big.Int), Nonce: 1, Code: []byte{byte(vm.GAS), byte(vm.PUSH1), 0, byte(vm.SSTORE), byte(vm.STOP)}},
	})
	signer := types.LatestSignerForChainID(fx.config.ChainID)
	gasPrice := uint256.NewInt(1_000_000_000)
	var legacy []*types.LegacyTx
	for i, burn := range burns {
		legacy = append(legacy, &types.LegacyTx{CommonTx: types.CommonTx{Nonce: uint64(i), To: &resumeBurner, GasLimit: burn}, GasPrice: *gasPrice})
	}
	legacy = append(legacy, &types.LegacyTx{CommonTx: types.CommonTx{Nonce: uint64(len(burns)), To: &resumeRecorder, GasLimit: gasLimit}, GasPrice: *gasPrice})
	var txs types.Transactions
	for _, txn := range legacy {
		signed, err := types.SignTx(txn, *signer, senderIsCoinbaseKey.key)
		require.NoError(t, err)
		txs = append(txs, signed)
	}
	if fromBlock {
		txs[len(burns)] = fromBlockTxn{txs[len(burns)]}
	}
	fx.senders = slices.Repeat([]common.Address{senderIsCoinbaseKey.rawAddress}, len(txs))
	return fx, txs
}

// recordGas returns the gas resumeRecorder stores in fx.block and points
// fx.inspect at have, which then receives what a run stores.
func (fx *resumeBALFixture) recordGas(t *testing.T, have *uint256.Int) uint256.Int {
	t.Helper()
	var want uint256.Int
	bal := fx.block.BlockAccessList()
	for i := range bal {
		if bal[i].Address == resumeRecorder {
			want = bal[i].StorageChanges[0].Changes[0].Value
		}
	}
	require.False(t, want.IsZero())
	fx.inspect = func(t *testing.T, sd *execctx.SharedDomains, tx kv.TemporalTx) {
		v, _, err := sd.GetLatest(kv.StorageDomain, tx, dbutils.GenerateStoragePlainKey(resumeRecorder, common.Hash{}))
		require.NoError(t, err)
		have.SetBytes(v)
	}
	return want
}

// TestResumedBlockGasFromBlock checks that both executors, resuming a block
// after transactions that left less gas than a later message's limit, give
// that message, which takes its gas from the block, what the transactions
// left, also when their receipts are missing, and check the message's bound
// gas limit against it.
func TestResumedBlockGasFromBlock(t *testing.T) {
	for _, prefix := range []struct {
		name  string
		burns []uint64
	}{
		{"one prefix tx", []uint64{16_000_000}},
		{"two prefix txs", []uint64{8_000_000, 8_000_000}},
	} {
		fx, txs := newRecorderFixture(t, prefix.burns, params.MaxTxnGasLimit, true)
		last := len(prefix.burns)
		var left uint64
		fx.engine = gasLeftAt{fx.engine, txs[last].Hash(), &left}
		fx.block, fx.receipts = fx.assembleBlock(t, txs, common.HexToAddress("0x000000000000000000000000000000000000c01b"))
		wantLeft := fx.block.GasLimit() - fx.receipts[last-1].CumulativeGasUsed
		require.Less(t, wantLeft, params.MaxTxnGasLimit)
		require.Equal(t, wantLeft, left)
		var recorded uint256.Int
		want := fx.recordGas(t, &recorded)

		for _, tc := range []struct {
			name             string
			point            resumePoint
			noPrefixReceipts bool
		}{
			{"full", fromBlockStart, false},
			{"after prefix", resumePoint(last), false},
			{"after prefix without receipts", resumePoint(last), true},
		} {
			for _, serial := range []bool{false, true} {
				name := prefix.name + "/parallel/" + tc.name
				if serial {
					name = prefix.name + "/serial/" + tc.name
				}
				t.Run(name, func(t *testing.T) {
					left, recorded = 0, uint256.Int{}
					fx.noPrefixReceipts = tc.noPrefixReceipts
					err := fx.run(t, fx.block, tc.point, serial)
					require.Equal(t, want, recorded, "gas recorded by the message")
					require.NoError(t, err)
					require.Equal(t, wantLeft, left, "gas left checked at the message")
				})
			}
		}
	}
}

const noWritesTxType = 0x7d

func init() {
	types.RegisterTxType(noWritesTxType, types.TxTypeSpec{New: func() types.Transaction { return nil }, Nonceless: true})
}

// noWritesTxn is a transaction whose message, free, nonceless and to an
// account that does not exist, writes no state.
type noWritesTxn struct{ types.Transaction }

func (txn noWritesTxn) AsMessage(s types.Signer, baseFee *uint256.Int, rules *chain.Rules) (*types.Message, error) {
	msg, err := txn.Transaction.AsMessage(s, baseFee, rules)
	if err != nil {
		return nil, err
	}
	var zero uint256.Int
	free := types.NewMessage(msg.From(), accounts.InternAddress(common.HexToAddress("0x000000000000000000000000000000000000dead")), msg.Nonce(), &zero,
		msg.Gas(), &zero, &zero, &zero, nil, nil, false /* checkNonce */, false /* checkTransaction */, false /* checkGas */, true /* isFree */, nil)
	free.SetTxType(noWritesTxType)
	return free, nil
}

// TestResumedBlockGasFromBlockAfterNoWrites checks that both executors resume a
// block after a transaction that wrote no state and left no receipt before a
// message taking its gas from the block: the block start's writes show the
// state history holds the block.
func TestResumedBlockGasFromBlockAfterNoWrites(t *testing.T) {
	fx, txs := newRecorderFixture(t, []uint64{21_000}, params.MaxTxnGasLimit, true)
	// tx0 leaves its sender's nonce to tx1.
	recorder, err := types.SignTx(&types.LegacyTx{
		CommonTx: types.CommonTx{Nonce: 0, To: &resumeRecorder, GasLimit: params.MaxTxnGasLimit}, GasPrice: *uint256.NewInt(1_000_000_000),
	}, *types.LatestSignerForChainID(fx.config.ChainID), senderIsCoinbaseKey.key)
	require.NoError(t, err)
	txs[0], txs[1] = noWritesTxn{txs[0]}, fromBlockTxn{recorder}
	fx.block, fx.receipts = fx.assembleBlock(t, txs, common.HexToAddress("0x000000000000000000000000000000000000c01b"))
	bal := fx.block.BlockAccessList()
	require.True(t, hasStorageChangeAt(bal, 0), "the block start must write storage")
	require.False(t, hasStorageChangeAt(bal, 1), "tx0 must write nothing")
	for i := range bal {
		for _, bc := range bal[i].BalanceChanges {
			require.NotEqual(t, uint32(1), bc.Index, "tx0 must write nothing")
		}
		for _, nc := range bal[i].NonceChanges {
			require.NotEqual(t, uint32(1), nc.Index, "tx0 must write nothing")
		}
	}
	var recorded uint256.Int
	want := fx.recordGas(t, &recorded)
	fx.noPrefixReceipts = true
	for _, serial := range []bool{false, true} {
		name := "parallel"
		if serial {
			name = "serial"
		}
		t.Run(name, func(t *testing.T) {
			recorded = uint256.Int{}
			require.NoError(t, fx.run(t, fx.block, afterTx0, serial))
			require.Equal(t, want, recorded, "gas recorded by tx1")
		})
	}
}

// TestResumedBlockWithoutPrefixGas checks that both executors, resuming a
// block after a transaction whose gas neither its receipt nor the state
// history tells, fail the run without judging the block when a later message
// takes its gas from the block, and execute the block otherwise.
func TestResumedBlockWithoutPrefixGas(t *testing.T) {
	for _, tc := range []struct {
		name      string
		gasLimit  uint64
		fromBlock bool
	}{
		{"gas from block", params.MaxTxnGasLimit, true},
		{"gas limit", 100_000, false},
	} {
		fx, txs := newRecorderFixture(t, []uint64{16_000_000}, tc.gasLimit, tc.fromBlock)
		fx.block, fx.receipts = fx.assembleBlock(t, txs, common.HexToAddress("0x000000000000000000000000000000000000c01b"))
		var recorded uint256.Int
		want := fx.recordGas(t, &recorded)
		fx.noPrefixReceipts, fx.wipeHistory = true, true
		for _, serial := range []bool{false, true} {
			name := "parallel/" + tc.name
			if serial {
				name = "serial/" + tc.name
			}
			t.Run(name, func(t *testing.T) {
				recorded = uint256.Int{}
				var err error
				if serial {
					err = fx.execSerial(t, fx.block, afterTx0)
				} else {
					var verdict *blockVerdict
					verdict, err = fx.exec(t, fx.block, afterTx0)
					if verdict != nil {
						t.Fatalf("block judged: %v", verdict.err)
					}
				}
				if !tc.fromBlock {
					require.NoError(t, err)
					require.Equal(t, want, recorded, "gas recorded by tx1")
					return
				}
				require.ErrorIs(t, err, state.ErrPruned)
				require.NotErrorIs(t, err, rules.ErrInvalidBlock)
				require.NotErrorIs(t, err, ErrWrongTrieRoot)
			})
		}
	}
}

// badMessageTxn is a transaction without a message.
type badMessageTxn struct{ types.Transaction }

func (badMessageTxn) AsMessage(types.Signer, *uint256.Int, *chain.Rules) (*types.Message, error) {
	return nil, errors.New("no message")
}

// TestRestorePriorGas checks that a resumed block's gas pool loses the gas and
// blob gas its earlier transactions' receipts tell, and is left as it is, and
// reported to lack them, where they do not tell it and no later message takes
// its gas from the block, which otherwise fails the run without the state
// history.
func TestRestorePriorGas(t *testing.T) {
	const gasLimit = 30_000_000
	db := newResumeTestDB(t)
	// The receipt of the transaction at txNum 4: 1M gas and a blob so far in its block.
	require.NoError(t, db.UpdateTemporal(t.Context(), func(tx kv.TemporalRwTx) error {
		domains, err := execctx.NewSharedDomains(t.Context(), tx, log.New())
		if err != nil {
			return err
		}
		defer domains.Close()
		if err := rawtemporaldb.AppendReceiptMetadata(domains.AsPutDel(tx), 0, 1_000_000, params.GasPerBlob, 4); err != nil {
			return err
		}
		return domains.Flush(t.Context(), tx)
	}))
	tx, err := db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	amsterdam := chain.TestChainOsakaConfig.Copy()
	amsterdam.AmsterdamTime = common.NewUint64(0)
	maxBlobGas := chain.TestChainOsakaConfig.GetMaxBlobGasPerBlock(0)
	tx0 := signSelfSendTx(t, 0, 0, 1, 21_000, chain.TestChainOsakaConfig, 0)
	tx1 := signSelfSendTx(t, 1, 0, 1, 21_000, chain.TestChainOsakaConfig, 0)
	tx2 := signSelfSendTx(t, 2, 0, 1, 21_000, chain.TestChainOsakaConfig, 0)
	for _, tc := range []struct {
		name        string
		config      *chain.Config
		txNum       uint64
		txIndex     int
		txs         types.Transactions
		err         error
		lacking     bool
		gasUsed     uint64
		blobGasUsed uint64
	}{
		{"resumed", chain.TestChainOsakaConfig, 5, 1, types.Transactions{tx0, fromBlockTxn{tx1}}, nil, false, 1_000_000, params.GasPerBlob},
		{"resumed without receipts", chain.TestChainOsakaConfig, 4, 1, types.Transactions{tx0, tx1, tx2}, nil, true, 0, 0},
		{"resumed without receipts or history before gas from the block", chain.TestChainOsakaConfig, 4, 1, types.Transactions{tx0, fromBlockTxn{tx1}}, state.ErrPruned, false, 0, 0},
		{"resumed without receipts or history before later gas from the block", chain.TestChainOsakaConfig, 4, 1, types.Transactions{tx0, tx1, fromBlockTxn{tx2}}, state.ErrPruned, false, 0, 0},
		// Executing the transaction without a message rejects the block first.
		{"resumed without receipts before a transaction without a message", chain.TestChainOsakaConfig, 4, 1, types.Transactions{tx0, badMessageTxn{tx1}, fromBlockTxn{tx2}}, nil, true, 0, 0},
		{"resumed from amsterdam", amsterdam, 5, 1, types.Transactions{tx0, fromBlockTxn{tx1}}, nil, true, 0, 0},
		// The receipt before the first transaction is the parent block's last.
		{"first transaction", chain.TestChainOsakaConfig, 5, 0, types.Transactions{fromBlockTxn{tx0}}, nil, false, 0, 0},
		{"block start", chain.TestChainOsakaConfig, 5, -1, types.Transactions{fromBlockTxn{tx0}}, nil, false, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gp := protocol.NewGasPool(gasLimit, maxBlobGas)
			te := &txExecutor{cfg: ExecuteBlockCfg{chainConfig: tc.config}}
			header := &types.Header{Number: *uint256.NewInt(1), GasLimit: gasLimit}
			task := &exec.TxTask{TxNum: tc.txNum, TxIndex: tc.txIndex, Txs: tc.txs, Config: tc.config, Header: header}
			lacking, err := te.restorePriorGas(t.Context(), tx, gp, task)
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
				require.NotErrorIs(t, err, rules.ErrInvalidBlock)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.lacking, lacking)
			require.Equal(t, gasLimit-tc.gasUsed, gp.ExecutionGasAvailable())
			require.Equal(t, maxBlobGas-tc.blobGasUsed, gp.BlobGas())
		})
	}
}

// otherTask is a task of another kind than *exec.TxTask.
type otherTask struct{ *exec.TxTask }

// TestParallelResumedAtOtherTask checks that the parallel executor fails a
// block resumed at a task whose prior gas it cannot restore instead of running
// the block with a full gas pool.
func TestParallelResumedAtOtherTask(t *testing.T) {
	config := chain.TestChainOsakaConfig
	pe, roTx := newResumeTestExec(t, newResumeTestDB(t), config)
	pe.blockExecMetrics = newBlockExecMetrics()
	pe.workersCtx = t.Context()
	txs := types.Transactions{signSelfSendTx(t, 0, 0, 1, 21_000, config, 0), signSelfSendTx(t, 1, 0, 1, 21_000, config, 0)}
	header := &types.Header{Number: *uint256.NewInt(1), GasLimit: 30_000_000}
	task := otherTask{&exec.TxTask{TxNum: 4, TxIndex: 1, Txs: txs, Header: header, Config: config}}
	err := pe.processRequest(t.Context(), roTx, &execRequest{blockNum: 1, tasks: []exec.Task{task}})
	require.ErrorContains(t, err, "otherTask")
}

// TestResumedBlockBlobGas checks that both executors, resuming a block after a
// transaction that used blob gas, reject a later one whose blobs the rest of
// the block's blob gas does not fit, also without the first one's receipt when
// the later one takes its gas from the block.
func TestResumedBlockBlobGas(t *testing.T) {
	fx := newResumeFixture(t, chain.TestChainOsakaConfig, nil)
	target := common.HexToAddress("0x0000000000000000000000000000000000000a11")
	coinbase := common.HexToAddress("0x000000000000000000000000000000000000c01b")
	signer := types.LatestSignerForChainID(fx.config.ChainID)
	blobTx := func(nonce uint64) types.Transaction {
		hashes := make([]common.Hash, params.MaxBlobsPerTxn)
		for i := range hashes {
			hashes[i][0] = 0x01 // KZG version
			hashes[i][31] = byte(nonce<<4 | uint64(i))
		}
		txn, err := types.SignTx(&types.BlobTx{
			DynamicFeeTransaction: types.DynamicFeeTransaction{
				CommonTx: types.CommonTx{Nonce: nonce, To: &target, GasLimit: 21_000},
				ChainID:  *fx.config.ChainID, TipCap: *uint256.NewInt(1), FeeCap: *uint256.NewInt(1_000_000_000),
			},
			MaxFeePerBlobGas:    *uint256.NewInt(1_000_000_000),
			BlobVersionedHashes: hashes,
		}, *signer, senderIsCoinbaseKey.key)
		require.NoError(t, err)
		return txn
	}
	// tx0's blobs leave the block too few for tx1's; the header is the one of
	// the block of tx0 alone.
	require.Less(t, fx.config.GetMaxBlobsPerBlock(resumeBlockTime), uint64(2*params.MaxBlobsPerTxn))
	txs := types.Transactions{blobTx(0), blobTx(1)}
	prefix, receipts := fx.assembleBlock(t, txs[:1], coinbase)
	fx.receipts = receipts
	blockOf := func(txs types.Transactions) *types.Block {
		block := types.NewBlockForAsembling(prefix.Header(), txs, nil, receipts, prefix.Withdrawals(), prefix.BlockAccessListSidecar())
		block.SendersToTxs(fx.senders)
		return block
	}
	block, fromBlock := blockOf(txs), blockOf(types.Transactions{txs[0], fromBlockTxn{txs[1]}})

	for _, tc := range []struct {
		name             string
		block            *types.Block
		point            resumePoint
		noPrefixReceipts bool
	}{
		{"full", block, fromBlockStart, false},
		{"after tx0", block, afterTx0, false},
		{"after tx0 without receipts/gas from block", fromBlock, afterTx0, true},
	} {
		for _, serial := range []bool{false, true} {
			name := "parallel/" + tc.name
			if serial {
				name = "serial/" + tc.name
			}
			t.Run(name, func(t *testing.T) {
				fx.block, fx.noPrefixReceipts = tc.block, tc.noPrefixReceipts
				require.ErrorIs(t, fx.run(t, tc.block, tc.point, serial), protocol.ErrBlobGasLimitReached)
			})
		}
	}
}
