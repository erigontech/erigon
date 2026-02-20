// Copyright 2015 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package blockgen

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

// BlockGen creates blocks for testing.
// See GenerateChain for a detailed explanation.
type BlockGen struct {
	i           int
	parent      *types.Block
	chain       []*types.Block
	header      *types.Header
	stateReader state.StateReader
	ibs         *state.IntraBlockState
	versionMap  *state.VersionMap
	blockIO     *state.VersionedIO
	gasPool     *protocol.GasPool
	txs         []types.Transaction
	receipts    types.Receipts
	uncles      []*types.Header
	withdrawals []*types.Withdrawal

	config *chain.Config
	engine rules.Engine

	beforeAddTx func()
}

// SetCoinbase sets the coinbase of the generated block.
// It can be called at most once.
func (b *BlockGen) SetCoinbase(addr common.Address) {
	if b.gasPool != nil {
		if len(b.txs) > 0 {
			panic("coinbase must be set before adding transactions")
		}
		panic("coinbase can only be set once")
	}
	b.header.Coinbase = addr
	b.gasPool = new(protocol.GasPool).AddGas(b.header.GasLimit)
}

// SetExtra sets the extra data field of the generated block.
func (b *BlockGen) SetExtra(data []byte) {
	b.header.Extra = data
}

// SetNonce sets the nonce field of the generated block.
func (b *BlockGen) SetNonce(nonce types.BlockNonce) {
	b.header.Nonce = nonce
}

// SetDifficulty sets the difficulty field of the generated block. This method is
// useful for Clique tests where the difficulty does not depend on time. For the
// ethash tests, please use OffsetTime, which implicitly recalculates the diff.
func (b *BlockGen) SetDifficulty(diff *big.Int) {
	b.header.Difficulty = diff
}

// AddTx adds a transaction to the generated block. If no coinbase has
// been set, the block's coinbase is set to the zero address.
//
// AddTx panics if the transaction cannot be executed. In addition to
// the protocol-imposed limitations (gas limit, etc.), there are some
// further limitations on the content of transactions that can be
// added. Notably, contract code relying on the BLOCKHASH instruction
// will panic during execution.
func (b *BlockGen) AddTx(tx types.Transaction) {
	b.AddTxWithChain(nil, nil, tx)
}
func (b *BlockGen) AddFailedTx(tx types.Transaction) {
	b.AddFailedTxWithChain(nil, nil, tx)
}

// AddTxWithChain adds a transaction to the generated block. If no coinbase has
// been set, the block's coinbase is set to the zero address.
//
// AddTxWithChain panics if the transaction cannot be executed. In addition to
// the protocol-imposed limitations (gas limit, etc.), there are some
// further limitations on the content of transactions that can be
// added. If contract code relies on the BLOCKHASH instruction,
// the block in chain will be returned.
func (b *BlockGen) AddTxWithChain(getHeader func(hash common.Hash, number uint64) (*types.Header, error), engine rules.Engine, txn types.Transaction) {
	if b.beforeAddTx != nil {
		b.beforeAddTx()
	}
	if b.gasPool == nil {
		b.SetCoinbase(common.Address{})
	}
	// Clear any stale versioned reads/writes accumulated between transactions
	// (e.g. from TxNonce calls in the gen callback). These reads were recorded
	// with the previous tx's txIndex and must not pollute the next tx's read set.
	if b.ibs.IsVersioned() {
		b.ibs.ResetVersionedIO()
	}
	txVersion := state.Version{BlockNum: b.header.Number.Uint64(), TxIndex: len(b.txs)}
	b.ibs.SetTxContext(txVersion.BlockNum, txVersion.TxIndex)
	gasUsed := protocol.NewGasUsed(b.header, b.receipts.CumulativeGasUsed())
	receipt, err := protocol.ApplyTransaction(b.config, protocol.GetHashFn(b.header, getHeader), engine, accounts.InternAddress(b.header.Coinbase), b.gasPool, b.ibs, state.NewNoopWriter(), b.header, txn, gasUsed, vm.Config{})
	protocol.SetGasUsed(b.header, gasUsed)
	if err != nil {
		panic(err)
	}

	if b.ibs.IsVersioned() {
		writes := b.ibs.VersionedWrites(false)
		if b.blockIO != nil {
			b.blockIO.RecordReads(txVersion, b.ibs.VersionedReads())
			b.blockIO.RecordAccesses(txVersion, b.ibs.AccessedAddresses())
			b.blockIO.RecordWrites(txVersion, writes)
		}
		b.versionMap.FlushVersionedWrites(writes, true, "")
		b.ibs.ResetVersionedIO()
	}

	b.txs = append(b.txs, txn)
	b.receipts = append(b.receipts, receipt)
}

func (b *BlockGen) AddFailedTxWithChain(getHeader func(hash common.Hash, number uint64) (*types.Header, error), engine rules.Engine, txn types.Transaction) {
	if b.beforeAddTx != nil {
		b.beforeAddTx()
	}
	if b.gasPool == nil {
		b.SetCoinbase(common.Address{})
	}
	b.ibs.SetTxContext(b.header.Number.Uint64(), len(b.txs))
	gasUsed := protocol.NewGasUsed(b.header, b.receipts.CumulativeGasUsed())
	receipt, err := protocol.ApplyTransaction(b.config, protocol.GetHashFn(b.header, getHeader), engine, accounts.InternAddress(b.header.Coinbase), b.gasPool, b.ibs, state.NewNoopWriter(), b.header, txn, gasUsed, vm.Config{})
	protocol.SetGasUsed(b.header, gasUsed)
	_ = err // accept failed transactions
	b.txs = append(b.txs, txn)
	b.receipts = append(b.receipts, receipt)
}

// AddUncheckedTx forcefully adds a transaction to the block without any
// validation.
//
// AddUncheckedTx will cause consensus failures when used during real
// chain processing. This is best used in conjunction with raw block insertion.
func (b *BlockGen) AddUncheckedTx(tx types.Transaction) {
	b.txs = append(b.txs, tx)
}

func (b *BlockGen) AddWithdrawal(withdrawal *types.Withdrawal) {
	b.withdrawals = append(b.withdrawals, withdrawal)
}

// Number returns the block number of the block being generated.
func (b *BlockGen) Number() *big.Int {
	return new(big.Int).Set(b.header.Number)
}

// AddUncheckedReceipt forcefully adds a receipts to the block without a
// backing transaction.
//
// AddUncheckedReceipt will cause consensus failures when used during real
// chain processing. This is best used in conjunction with raw block insertion.
func (b *BlockGen) AddUncheckedReceipt(receipt *types.Receipt) {
	b.receipts = append(b.receipts, receipt)
}

// TxNonce returns the next valid transaction nonce for the
// account at addr. It panics if the account does not exist.
func (b *BlockGen) TxNonce(addr common.Address) uint64 {
	// When using a versionMap we must read with the "next tx" txIndex so that
	// versionedRead's floor(txIdx-1) sees all previously completed txs' writes.
	// Without this, reads use the previous tx's txIndex and miss its nonce write.
	if b.versionMap != nil {
		b.ibs.SetTxContext(b.header.Number.Uint64(), len(b.txs))
	}
	exist, err := b.ibs.Exist(accounts.InternAddress(addr))
	if err != nil {
		panic(fmt.Sprintf("can't get account: %s", err))
	}
	if !exist {
		panic("account does not exist")
	}
	nonce, err := b.ibs.GetNonce(accounts.InternAddress(addr))
	if err != nil {
		panic(fmt.Sprintf("can't get account: %s", err))
	}
	return nonce
}

// AddUncle adds an uncle header to the generated block.
// If the block is a PoS block (difficulty == 0), uncles are not allowed and the
// call is silently ignored to avoid failures when using the merge engine.
func (b *BlockGen) AddUncle(h *types.Header) {
	if misc.IsPoSHeader(b.header) {
		return
	}
	b.uncles = append(b.uncles, h)
}

// PrevBlock returns a previously generated block by number. It panics if
// num is greater or equal to the number of the block being generated.
// For index -1, PrevBlock returns the parent block given to GenerateChain.
func (b *BlockGen) PrevBlock(index int) *types.Block {
	if index >= b.i {
		panic(fmt.Errorf("block index %d out of range (%d,%d)", index, -1, b.i))
	}
	if index == -1 {
		return b.parent
	}
	return b.chain[index]
}

// OffsetTime modifies the time instance of a block, implicitly changing its
// associated difficulty. It's useful to test scenarios where forking is not
// tied to chain length directly.
func (b *BlockGen) OffsetTime(seconds int64) {
	b.header.Time += uint64(seconds)
	parent := b.parent
	if b.header.Time <= parent.Time() {
		panic("block time out of range")
	}
	chainreader := &FakeChainReader{Cfg: b.config}
	b.header.Difficulty = b.engine.CalcDifficulty(
		chainreader,
		b.header.Time,
		parent.Time(),
		parent.Difficulty(),
		parent.NumberU64(),
		parent.Hash(),
		parent.UncleHash(),
		parent.Header().AuRaStep,
	)
}

func (b *BlockGen) GetHeader() *types.Header {
	return b.header
}

func (b *BlockGen) GetParent() *types.Block {
	return b.parent
}

func (b *BlockGen) GetReceipts() []*types.Receipt {
	return b.receipts
}

var GenerateTrace bool

type ChainPack struct {
	Headers          []*types.Header
	Blocks           []*types.Block
	Receipts         []types.Receipts
	TopBlock         *types.Block // Convenience field to access the last block
	BlockAccessLists [][]byte     // RLP-encoded block access list bytes, indexed parallel to Blocks (nil entry = no BAL)
}

func (cp *ChainPack) Length() int {
	return len(cp.Blocks)
}

// OneBlock returns a ChainPack which contains just one
// block with given index
func (cp *ChainPack) Slice(i, j int) *ChainPack {
	result := &ChainPack{
		Headers:  cp.Headers[i:j],
		Blocks:   cp.Blocks[i:j],
		Receipts: cp.Receipts[i:j],
		TopBlock: cp.Blocks[j-1],
	}
	if len(cp.BlockAccessLists) > 0 {
		result.BlockAccessLists = cp.BlockAccessLists[i:j]
	}
	return result
}

// Copy creates a deep copy of the ChainPack.
func (cp *ChainPack) Copy() *ChainPack {
	headers := make([]*types.Header, 0, len(cp.Headers))
	for _, header := range cp.Headers {
		headers = append(headers, types.CopyHeader(header))
	}

	blocks := make([]*types.Block, 0, len(cp.Blocks))
	for _, block := range cp.Blocks {
		blocks = append(blocks, block.Copy())
	}

	receipts := make([]types.Receipts, 0, len(cp.Receipts))
	for _, receiptList := range cp.Receipts {
		receiptListCopy := make(types.Receipts, 0, len(receiptList))
		for _, receipt := range receiptList {
			receiptListCopy = append(receiptListCopy, receipt.Copy())
		}
		receipts = append(receipts, receiptListCopy)
	}

	topBlock := cp.TopBlock.Copy()

	var blockAccessLists [][]byte
	if len(cp.BlockAccessLists) > 0 {
		blockAccessLists = make([][]byte, len(cp.BlockAccessLists))
		for i, bal := range cp.BlockAccessLists {
			if bal != nil {
				blockAccessLists[i] = make([]byte, len(bal))
				copy(blockAccessLists[i], bal)
			}
		}
	}

	return &ChainPack{
		Headers:          headers,
		Blocks:           blocks,
		Receipts:         receipts,
		TopBlock:         topBlock,
		BlockAccessLists: blockAccessLists,
	}
}

var withdrawalRequestCode = common.Hex2Bytes("3373fffffffffffffffffffffffffffffffffffffffe1460cb5760115f54807fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff146101f457600182026001905f5b5f82111560685781019083028483029004916001019190604d565b909390049250505036603814608857366101f457346101f4575f5260205ff35b34106101f457600154600101600155600354806003026004013381556001015f35815560010160203590553360601b5f5260385f601437604c5fa0600101600355005b6003546002548082038060101160df575060105b5f5b8181146101835782810160030260040181604c02815460601b8152601401816001015481526020019060020154807fffffffffffffffffffffffffffffffff00000000000000000000000000000000168252906010019060401c908160381c81600701538160301c81600601538160281c81600501538160201c81600401538160181c81600301538160101c81600201538160081c81600101535360010160e1565b910180921461019557906002556101a0565b90505f6002555f6003555b5f54807fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff14156101cd57505f5b6001546002828201116101e25750505f6101e8565b01600290035b5f555f600155604c025ff35b5f5ffd")
var withdrawalRequestCodeHash = accounts.InternCodeHash(common.BytesToHash(crypto.Keccak256(withdrawalRequestCode)))
var consolidationRequestCode = common.Hex2Bytes("3373fffffffffffffffffffffffffffffffffffffffe1460d35760115f54807fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff1461019a57600182026001905f5b5f82111560685781019083028483029004916001019190604d565b9093900492505050366060146088573661019a573461019a575f5260205ff35b341061019a57600154600101600155600354806004026004013381556001015f358155600101602035815560010160403590553360601b5f5260605f60143760745fa0600101600355005b6003546002548082038060021160e7575060025b5f5b8181146101295782810160040260040181607402815460601b815260140181600101548152602001816002015481526020019060030154905260010160e9565b910180921461013b5790600255610146565b90505f6002555f6003555b5f54807fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff141561017357505f5b6001546001828201116101885750505f61018e565b01600190035b5f555f6001556074025ff35b5f5ffd")
var consolidationRequestCodeHash = accounts.InternCodeHash(common.BytesToHash(crypto.Keccak256(consolidationRequestCode)))

func InitPraguePreDeploys(db kv.TemporalRwDB, logger log.Logger) error {
	ctx := context.Background()
	return db.UpdateTemporal(ctx, func(tx kv.TemporalRwTx) error {
		domains, err := execctx.NewSharedDomains(ctx, tx, logger)
		if err != nil {
			return err
		}
		defer domains.Close()
		stateWriter := state.NewWriter(domains.AsPutDel(tx), nil, domains.TxNum())

		stateWriter.UpdateAccountData(params.WithdrawalRequestAddress, &accounts.Account{}, &accounts.Account{
			CodeHash: withdrawalRequestCodeHash,
		})
		stateWriter.UpdateAccountCode(params.WithdrawalRequestAddress, 0, withdrawalRequestCodeHash, withdrawalRequestCode)
		stateWriter.UpdateAccountData(params.ConsolidationRequestAddress, &accounts.Account{}, &accounts.Account{
			CodeHash: consolidationRequestCodeHash,
		})
		stateWriter.UpdateAccountCode(params.ConsolidationRequestAddress, 0, consolidationRequestCodeHash, consolidationRequestCode)

		if err := domains.Flush(ctx, tx); err != nil {
			return err
		}

		return nil
	})
}

// GenerateChain creates a chain of n blocks. The first block's
// parent will be the provided parent. db is used to store
// intermediate states and should contain the parent's state trie.
//
// The generator function is called with a new block generator for
// every block. Any transactions and uncles added to the generator
// become part of the block. If gen is nil, the blocks will be empty
// and their coinbase will be the zero address.
//
// Blocks created by GenerateChain do not contain valid proof of work
// values. Inserting them into BlockChain requires use of FakePow or
// a similar non-validating proof of work implementation.
func GenerateChain(config *chain.Config, parent *types.Block, engine rules.Engine, db kv.TemporalRoDB, n int, gen func(int, *BlockGen)) (*ChainPack, error) {
	if config == nil {
		config = chain.TestChainConfig
	}
	headers, blocks, receipts := make([]*types.Header, n), make(types.Blocks, n), make([]types.Receipts, n)
	chainreader := &FakeChainReader{Cfg: config, current: parent}
	ctx := context.Background()
	tx, errBegin := db.BeginTemporalRo(context.Background())
	if errBegin != nil {
		return nil, errBegin
	}
	defer tx.Rollback()
	logger := log.New("generate-chain", config.ChainName)

	domains, err := execctx.NewSharedDomains(ctx, tx, logger)
	if err != nil {
		return nil, err
	}
	defer domains.Close()
	latestTxNum, _, err := domains.SeekCommitment(ctx, tx)
	if err != nil {
		return nil, err
	}

	stateReader := state.NewReaderV3(domains.AsGetter(tx))
	stateWriter := state.NewWriter(domains.AsPutDel(tx), nil, latestTxNum)

	txNum, err := rawdbv3.TxNums.Max(ctx, tx, parent.NumberU64())
	if err != nil {
		return nil, err
	}
	txNumIncrement := func() {
		txNum++
		stateWriter.SetTxNum(txNum)
		domains.SetTxNum(txNum)
	}
	genblock := func(i int, parent *types.Block, ibs *state.IntraBlockState, stateReader state.StateReader,
		stateWriter state.StateWriter) (*types.Block, types.Receipts, []byte, error) {
		txNumIncrement()

		var versionMap *state.VersionMap
		if dbg.Exec3Parallel {
			versionMap = state.NewVersionMap(nil)
			ibs.SetVersionMap(versionMap)
		}

		b := &BlockGen{i: i,
			chain:       blocks,
			parent:      parent,
			ibs:         ibs,
			versionMap:  versionMap,
			stateReader: stateReader,
			config:      config,
			engine:      engine,
			txs:         make([]types.Transaction, 0, 1),
			receipts:    make([]*types.Receipt, 0, 1),
			uncles:      make([]*types.Header, 0, 1),
			beforeAddTx: func() {
				txNumIncrement()
			},
		}
		if chainreader.Config().IsShanghai(parent.Time()) {
			b.withdrawals = []*types.Withdrawal{}
		}
		if chainreader.Config().IsAmsterdam(parent.Time()) {
			b.blockIO = &state.VersionedIO{}
		}

		b.header = makeHeader(chainreader, parent, ibs, b.engine)
		// Mutate the state and block according to any hard-fork specs
		if daoBlock := config.DAOForkBlock; daoBlock != nil {
			limit := new(big.Int).Add(daoBlock, misc.DAOForkExtraRange)
			if b.header.Number.Cmp(daoBlock) >= 0 && b.header.Number.Cmp(limit) < 0 {
				b.header.Extra = common.Copy(misc.DAOForkBlockExtra)
			}
		}
		if b.engine != nil {
			err := protocol.InitializeBlockExecution(b.engine, chainreader, b.header, config, ibs, nil, logger, nil)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("call to InitializeBlockExecution: %w", err)
			}
		}
		// Execute any user modifications to the block
		if gen != nil {
			gen(i, b)
		}
		txNumIncrement()
		if b.engine != nil {
			// When versionMap is active (parallel exec mode), advance IBS txIndex to the
			// end-of-block position so FinalizeAndAssemble (e.g. Ethash block reward via
			// AddBalance) reads from the latest tx writes in the versionMap.
			// Without this, versionedRead uses floor(txIdx-1) with the last real tx's index,
			// missing that tx's own writes.
			if b.versionMap != nil {
				b.ibs.SetTxContext(b.header.Number.Uint64(), len(b.txs))
			}
			// Finalize and seal the block
			syscall := func(contract accounts.Address, data []byte) ([]byte, error) {
				return protocol.SysCallContract(contract, data, config, ibs, b.header, b.engine, false /* constCall */, vm.Config{})
			}
			_, requests, err := b.engine.FinalizeAndAssemble(config, b.header, ibs, b.txs, b.uncles, b.receipts, nil, chainreader, syscall, nil, logger)

			if err != nil {
				return nil, nil, nil, fmt.Errorf("call to FinaliseAndAssemble: %w", err)
			}

			// Write state changes to db
			blockContext := protocol.NewEVMBlockContext(b.header, protocol.GetHashFn(b.header, nil), b.engine, accounts.NilAddress, config)
			if err := ibs.CommitBlock(blockContext.Rules(config), stateWriter); err != nil {
				return nil, nil, nil, fmt.Errorf("call to CommitBlock to stateWriter: %w", err)
			}

			if config.IsPrague(b.header.Time) {
				b.header.RequestsHash = requests.Hash()
				var beaconBlockRoot common.Hash
				if _, err := rand.Read(beaconBlockRoot[:]); err != nil {
					return nil, nil, nil, fmt.Errorf("can't create beacon block root: %w", err)
				}
				b.header.ParentBeaconBlockRoot = &beaconBlockRoot
			}

			var bal types.BlockAccessList
			var balBytes []byte
			if config.IsAmsterdam(b.header.Time) {
				bal = b.blockIO.AsBlockAccessList()
				balHash := bal.Hash()
				b.header.BlockAccessListHash = &balHash
				var encErr error
				balBytes, encErr = types.EncodeBlockAccessListBytes(bal)
				if encErr != nil {
					return nil, nil, nil, fmt.Errorf("encode block access list: %w", encErr)
				}
			}

			stateRoot, err := domains.ComputeCommitment(ctx, tx, true, b.header.Number.Uint64(), uint64(txNum), "", nil)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("call to CalcTrieRoot: %w", err)
			}
			b.header.Root = common.BytesToHash(stateRoot)
			// Recreating block to make sure Root makes it into the header
			block := types.NewBlockForAsembling(b.header, b.txs, b.uncles, b.receipts, b.withdrawals)
			return block, b.receipts, balBytes, nil
		}
		return nil, nil, nil, errors.New("no engine to generate blocks")
	}

	blockAccessLists := make([][]byte, n)
	for i := 0; i < n; i++ {
		ibs := state.New(stateReader)
		if dbg.TraceBlock(uint64(i)) {
			ibs.SetTrace(true)
			domains.SetTrace(true, false)
		}
		block, receipt, balBytes, err := genblock(i, parent, ibs, stateReader, stateWriter)
		ibs.SetTrace(false)
		domains.SetTrace(false, false)
		if err != nil {
			return nil, fmt.Errorf("generating block %d: %w", i, err)
		}
		headers[i] = block.Header()
		blocks[i] = block
		receipts[i] = receipt
		blockAccessLists[i] = balBytes
		parent = block
	}

	return &ChainPack{Headers: headers, Blocks: blocks, Receipts: receipts, BlockAccessLists: blockAccessLists, TopBlock: blocks[n-1]}, nil
}

func makeHeader(chain rules.ChainReader, parent *types.Block, state *state.IntraBlockState, engine rules.Engine) *types.Header {
	var time uint64
	if parent.Time() == 0 {
		time = 10
	} else {
		time = parent.Time() + 10 // block time is fixed at 10 seconds
	}

	header := builder.MakeEmptyHeader(parent.Header(), chain.Config(), time, nil)
	header.Coinbase = parent.Coinbase()
	header.Difficulty = engine.CalcDifficulty(chain, time,
		time-10,
		parent.Difficulty(),
		parent.NumberU64(),
		parent.Hash(),
		parent.UncleHash(),
		parent.Header().AuRaStep,
	)

	return header
}

type FakeChainReader struct {
	Cfg     *chain.Config
	current *types.Block
}

// Config returns the chain configuration.
func (cr *FakeChainReader) Config() *chain.Config {
	return cr.Cfg
}

func (cr *FakeChainReader) CurrentHeader() *types.Header { return cr.current.Header() }
func (cr *FakeChainReader) CurrentFinalizedHeader() *types.Header {
	return nil
}
func (cr *FakeChainReader) CurrentSafeHeader() *types.Header {
	return nil
}
func (cr *FakeChainReader) GetHeaderByNumber(number uint64) *types.Header           { return nil }
func (cr *FakeChainReader) GetHeaderByHash(hash common.Hash) *types.Header          { return nil }
func (cr *FakeChainReader) GetHeader(hash common.Hash, number uint64) *types.Header { return nil }
func (cr *FakeChainReader) GetBlock(hash common.Hash, number uint64) *types.Block   { return nil }
func (cr *FakeChainReader) HasBlock(hash common.Hash, number uint64) bool           { return false }
func (cr *FakeChainReader) GetTd(hash common.Hash, number uint64) *big.Int          { return nil }
func (cr *FakeChainReader) FrozenBlocks() uint64                                    { return 0 }
func (cr *FakeChainReader) FrozenBorBlocks(align bool) uint64                       { return 0 }
