// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package core

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/systemcontracts"
	"github.com/ledgerwatch/erigon/eth/ethconfig"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/merge"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/trie"
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

	gasPool  *GasPool
	txs      []types.Transaction
	receipts []*types.Receipt
	uncles   []*types.Header

	config *chain.Config
	engine consensus.Engine
}

// SetCoinbase sets the coinbase of the generated block.
// It can be called at most once.
func (b *BlockGen) SetCoinbase(addr libcommon.Address) {
	if b.gasPool != nil {
		if len(b.txs) > 0 {
			panic("coinbase must be set before adding transactions")
		}
		panic("coinbase can only be set once")
	}
	b.header.Coinbase = addr
	b.gasPool = new(GasPool).AddGas(b.header.GasLimit)
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
func (b *BlockGen) AddTxWithChain(getHeader func(hash libcommon.Hash, number uint64) *types.Header, engine consensus.Engine, tx types.Transaction) {
	if b.gasPool == nil {
		b.SetCoinbase(libcommon.Address{})
	}
	b.ibs.SetTxContext(tx.Hash(), libcommon.Hash{}, len(b.txs))
	receipt, _, err := ApplyTransaction(b.config, GetHashFn(b.header, getHeader), engine, &b.header.Coinbase, b.gasPool, b.ibs, state.NewNoopWriter(), b.header, tx, &b.header.GasUsed, vm.Config{})
	if err != nil {
		panic(err)
	}
	b.txs = append(b.txs, tx)
	b.receipts = append(b.receipts, receipt)
}

func (b *BlockGen) AddFailedTxWithChain(getHeader func(hash libcommon.Hash, number uint64) *types.Header, engine consensus.Engine, tx types.Transaction) {
	if b.gasPool == nil {
		b.SetCoinbase(libcommon.Address{})
	}
	b.ibs.SetTxContext(tx.Hash(), libcommon.Hash{}, len(b.txs))
	receipt, _, err := ApplyTransaction(b.config, GetHashFn(b.header, getHeader), engine, &b.header.Coinbase, b.gasPool, b.ibs, state.NewNoopWriter(), b.header, tx, &b.header.GasUsed, vm.Config{})
	_ = err // accept failed transactions
	b.txs = append(b.txs, tx)
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
func (b *BlockGen) TxNonce(addr libcommon.Address) uint64 {
	if !b.ibs.Exist(addr) {
		panic("account does not exist")
	}
	return b.ibs.GetNonce(addr)
}

// AddUncle adds an uncle header to the generated block.
func (b *BlockGen) AddUncle(h *types.Header) {
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
	Headers  []*types.Header
	Blocks   []*types.Block
	Receipts []types.Receipts
	TopBlock *types.Block // Convenience field to access the last block
}

func (cp *ChainPack) Length() int {
	return len(cp.Blocks)
}

// OneBlock returns a ChainPack which contains just one
// block with given index
func (cp *ChainPack) Slice(i, j int) *ChainPack {
	return &ChainPack{
		Headers:  cp.Headers[i:j],
		Blocks:   cp.Blocks[i:j],
		Receipts: cp.Receipts[i:j],
		TopBlock: cp.Blocks[j-1],
	}
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

	return &ChainPack{
		Headers:  headers,
		Blocks:   blocks,
		Receipts: receipts,
		TopBlock: topBlock,
	}
}

func (cp *ChainPack) NumberOfPoWBlocks() int {
	for i, header := range cp.Headers {
		if header.Difficulty.Cmp(merge.ProofOfStakeDifficulty) == 0 {
			return i
		}
	}
	return len(cp.Headers)
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
func GenerateChain(config *chain.Config, parent *types.Block, engine consensus.Engine, db kv.RwDB, n int, gen func(int, *BlockGen),
	intermediateHashes bool,
) (*ChainPack, error) {
	if config == nil {
		config = params.TestChainConfig
	}
	headers, blocks, receipts := make([]*types.Header, n), make(types.Blocks, n), make([]types.Receipts, n)
	chainreader := &FakeChainReader{Cfg: config, current: parent}
	tx, errBegin := db.BeginRw(context.Background())
	if errBegin != nil {
		return nil, errBegin
	}
	defer tx.Rollback()

	genblock := func(i int, parent *types.Block, ibs *state.IntraBlockState, stateReader state.StateReader,
		stateWriter state.StateWriter) (*types.Block, types.Receipts, error) {
		b := &BlockGen{i: i, chain: blocks, parent: parent, ibs: ibs, stateReader: stateReader, config: config, engine: engine, txs: make([]types.Transaction, 0, 1), receipts: make([]*types.Receipt, 0, 1), uncles: make([]*types.Header, 0, 1)}
		b.header = makeHeader(chainreader, parent, ibs, b.engine)
		// Mutate the state and block according to any hard-fork specs
		if daoBlock := config.DAOForkBlock; daoBlock != nil {
			limit := new(big.Int).Add(daoBlock, params.DAOForkExtraRange)
			if b.header.Number.Cmp(daoBlock) >= 0 && b.header.Number.Cmp(limit) < 0 {
				b.header.Extra = common.CopyBytes(params.DAOForkBlockExtra)
			}
			if daoBlock.Cmp(b.header.Number) == 0 {
				misc.ApplyDAOHardFork(ibs)
			}
		}
		systemcontracts.UpgradeBuildInSystemContract(config, b.header.Number, ibs)
		// Execute any user modifications to the block
		if gen != nil {
			gen(i, b)
		}
		if b.engine != nil {
			// Finalize and seal the block
			if _, _, _, err := b.engine.FinalizeAndAssemble(config, b.header, ibs, b.txs, b.uncles, b.receipts, nil, nil, nil, nil); err != nil {
				return nil, nil, fmt.Errorf("call to FinaliseAndAssemble: %w", err)
			}
			// Write state changes to db
			if err := ibs.CommitBlock(config.Rules(b.header.Number.Uint64(), b.header.Time), stateWriter); err != nil {
				return nil, nil, fmt.Errorf("call to CommitBlock to stateWriter: %w", err)
			}

			var err error
			b.header.Root, err = hashRoot(tx, b.header)
			if err != nil {
				return nil, nil, fmt.Errorf("call to CalcTrieRoot: %w", err)
			}
			// Recreating block to make sure Root makes it into the header
			block := types.NewBlock(b.header, b.txs, b.uncles, b.receipts, nil /* withdrawals */)
			return block, b.receipts, nil
		}
		return nil, nil, fmt.Errorf("no engine to generate blocks")
	}

	var txNum uint64
	for i := 0; i < n; i++ {
		var stateReader state.StateReader
		var stateWriter state.StateWriter

		if ethconfig.EnableHistoryV4InTest {
			panic("implement me on v4")
		} else {
			stateReader = state.NewPlainStateReader(tx)
			stateWriter = state.NewPlainStateWriter(tx, nil, parent.NumberU64()+uint64(i)+1)
		}
		ibs := state.New(stateReader)
		block, receipt, err := genblock(i, parent, ibs, stateReader, stateWriter)
		if err != nil {
			return nil, fmt.Errorf("generating block %d: %w", i, err)
		}
		headers[i] = block.Header()
		blocks[i] = block
		receipts[i] = receipt
		parent = block
		//TODO: genblock must call agg.SetTxNum after each txNum???
		txNum += uint64(block.Transactions().Len() + 2) //2 system txsr
	}

	tx.Rollback()

	return &ChainPack{Headers: headers, Blocks: blocks, Receipts: receipts, TopBlock: blocks[n-1]}, nil
}

func hashRoot(tx kv.RwTx, header *types.Header) (hashRoot libcommon.Hash, err error) {
	if ethconfig.EnableHistoryV4InTest {
		if GenerateTrace {
			panic("implement me on v4")
		}
		panic("implement me on v4")
	}

	if err := tx.ClearBucket(kv.HashedAccounts); err != nil {
		return hashRoot, fmt.Errorf("clear HashedAccounts bucket: %w", err)
	}
	if err := tx.ClearBucket(kv.HashedStorage); err != nil {
		return hashRoot, fmt.Errorf("clear HashedStorage bucket: %w", err)
	}
	if err := tx.ClearBucket(kv.TrieOfAccounts); err != nil {
		return hashRoot, fmt.Errorf("clear TrieOfAccounts bucket: %w", err)
	}
	if err := tx.ClearBucket(kv.TrieOfStorage); err != nil {
		return hashRoot, fmt.Errorf("clear TrieOfStorage bucket: %w", err)
	}
	c, err := tx.Cursor(kv.PlainState)
	if err != nil {
		return hashRoot, err
	}
	h := common.NewHasher()
	defer common.ReturnHasherToPool(h)
	for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
		if err != nil {
			return hashRoot, fmt.Errorf("interate over plain state: %w", err)
		}
		var newK []byte
		if len(k) == length.Addr {
			newK = make([]byte, length.Hash)
		} else {
			newK = make([]byte, length.Hash*2+length.Incarnation)
		}
		h.Sha.Reset()
		//nolint:errcheck
		h.Sha.Write(k[:length.Addr])
		//nolint:errcheck
		h.Sha.Read(newK[:length.Hash])
		if len(k) > length.Addr {
			copy(newK[length.Hash:], k[length.Addr:length.Addr+length.Incarnation])
			h.Sha.Reset()
			//nolint:errcheck
			h.Sha.Write(k[length.Addr+length.Incarnation:])
			//nolint:errcheck
			h.Sha.Read(newK[length.Hash+length.Incarnation:])
			if err = tx.Put(kv.HashedStorage, newK, common.CopyBytes(v)); err != nil {
				return hashRoot, fmt.Errorf("insert hashed key: %w", err)
			}
		} else {
			if err = tx.Put(kv.HashedAccounts, newK, common.CopyBytes(v)); err != nil {
				return hashRoot, fmt.Errorf("insert hashed key: %w", err)
			}
		}

	}
	c.Close()
	if GenerateTrace {
		fmt.Printf("State after %d================\n", header.Number)
		it, err := tx.Range(kv.HashedAccounts, nil, nil)
		if err != nil {
			return hashRoot, err
		}
		for it.HasNext() {
			k, v, err := it.Next()
			if err != nil {
				return hashRoot, err
			}
			fmt.Printf("%x: %x\n", k, v)
		}
		fmt.Printf("..................\n")
		it, err = tx.Range(kv.HashedStorage, nil, nil)
		if err != nil {
			return hashRoot, err
		}
		for it.HasNext() {
			k, v, err := it.Next()
			if err != nil {
				return hashRoot, err
			}
			fmt.Printf("%x: %x\n", k, v)
		}
		fmt.Printf("===============================\n")
	}
	if hash, err := trie.CalcRoot("GenerateChain", tx); err == nil {
		return hash, nil
	} else {
		return libcommon.Hash{}, fmt.Errorf("call to CalcTrieRoot: %w", err)
	}
}

func MakeEmptyHeader(parent *types.Header, chainConfig *chain.Config, timestamp uint64, targetGasLimit *uint64) *types.Header {
	header := &types.Header{
		Root:       parent.Root,
		ParentHash: parent.Hash(),
		Number:     new(big.Int).Add(parent.Number, libcommon.Big1),
		Difficulty: libcommon.Big0,
		Time:       timestamp,
	}

	parentGasLimit := parent.GasLimit
	// Set baseFee and GasLimit if we are on an EIP-1559 chain
	if chainConfig.IsLondon(header.Number.Uint64()) {
		header.BaseFee = misc.CalcBaseFee(chainConfig, parent)
		if !chainConfig.IsLondon(parent.Number.Uint64()) {
			parentGasLimit = parent.GasLimit * params.ElasticityMultiplier
		}
	}
	if targetGasLimit != nil {
		header.GasLimit = CalcGasLimit(parentGasLimit, *targetGasLimit)
	} else {
		header.GasLimit = parentGasLimit
	}

	if chainConfig.IsCancun(header.Time) {
		excessDataGas := misc.CalcExcessDataGas(parent)
		header.ExcessDataGas = &excessDataGas
	}

	return header
}

func makeHeader(chain consensus.ChainReader, parent *types.Block, state *state.IntraBlockState, engine consensus.Engine) *types.Header {
	var time uint64
	if parent.Time() == 0 {
		time = 10
	} else {
		time = parent.Time() + 10 // block time is fixed at 10 seconds
	}

	header := MakeEmptyHeader(parent.Header(), chain.Config(), time, nil)
	header.Coinbase = parent.Coinbase()
	header.Difficulty = engine.CalcDifficulty(chain, time,
		time-10,
		parent.Difficulty(),
		parent.NumberU64(),
		parent.Hash(),
		parent.UncleHash(),
		parent.Header().AuRaStep,
	)
	header.AuRaSeal = engine.GenerateSeal(chain, header, parent.Header(), nil)

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

func (cr *FakeChainReader) CurrentHeader() *types.Header                               { return cr.current.Header() }
func (cr *FakeChainReader) GetHeaderByNumber(number uint64) *types.Header              { return nil }
func (cr *FakeChainReader) GetHeaderByHash(hash libcommon.Hash) *types.Header          { return nil }
func (cr *FakeChainReader) GetHeader(hash libcommon.Hash, number uint64) *types.Header { return nil }
func (cr *FakeChainReader) GetBlock(hash libcommon.Hash, number uint64) *types.Block   { return nil }
func (cr *FakeChainReader) HasBlock(hash libcommon.Hash, number uint64) bool           { return false }
func (cr *FakeChainReader) GetTd(hash libcommon.Hash, number uint64) *big.Int          { return nil }
