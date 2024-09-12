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

package core

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"

	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/chain"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/kv"
	libstate "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/consensus/merge"
	"github.com/erigontech/erigon/consensus/misc"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/types/accounts"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/rlp"
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

	beforeAddTx func()
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
func (b *BlockGen) AddTxWithChain(getHeader func(hash libcommon.Hash, number uint64) *types.Header, engine consensus.Engine, txn types.Transaction) {
	if b.beforeAddTx != nil {
		b.beforeAddTx()
	}
	if b.gasPool == nil {
		b.SetCoinbase(libcommon.Address{})
	}
	b.ibs.SetTxContext(txn.Hash(), len(b.txs))
	receipt, _, err := ApplyTransaction(b.config, GetHashFn(b.header, getHeader), engine, &b.header.Coinbase, b.gasPool, b.ibs, state.NewNoopWriter(), b.header, txn, &b.header.GasUsed, b.header.BlobGasUsed, vm.Config{})
	if err != nil {
		panic(err)
	}
	b.txs = append(b.txs, txn)
	b.receipts = append(b.receipts, receipt)
}

func (b *BlockGen) AddFailedTxWithChain(getHeader func(hash libcommon.Hash, number uint64) *types.Header, engine consensus.Engine, txn types.Transaction) {
	if b.beforeAddTx != nil {
		b.beforeAddTx()
	}
	if b.gasPool == nil {
		b.SetCoinbase(libcommon.Address{})
	}
	b.ibs.SetTxContext(txn.Hash(), len(b.txs))
	receipt, _, err := ApplyTransaction(b.config, GetHashFn(b.header, getHeader), engine, &b.header.Coinbase, b.gasPool, b.ibs, state.NewNoopWriter(), b.header, txn, &b.header.GasUsed, b.header.BlobGasUsed, vm.Config{})
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
func GenerateChain(config *chain.Config, parent *types.Block, engine consensus.Engine, db kv.RwDB, n int, gen func(int, *BlockGen)) (*ChainPack, error) {
	if config == nil {
		config = params.TestChainConfig
	}
	headers, blocks, receipts := make([]*types.Header, n), make(types.Blocks, n), make([]types.Receipts, n)
	chainreader := &FakeChainReader{Cfg: config, current: parent}
	ctx := context.Background()
	tx, errBegin := db.BeginRw(context.Background())
	if errBegin != nil {
		return nil, errBegin
	}
	defer tx.Rollback()
	logger := log.New("generate-chain", config.ChainName)

	domains, err := libstate.NewSharedDomains(tx, logger)
	if err != nil {
		return nil, err
	}
	defer domains.Close()
	stateReader := state.NewReaderV3(domains)
	stateWriter := state.NewWriterV4(domains)

	txNum := -1
	setBlockNum := func(blockNum uint64) {
		domains.SetBlockNum(blockNum)
	}
	txNumIncrement := func() {
		txNum++
		domains.SetTxNum(uint64(txNum))
	}
	genblock := func(i int, parent *types.Block, ibs *state.IntraBlockState, stateReader state.StateReader,
		stateWriter state.StateWriter) (*types.Block, types.Receipts, error) {
		txNumIncrement()

		b := &BlockGen{i: i, chain: blocks, parent: parent, ibs: ibs, stateReader: stateReader, config: config, engine: engine, txs: make([]types.Transaction, 0, 1), receipts: make([]*types.Receipt, 0, 1), uncles: make([]*types.Header, 0, 1),
			beforeAddTx: func() {
				txNumIncrement()
			},
		}
		b.header = makeHeader(chainreader, parent, ibs, b.engine)
		// Mutate the state and block according to any hard-fork specs
		if daoBlock := config.DAOForkBlock; daoBlock != nil {
			limit := new(big.Int).Add(daoBlock, params.DAOForkExtraRange)
			if b.header.Number.Cmp(daoBlock) >= 0 && b.header.Number.Cmp(limit) < 0 {
				b.header.Extra = libcommon.CopyBytes(params.DAOForkBlockExtra)
			}
		}
		if b.engine != nil {
			err := InitializeBlockExecution(b.engine, nil, b.header, config, ibs, logger, nil)
			if err != nil {
				return nil, nil, fmt.Errorf("call to InitializeBlockExecution: %w", err)
			}
		}
		// Execute any user modifications to the block
		if gen != nil {
			gen(i, b)
		}
		txNumIncrement()
		if b.engine != nil {
			// Finalize and seal the block
			if _, _, _, err := b.engine.FinalizeAndAssemble(config, b.header, ibs, b.txs, b.uncles, b.receipts, nil, nil, nil, nil, nil, logger); err != nil {
				return nil, nil, fmt.Errorf("call to FinaliseAndAssemble: %w", err)
			}
			// Write state changes to db
			if err := ibs.CommitBlock(config.Rules(b.header.Number.Uint64(), b.header.Time), stateWriter); err != nil {
				return nil, nil, fmt.Errorf("call to CommitBlock to stateWriter: %w", err)
			}

			var err error
			//To use `CalcHashRootForTests` need flush before, but to use `domains.ComputeCommitment` need flush after
			//if err = domains.Flush(ctx, tx); err != nil {
			//	return nil, nil, err
			//}
			//b.header.Root, err = CalcHashRootForTests(tx, b.header, histV3, true)
			stateRoot, err := domains.ComputeCommitment(ctx, true, b.header.Number.Uint64(), "")
			if err != nil {
				return nil, nil, fmt.Errorf("call to CalcTrieRoot: %w", err)
			}
			if err = domains.Flush(ctx, tx); err != nil {
				return nil, nil, err
			}
			b.header.Root = libcommon.BytesToHash(stateRoot)

			// Recreating block to make sure Root makes it into the header
			block := types.NewBlockForAsembling(b.header, b.txs, b.uncles, b.receipts, nil /* withdrawals */, nil /*requests*/)
			return block, b.receipts, nil
		}
		return nil, nil, errors.New("no engine to generate blocks")
	}

	for i := 0; i < n; i++ {
		setBlockNum(uint64(i))
		ibs := state.New(stateReader)
		block, receipt, err := genblock(i, parent, ibs, stateReader, stateWriter)
		if err != nil {
			return nil, fmt.Errorf("generating block %d: %w", i, err)
		}
		headers[i] = block.Header()
		blocks[i] = block
		receipts[i] = receipt
		parent = block
	}
	tx.Rollback()

	return &ChainPack{Headers: headers, Blocks: blocks, Receipts: receipts, TopBlock: blocks[n-1]}, nil
}

func hashKeyAndAddIncarnation(k []byte, h *libcommon.Hasher) (newK []byte, err error) {
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
	if len(k) == length.Addr+length.Incarnation+length.Hash { // PlainState storage
		copy(newK[length.Hash:], k[length.Addr:length.Addr+length.Incarnation])
		h.Sha.Reset()
		//nolint:errcheck
		h.Sha.Write(k[length.Addr+length.Incarnation:])
		//nolint:errcheck
		h.Sha.Read(newK[length.Hash+length.Incarnation:])
	} else if len(k) == length.Addr+length.Hash { // e4 Domain storage
		binary.BigEndian.PutUint64(newK[length.Hash:], 1)
		h.Sha.Reset()
		//nolint:errcheck
		h.Sha.Write(k[len(k)-length.Hash:])
		//nolint:errcheck
		h.Sha.Read(newK[length.Hash+length.Incarnation:])
	}
	return newK, nil
}

func CalcHashRootForTests(tx kv.RwTx, header *types.Header, histV4, trace bool) (hashRoot libcommon.Hash, err error) {
	domains, err := libstate.NewSharedDomains(tx, log.New())
	if err != nil {
		return hashRoot, fmt.Errorf("NewSharedDomains: %w", err)
	}
	defer domains.Close()

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

	h := libcommon.NewHasher()
	defer libcommon.ReturnHasherToPool(h)

	it, err := tx.(libstate.HasAggTx).AggTx().(*libstate.AggregatorRoTx).DomainRangeLatest(tx, kv.AccountsDomain, nil, nil, -1)
	if err != nil {
		return libcommon.Hash{}, err
	}

	for it.HasNext() {
		k, v, err := it.Next()
		if err != nil {
			return hashRoot, fmt.Errorf("interate over plain state: %w", err)
		}
		if len(v) > 0 {
			v, err = accounts.ConvertV3toV2(v)
			if err != nil {
				return hashRoot, fmt.Errorf("interate over plain state: %w", err)
			}
		}
		newK, err := hashKeyAndAddIncarnation(k, h)
		if err != nil {
			return hashRoot, fmt.Errorf("clear HashedAccounts bucket: %w", err)
		}
		if err := tx.Put(kv.HashedAccounts, newK, v); err != nil {
			return hashRoot, fmt.Errorf("clear HashedAccounts bucket: %w", err)
		}
	}

	it, err = tx.(libstate.HasAggTx).AggTx().(*libstate.AggregatorRoTx).DomainRangeLatest(tx, kv.StorageDomain, nil, nil, -1)
	if err != nil {
		return libcommon.Hash{}, err
	}
	for it.HasNext() {
		k, v, err := it.Next()
		if err != nil {
			return hashRoot, fmt.Errorf("interate over plain state: %w", err)
		}
		newK, err := hashKeyAndAddIncarnation(k, h)
		if err != nil {
			return hashRoot, fmt.Errorf("clear HashedStorage bucket: %w", err)
		}
		fmt.Printf("storage %x -> %x\n", k, newK)
		if err := tx.Put(kv.HashedStorage, newK, v); err != nil {
			return hashRoot, fmt.Errorf("clear HashedStorage bucket: %w", err)
		}

	}

	if trace {
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
		root, err := domains.ComputeCommitment(context.Background(), true, domains.BlockNum(), "")
		if err != nil {
			return hashRoot, err
		}
		hashRoot.SetBytes(root)
		return hashRoot, nil
	}
	root, err := domains.ComputeCommitment(context.Background(), true, domains.BlockNum(), "")
	if err != nil {
		return hashRoot, err
	}
	hashRoot.SetBytes(root)
	return hashRoot, nil

	//var root libcommon.Hash
	//rootB, err := tx.(*temporal.Tx).Agg().ComputeCommitment(false, false)
	//if err != nil {
	//	return root, err
	//}
	//root = libcommon.BytesToHash(rootB)
	//return root, err
}

func MakeEmptyHeader(parent *types.Header, chainConfig *chain.Config, timestamp uint64, targetGasLimit *uint64) *types.Header {
	header := types.NewEmptyHeaderForAssembling()
	header.Root = parent.Root
	header.ParentHash = parent.Hash()
	header.Number = new(big.Int).Add(parent.Number, libcommon.Big1)
	header.Difficulty = libcommon.Big0
	header.Time = timestamp

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
		excessBlobGas := misc.CalcExcessBlobGas(chainConfig, parent)
		header.ExcessBlobGas = &excessBlobGas
		header.BlobGasUsed = new(uint64)
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
func (cr *FakeChainReader) GetHeaderByNumber(number uint64) *types.Header              { return nil }
func (cr *FakeChainReader) GetHeaderByHash(hash libcommon.Hash) *types.Header          { return nil }
func (cr *FakeChainReader) GetHeader(hash libcommon.Hash, number uint64) *types.Header { return nil }
func (cr *FakeChainReader) GetBlock(hash libcommon.Hash, number uint64) *types.Block   { return nil }
func (cr *FakeChainReader) HasBlock(hash libcommon.Hash, number uint64) bool           { return false }
func (cr *FakeChainReader) GetTd(hash libcommon.Hash, number uint64) *big.Int          { return nil }
func (cr *FakeChainReader) FrozenBlocks() uint64                                       { return 0 }
func (cr *FakeChainReader) FrozenBorBlocks() uint64                                    { return 0 }
func (cr *FakeChainReader) BorEventsByBlock(hash libcommon.Hash, number uint64) []rlp.RawValue {
	return nil
}
func (cr *FakeChainReader) BorStartEventID(hash libcommon.Hash, number uint64) uint64 {
	return 0
}
func (cr *FakeChainReader) BorSpan(spanId uint64) []byte { return nil }
