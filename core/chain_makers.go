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

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus"
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

	config *params.ChainConfig
	engine consensus.Engine
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
func (b *BlockGen) AddTxWithChain(getHeader func(hash common.Hash, number uint64) *types.Header, engine consensus.Engine, tx types.Transaction) {
	if b.gasPool == nil {
		b.SetCoinbase(common.Address{})
	}
	b.ibs.Prepare(tx.Hash(), common.Hash{}, len(b.txs))
	contractHasTEVM := func(_ common.Hash) (bool, error) { return false, nil }
	receipt, _, err := ApplyTransaction(b.config, getHeader, engine, &b.header.Coinbase, b.gasPool, b.ibs, state.NewNoopWriter(), b.header, tx, &b.header.GasUsed, vm.Config{}, contractHasTEVM)
	if err != nil {
		panic(err)
	}
	b.txs = append(b.txs, tx)
	b.receipts = append(b.receipts, receipt)
}

func (b *BlockGen) AddFailedTxWithChain(getHeader func(hash common.Hash, number uint64) *types.Header, engine consensus.Engine, tx types.Transaction) {
	if b.gasPool == nil {
		b.SetCoinbase(common.Address{})
	}
	b.ibs.Prepare(tx.Hash(), common.Hash{}, len(b.txs))
	contractHasTEVM := func(common.Hash) (bool, error) { return false, nil }
	receipt, _, err := ApplyTransaction(b.config, getHeader, engine, &b.header.Coinbase, b.gasPool, b.ibs, state.NewNoopWriter(), b.header, tx, &b.header.GasUsed, vm.Config{}, contractHasTEVM)
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
func (b *BlockGen) TxNonce(addr common.Address) uint64 {
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
	if b.header.Time <= b.parent.Header().Time {
		panic("block time out of range")
	}
	chainreader := &FakeChainReader{Cfg: b.config}
	parent := b.parent.Header()
	b.header.Difficulty = b.engine.CalcDifficulty(chainreader, b.header.Time, parent.Time, parent.Difficulty, parent.Number.Uint64(), parent.Hash(), parent.UncleHash, parent.Seal)
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
	Length   int
	Headers  []*types.Header
	Blocks   []*types.Block
	Receipts []types.Receipts
	TopBlock *types.Block // Convinience field to access the last block
}

// OneBlock returns a ChainPack which contains just one
// block with given index
func (cp ChainPack) Slice(i, j int) *ChainPack {
	return &ChainPack{
		Length:   j + 1 - i,
		Headers:  cp.Headers[i:j],
		Blocks:   cp.Blocks[i:j],
		Receipts: cp.Receipts[i:j],
		TopBlock: cp.Blocks[j-1],
	}
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
func GenerateChain(config *params.ChainConfig, parent *types.Block, engine consensus.Engine, db kv.RwDB, n int, gen func(int, *BlockGen),
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
		plainStateWriter *state.PlainStateWriter) (*types.Block, types.Receipts, error) {
		b := &BlockGen{i: i, chain: blocks, parent: parent, ibs: ibs, stateReader: stateReader, config: config, engine: engine, txs: make([]types.Transaction, 0, 1), receipts: make([]*types.Receipt, 0, 1), uncles: make([]*types.Header, 0, 1)}
		b.header = makeHeader(chainreader, parent, ibs, b.engine)
		// Mutate the state and block according to any hard-fork specs
		if daoBlock := config.DAOForkBlock; daoBlock != nil {
			limit := new(big.Int).Add(daoBlock, params.DAOForkExtraRange)
			if b.header.Number.Cmp(daoBlock) >= 0 && b.header.Number.Cmp(limit) < 0 {
				if config.DAOForkSupport {
					b.header.Extra = common.CopyBytes(params.DAOForkBlockExtra)
				}
			}
		}
		if config.DAOForkSupport && config.DAOForkBlock != nil && config.DAOForkBlock.Cmp(b.header.Number) == 0 {
			misc.ApplyDAOHardFork(ibs)
		}
		// Execute any user modifications to the block
		if gen != nil {
			gen(i, b)
		}
		if b.engine != nil {
			// Finalize and seal the block
			if _, err := b.engine.FinalizeAndAssemble(config, b.header, ibs, b.txs, b.uncles, b.receipts, nil, nil, nil, nil); err != nil {
				return nil, nil, fmt.Errorf("call to FinaliseAndAssemble: %w", err)
			}
			// Write state changes to db
			if err := ibs.CommitBlock(config.Rules(b.header.Number.Uint64()), plainStateWriter); err != nil {
				return nil, nil, fmt.Errorf("call to CommitBlock to plainStateWriter: %w", err)
			}

			if err := tx.ClearBucket(kv.HashedAccounts); err != nil {
				return nil, nil, fmt.Errorf("clear HashedAccounts bucket: %w", err)
			}
			if err := tx.ClearBucket(kv.HashedStorage); err != nil {
				return nil, nil, fmt.Errorf("clear HashedStorage bucket: %w", err)
			}
			if err := tx.ClearBucket(kv.TrieOfAccounts); err != nil {
				return nil, nil, fmt.Errorf("clear TrieOfAccounts bucket: %w", err)
			}
			if err := tx.ClearBucket(kv.TrieOfStorage); err != nil {
				return nil, nil, fmt.Errorf("clear TrieOfStorage bucket: %w", err)
			}
			c, err := tx.Cursor(kv.PlainState)
			if err != nil {
				return nil, nil, err
			}
			h := common.NewHasher()
			defer common.ReturnHasherToPool(h)
			for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
				if err != nil {
					return nil, nil, fmt.Errorf("interate over plain state: %w", err)
				}
				var newK []byte
				if len(k) == common.AddressLength {
					newK = make([]byte, common.HashLength)
				} else {
					newK = make([]byte, common.HashLength*2+common.IncarnationLength)
				}
				h.Sha.Reset()
				//nolint:errcheck
				h.Sha.Write(k[:common.AddressLength])
				//nolint:errcheck
				h.Sha.Read(newK[:common.HashLength])
				if len(k) > common.AddressLength {
					copy(newK[common.HashLength:], k[common.AddressLength:common.AddressLength+common.IncarnationLength])
					h.Sha.Reset()
					//nolint:errcheck
					h.Sha.Write(k[common.AddressLength+common.IncarnationLength:])
					//nolint:errcheck
					h.Sha.Read(newK[common.HashLength+common.IncarnationLength:])
					if err = tx.Put(kv.HashedStorage, newK, common.CopyBytes(v)); err != nil {
						return nil, nil, fmt.Errorf("insert hashed key: %w", err)
					}
				} else {
					if err = tx.Put(kv.HashedAccounts, newK, common.CopyBytes(v)); err != nil {
						return nil, nil, fmt.Errorf("insert hashed key: %w", err)
					}
				}

			}
			c.Close()
			if GenerateTrace {
				fmt.Printf("State after %d================\n", b.header.Number)
				if err := tx.ForEach(kv.HashedAccounts, nil, func(k, v []byte) error {
					fmt.Printf("%x: %x\n", k, v)
					return nil
				}); err != nil {
					return nil, nil, fmt.Errorf("print state: %w", err)
				}
				fmt.Printf("..................\n")
				if err := tx.ForEach(kv.HashedStorage, nil, func(k, v []byte) error {
					fmt.Printf("%x: %x\n", k, v)
					return nil
				}); err != nil {
					return nil, nil, fmt.Errorf("print state: %w", err)
				}
				fmt.Printf("===============================\n")
			}
			if hash, err := trie.CalcRoot("GenerateChain", tx); err == nil {
				b.header.Root = hash
			} else {
				return nil, nil, fmt.Errorf("call to CalcTrieRoot: %w", err)
			}

			// Recreating block to make sure Root makes it into the header
			block := types.NewBlock(b.header, b.txs, b.uncles, b.receipts)
			return block, b.receipts, nil
		}
		return nil, nil, fmt.Errorf("no engine to generate blocks")
	}

	for i := 0; i < n; i++ {
		stateReader := state.NewPlainStateReader(tx)
		plainStateWriter := state.NewPlainStateWriter(tx, nil, parent.Number().Uint64()+uint64(i)+1)
		ibs := state.New(stateReader)
		block, receipt, err := genblock(i, parent, ibs, stateReader, plainStateWriter)
		if err != nil {
			return nil, fmt.Errorf("generating block %d: %w", i, err)
		}
		headers[i] = block.Header()
		blocks[i] = block
		receipts[i] = receipt
		parent = block
	}

	tx.Rollback()

	return &ChainPack{Length: n, Headers: headers, Blocks: blocks, Receipts: receipts, TopBlock: blocks[n-1]}, nil
}

func makeHeader(chain consensus.ChainReader, parent *types.Block, state *state.IntraBlockState, engine consensus.Engine) *types.Header {
	var time uint64
	if parent.Time() == 0 {
		time = 10
	} else {
		time = parent.Time() + 10 // block time is fixed at 10 seconds
	}

	header := &types.Header{
		Root:       common.Hash{},
		ParentHash: parent.Hash(),
		Coinbase:   parent.Coinbase(),
		Difficulty: engine.CalcDifficulty(chain, time,
			time-10,
			parent.Difficulty(),
			parent.Number().Uint64(),
			parent.Hash(),
			parent.UncleHash(),
			parent.Header().Seal,
		),
		GasLimit: CalcGasLimit(parent.GasUsed(), parent.GasLimit(), parent.GasLimit(), parent.GasLimit()),
		Number:   new(big.Int).Add(parent.Number(), common.Big1),
		Time:     time,
	}
	header.Seal = engine.GenerateSeal(chain, header, parent.Header(), nil)

	if chain.Config().IsLondon(header.Number.Uint64()) {
		header.BaseFee = misc.CalcBaseFee(chain.Config(), parent.Header())
		header.Eip1559 = true
	}
	//header.WithSeal = debug.HeadersSeal()

	return header
}

type FakeChainReader struct {
	Cfg     *params.ChainConfig
	current *types.Block
}

// Config returns the chain configuration.
func (cr *FakeChainReader) Config() *params.ChainConfig {
	return cr.Cfg
}

func (cr *FakeChainReader) CurrentHeader() *types.Header                            { return cr.current.Header() }
func (cr *FakeChainReader) GetHeaderByNumber(number uint64) *types.Header           { return nil }
func (cr *FakeChainReader) GetHeaderByHash(hash common.Hash) *types.Header          { return nil }
func (cr *FakeChainReader) GetHeader(hash common.Hash, number uint64) *types.Header { return nil }
func (cr *FakeChainReader) GetBlock(hash common.Hash, number uint64) *types.Block   { return nil }
func (cr *FakeChainReader) HasBlock(hash common.Hash, number uint64) bool           { return false }
