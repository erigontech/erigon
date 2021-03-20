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

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/consensus/misc"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/turbo/trie"
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
	txs      []*types.Transaction
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
func (b *BlockGen) AddTx(tx *types.Transaction) {
	b.AddTxWithChain(nil, tx)
}

// AddTxWithChain adds a transaction to the generated block. If no coinbase has
// been set, the block's coinbase is set to the zero address.
//
// AddTxWithChain panics if the transaction cannot be executed. In addition to
// the protocol-imposed limitations (gas limit, etc.), there are some
// further limitations on the content of transactions that can be
// added. If contract code relies on the BLOCKHASH instruction,
// the block in chain will be returned.
func (b *BlockGen) AddTxWithChain(bc ChainContext, tx *types.Transaction) {
	if b.gasPool == nil {
		b.SetCoinbase(common.Address{})
	}
	b.ibs.Prepare(tx.Hash(), common.Hash{}, len(b.txs))
	receipt, err := ApplyTransaction(b.config, bc, &b.header.Coinbase, b.gasPool, b.ibs, state.NewNoopWriter(), b.header, tx, &b.header.GasUsed, vm.Config{})
	if err != nil {
		panic(err)
	}
	b.txs = append(b.txs, tx)
	b.receipts = append(b.receipts, receipt)
}

// AddUncheckedTx forcefully adds a transaction to the block without any
// validation.
//
// AddUncheckedTx will cause consensus failures when used during real
// chain processing. This is best used in conjunction with raw block insertion.
func (b *BlockGen) AddUncheckedTx(tx *types.Transaction) {
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
	chainreader := &fakeChainReader{config: b.config}
	parent := b.parent.Header()
	b.header.Difficulty = b.engine.CalcDifficulty(chainreader, b.header.Time, parent.Time, parent.Difficulty, parent.Number, parent.Hash(), parent.UncleHash)
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

// makeBlockChain creates a deterministic chain of blocks rooted at parent.
func makeBlockChain(parent *types.Block, n int, engine consensus.Engine, db *ethdb.ObjectDatabase, seed int) []*types.Block { //nolint:unused
	blocks, _, _ := GenerateChain(params.TestChainConfig, parent, engine, db, n, func(i int, b *BlockGen) {
		b.SetCoinbase(common.Address{0: byte(seed), 19: byte(i)})
	}, false /*intermediate hashes*/)
	return blocks
}

// makeHeaderChain creates a deterministic chain of headers rooted at parent.
func makeHeaderChain(parent *types.Header, n int, engine consensus.Engine, db *ethdb.ObjectDatabase, seed int) []*types.Header { //nolint:unused
	blocks := makeBlockChain(types.NewBlockWithHeader(parent), n, engine, db, seed)
	headers := make([]*types.Header, len(blocks))
	for i, block := range blocks {
		headers[i] = block.Header()
	}
	return headers
}

var GenerateTrace bool

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
func GenerateChain(config *params.ChainConfig, parent *types.Block, engine consensus.Engine, db *ethdb.ObjectDatabase, n int, gen func(int, *BlockGen),
	intermediateHashes bool,
) ([]*types.Block, []types.Receipts, error) {
	if config == nil {
		config = params.TestChainConfig
	}
	blocks, receipts := make(types.Blocks, n), make([]types.Receipts, n)
	chainreader := &fakeChainReader{config: config}
	//dbCopy := db.MemCopy()
	//defer dbCopy.Close()
	tx, errBegin := db.Begin(context.Background(), ethdb.RW)
	if errBegin != nil {
		return nil, nil, errBegin
	}
	defer tx.Rollback()

	genblock := func(i int, parent *types.Block, ibs *state.IntraBlockState, stateReader state.StateReader,
		plainStateWriter *state.PlainStateWriter) (*types.Block, types.Receipts, error) {
		b := &BlockGen{i: i, chain: blocks, parent: parent, ibs: ibs, stateReader: stateReader, config: config, engine: engine, txs: make([]*types.Transaction, 0, 1), receipts: make([]*types.Receipt, 0, 1), uncles: make([]*types.Header, 0, 1)}
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
			if _, err := b.engine.FinalizeAndAssemble(config, b.header, ibs, b.txs, b.uncles, b.receipts); err != nil {
				return nil, nil, fmt.Errorf("call to FinaliseAndAssemble: %w", err)
			}
			ctx := config.WithEIPsFlags(context.Background(), b.header.Number)
			// Write state changes to db
			//if err := ibs.CommitBlock(ctx, stateWriter); err != nil {
			//	return nil, nil, fmt.Errorf("call to CommitBlock to stateWriter:  %w", err)
			//}
			if err := ibs.CommitBlock(ctx, plainStateWriter); err != nil {
				return nil, nil, fmt.Errorf("call to CommitBlock to plainStateWriter: %w", err)
			}

			if err := tx.Walk(dbutils.HashedAccountsBucket, nil, 0, func(k, v []byte) (bool, error) {
				return true, tx.Delete(dbutils.HashedAccountsBucket, k, v)
			}); err != nil {
				return nil, nil, fmt.Errorf("clear HashedState bucket: %w", err)
			}
			if err := tx.Walk(dbutils.HashedStorageBucket, nil, 0, func(k, v []byte) (bool, error) {
				return true, tx.Delete(dbutils.HashedStorageBucket, k, v)
			}); err != nil {
				return nil, nil, fmt.Errorf("clear HashedState bucket: %w", err)
			}
			c := tx.(ethdb.HasTx).Tx().Cursor(dbutils.PlainStateBucket)
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
					if err = tx.Put(dbutils.HashedStorageBucket, newK, common.CopyBytes(v)); err != nil {
						return nil, nil, fmt.Errorf("insert hashed key: %w", err)
					}
				} else {
					if err = tx.Put(dbutils.HashedAccountsBucket, newK, common.CopyBytes(v)); err != nil {
						return nil, nil, fmt.Errorf("insert hashed key: %w", err)
					}
				}

			}
			c.Close()
			if GenerateTrace {
				fmt.Printf("State after %d================\n", i)
				if err := tx.Walk(dbutils.HashedAccountsBucket, nil, 0, func(k, v []byte) (bool, error) {
					fmt.Printf("%x: %x\n", k, v)
					return true, nil
				}); err != nil {
					return nil, nil, fmt.Errorf("print state: %w", err)
				}
				fmt.Printf("===============================\n")
			}
			var hashCollector func(keyHex []byte, _, _, _ uint16, hashes []byte, rootHash []byte) error
			var storageHashCollector func(addrWithInc []byte, keyHex []byte, _, _, _ uint16, hashes []byte, rootHash []byte) error
			unfurl := trie.NewRetainList(0)
			loader := trie.NewFlatDBTrieLoader("GenerateChain")
			if err := loader.Reset(unfurl, hashCollector, storageHashCollector, false); err != nil {
				return nil, nil, fmt.Errorf("call to FlatDbSubTrieLoader.Reset: %w", err)
			}
			if hash, err := loader.CalcTrieRoot(tx, []byte{}, nil); err == nil {
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
			return nil, nil, fmt.Errorf("generating block %d: %w", i, err)
		}
		blocks[i] = block
		receipts[i] = receipt
		parent = block
	}

	//if _, err := tx.Commit(); err != nil {
	//	return nil, nil, err
	//}
	return blocks, receipts, nil
}

func makeHeader(chain consensus.ChainReader, parent *types.Block, state *state.IntraBlockState, engine consensus.Engine) *types.Header {
	var time uint64
	if parent.Time() == 0 {
		time = 10
	} else {
		time = parent.Time() + 10 // block time is fixed at 10 seconds
	}

	return &types.Header{
		Root:       common.Hash{},
		ParentHash: parent.Hash(),
		Coinbase:   parent.Coinbase(),
		Difficulty: engine.CalcDifficulty(chain, time,
			time-10,
			parent.Difficulty(),
			parent.Number(),
			parent.Hash(),
			parent.UncleHash(),
		),

		GasLimit: CalcGasLimit(parent, parent.GasLimit(), parent.GasLimit()),
		Number:   new(big.Int).Add(parent.Number(), common.Big1),
		Time:     time,
	}
}

type fakeChainReader struct {
	config *params.ChainConfig
}

// Config returns the chain configuration.
func (cr *fakeChainReader) Config() *params.ChainConfig {
	return cr.config
}

func (cr *fakeChainReader) CurrentHeader() *types.Header                            { return nil }
func (cr *fakeChainReader) GetHeaderByNumber(number uint64) *types.Header           { return nil }
func (cr *fakeChainReader) GetHeaderByHash(hash common.Hash) *types.Header          { return nil }
func (cr *fakeChainReader) GetHeader(hash common.Hash, number uint64) *types.Header { return nil }
func (cr *fakeChainReader) GetBlock(hash common.Hash, number uint64) *types.Block   { return nil }
