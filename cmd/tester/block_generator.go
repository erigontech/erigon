package main

import (
	"crypto/ecdsa"
	"encoding/binary"
	"fmt"
	"math/big"
	"math/rand"
	"os"

	"context"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

type BlockGenerator struct {
	input               *os.File
	genesisBlock        *types.Block
	coinbaseKey         *ecdsa.PrivateKey
	blockOffsetByHash   map[common.Hash]uint64
	blockOffsetByNumber map[uint64]uint64
	headersByHash       map[common.Hash]*types.Header
	headersByNumber     map[uint64]*types.Header
	lastBlock           *types.Block
	totalDifficulty     *big.Int
}

func (bg *BlockGenerator) Close() {
	bg.input.Close()
}

func (bg *BlockGenerator) GetHeaderByHash(hash common.Hash) *types.Header {
	return bg.headersByHash[hash]
}

func (bg *BlockGenerator) GetHeaderByNumber(number uint64) *types.Header {
	return bg.headersByNumber[number]
}

func (bg *BlockGenerator) readBlockFromOffset(offset uint64) (*types.Block, error) {
	bg.input.Seek(int64(offset), 0)
	stream := rlp.NewStream(bg.input, 0)
	var b types.Block
	if err := stream.Decode(&b); err != nil {
		return nil, err
	}
	return &b, nil
}

func (bg *BlockGenerator) GetBlockByHash(hash common.Hash) (*types.Block, error) {
	if blockOffset, ok := bg.blockOffsetByHash[hash]; ok {
		return bg.readBlockFromOffset(blockOffset)
	}
	return nil, nil
}

func (bg *BlockGenerator) GetBlockByNumber(number uint64) (*types.Block, error) {
	if blockOffset, ok := bg.blockOffsetByNumber[number]; ok {
		return bg.readBlockFromOffset(blockOffset)
	}
	return nil, nil
}

func (bg *BlockGenerator) TotalDifficulty() *big.Int {
	return bg.totalDifficulty
}

func (bg *BlockGenerator) LastBlock() *types.Block {
	return bg.lastBlock
}

func randAddress(r *rand.Rand) common.Address {
	var b common.Address
	binary.BigEndian.PutUint64(b[:], r.Uint64())
	binary.BigEndian.PutUint64(b[8:], r.Uint64())
	binary.BigEndian.PutUint32(b[16:], r.Uint32())
	return b
}

func NewBlockGenerator(outputFile string, initialHeight int) (*BlockGenerator, error) {
	output, err := os.OpenFile(outputFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return nil, err
	}
	defer output.Close()
	db := ethdb.NewMemDatabase()
	genesisBlock, _, tds, err := core.DefaultGenesisBlock().ToBlock(db)
	if err != nil {
		return nil, err
	}
	parent := genesisBlock
	extra := []byte("BlockGenerator")
	coinbaseKey, err := crypto.GenerateKey()
	if err != nil {
		return nil, err
	}
	coinbase := crypto.PubkeyToAddress(coinbaseKey.PublicKey)
	chainConfig := params.MainnetChainConfig
	var pos uint64
	td := new(big.Int)
	bg := &BlockGenerator{
		genesisBlock:        genesisBlock,
		coinbaseKey:         coinbaseKey,
		blockOffsetByHash:   make(map[common.Hash]uint64),
		blockOffsetByNumber: make(map[uint64]uint64),
		headersByHash:       make(map[common.Hash]*types.Header),
		headersByNumber:     make(map[uint64]*types.Header),
	}
	bg.headersByHash[genesisBlock.Header().Hash()] = genesisBlock.Header()
	bg.headersByNumber[0] = genesisBlock.Header()
	r := rand.New(rand.NewSource(4589489854))
	var nonce uint64        // nonce of the sender (coinbase)
	amount := big.NewInt(1) // 1 wei
	gasPrice := big.NewInt(10000000)
	engine := ethash.NewFullFaker()

	for height := 1; height <= initialHeight; height++ {
		num := parent.Number()
		tstamp := parent.Time().Int64() + 15
		gasLimit := core.CalcGasLimit(parent, 100000, 8000000)
		header := &types.Header{
			ParentHash: parent.Hash(),
			Number:     num.Add(num, common.Big1),
			GasLimit:   gasLimit,
			Extra:      extra,
			Time:       big.NewInt(tstamp),
			Coinbase:   coinbase,
			Difficulty: ethash.CalcDifficulty(chainConfig, uint64(tstamp), parent.Header()),
		}
		tds.SetBlockNr(parent.NumberU64())
		statedb := state.New(tds)
		// Add more transactions
		signedTxs := []*types.Transaction{}
		receipts := []*types.Receipt{}
		usedGas := new(uint64)
		tds.StartNewBuffer()
		if height > 1 && gasLimit >= 21000 {
			signer := types.MakeSigner(chainConfig, big.NewInt(int64(height)))
			gp := new(core.GasPool).AddGas(header.GasLimit)
			vmConfig := vm.Config{}

			to := randAddress(r)
			tx := types.NewTransaction(nonce, to, amount, 21000, gasPrice, []byte{})
			signed_tx, err := types.SignTx(tx, signer, coinbaseKey)
			if err != nil {
				return nil, err
			}
			signedTxs = append(signedTxs, signed_tx)
			receipt, _, err := core.ApplyTransaction(chainConfig, nil, &coinbase, gp, statedb, tds.TrieStateWriter(), header, signed_tx, usedGas, vmConfig)
			if err != nil {
				return nil, fmt.Errorf("tx %x failed: %v", signed_tx.Hash(), err)
			}
			if !chainConfig.IsByzantium(header.Number) {
				tds.StartNewBuffer()
			}
			receipts = append(receipts, receipt)
			nonce++
		} else {
			fmt.Printf("Block %d: Gas limit too low for a transaction: %d\n", height, gasLimit)
		}

		if _, err := engine.Finalize(chainConfig, header, statedb, signedTxs, []*types.Header{}, receipts); err != nil {
			return nil, err
		}
		ctx := chainConfig.WithEIPsFlags(context.Background(), header.Number)
		if err = statedb.FinalizeTx(ctx, tds.TrieStateWriter()); err != nil {
			return nil, err
		}

		var roots []common.Hash
		roots, err = tds.ComputeTrieRoots()
		if err != nil {
			return nil, err
		}
		if !chainConfig.IsByzantium(header.Number) {
			for i, receipt := range receipts {
				receipt.PostState = roots[i].Bytes()
			}
		}
		header.Root = roots[len(roots)-1]
		header.GasUsed = *usedGas
		tds.SetBlockNr(uint64(height))
		err = statedb.CommitBlock(ctx, tds.DbStateWriter())
		if err != nil {
			return nil, err
		}
		// Generate an empty block
		block := types.NewBlock(header, signedTxs, []*types.Header{}, receipts)
		//fmt.Printf("block hash for %d: %x\n", block.NumberU64(), block.Hash())
		if buffer, err := rlp.EncodeToBytes(block); err != nil {
			return nil, err
		} else {
			output.Write(buffer)
			pos += uint64(len(buffer))
		}
		header = block.Header()
		hash := header.Hash()
		bg.headersByHash[hash] = header
		bg.headersByNumber[block.NumberU64()] = header
		bg.blockOffsetByHash[hash] = pos
		bg.blockOffsetByNumber[block.NumberU64()] = pos
		td = new(big.Int).Add(td, block.Difficulty())
		parent = block
	}
	bg.lastBlock = parent
	bg.totalDifficulty = td
	output.Close()
	// Reopen the file for reading
	bg.input, err = os.Open(outputFile)
	if err != nil {
		return nil, err
	}
	return bg, nil
}

// Creates a fork from the existing block generator
func NewForkGenerator(base *BlockGenerator, outputFile string, forkBase int, forkHeight int) (*BlockGenerator, error) {
	output, err := os.OpenFile(outputFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return nil, err
	}
	defer output.Close()
	db := ethdb.NewMemDatabase()
	genesisBlock, _, tds, err := core.DefaultGenesisBlock().ToBlock(db)
	if err != nil {
		return nil, err
	}
	parent := genesisBlock
	extra := []byte("BlockGenerator")
	forkCoinbaseKey, err := crypto.GenerateKey()
	if err != nil {
		return nil, err
	}
	coinbase := crypto.PubkeyToAddress(base.coinbaseKey.PublicKey)
	forkCoinbase := crypto.PubkeyToAddress(forkCoinbaseKey.PublicKey)
	config := params.MainnetChainConfig
	var pos uint64
	td := new(big.Int)
	bg := &BlockGenerator{
		genesisBlock:        genesisBlock,
		coinbaseKey:         forkCoinbaseKey,
		blockOffsetByHash:   make(map[common.Hash]uint64),
		blockOffsetByNumber: make(map[uint64]uint64),
		headersByHash:       make(map[common.Hash]*types.Header),
		headersByNumber:     make(map[uint64]*types.Header),
	}
	bg.headersByHash[genesisBlock.Header().Hash()] = genesisBlock.Header()
	bg.headersByNumber[0] = genesisBlock.Header()
	for height := 1; height <= forkBase+forkHeight; height++ {
		num := parent.Number()
		tstamp := parent.Time().Int64() + 15
		if height >= forkBase {
			coinbase = forkCoinbase
		}
		header := &types.Header{
			ParentHash: parent.Hash(),
			Number:     num.Add(num, common.Big1),
			GasLimit:   core.CalcGasLimit(parent, 0, 8000000),
			Extra:      extra,
			Time:       big.NewInt(tstamp),
			Coinbase:   coinbase,
			Difficulty: ethash.CalcDifficulty(config, uint64(tstamp), parent.Header()),
		}
		tds.SetBlockNr(parent.NumberU64())
		statedb := state.New(tds)
		tds.StartNewBuffer()
		accumulateRewards(config, statedb, header, []*types.Header{})
		ctx := config.WithEIPsFlags(context.Background(), header.Number)
		if err = statedb.FinalizeTx(ctx, tds.TrieStateWriter()); err != nil {
			return nil, err
		}

		var roots []common.Hash
		roots, err = tds.ComputeTrieRoots()
		if err != nil {
			return nil, err
		}
		header.Root = roots[len(roots)-1]
		err = statedb.CommitBlock(ctx, tds.DbStateWriter())
		if err != nil {
			return nil, err
		}
		// Generate an empty block
		block := types.NewBlock(header, []*types.Transaction{}, []*types.Header{}, []*types.Receipt{})
		fmt.Printf("block hash for %d: %x\n", block.NumberU64(), block.Hash())
		if buffer, err := rlp.EncodeToBytes(block); err != nil {
			return nil, err
		} else {
			output.Write(buffer)
			pos += uint64(len(buffer))
		}
		header = block.Header()
		hash := header.Hash()
		bg.headersByHash[hash] = header
		bg.headersByNumber[block.NumberU64()] = header
		bg.blockOffsetByHash[hash] = pos
		bg.blockOffsetByNumber[block.NumberU64()] = pos
		td = new(big.Int).Add(td, block.Difficulty())
		parent = block
	}
	bg.lastBlock = parent
	bg.totalDifficulty = td
	output.Close()
	// Reopen the file for reading
	bg.input, err = os.Open(outputFile)
	if err != nil {
		return nil, err
	}
	return bg, nil
}

// Some weird constants to avoid constant memory allocs for them.
var (
	big8  = big.NewInt(8)
	big32 = big.NewInt(32)
)

// AccumulateRewards credits the coinbase of the given block with the mining
// reward. The total reward consists of the static block reward and rewards for
// included uncles. The coinbase of each uncle block is also rewarded.
func accumulateRewards(config *params.ChainConfig, state *state.IntraBlockState, header *types.Header, uncles []*types.Header) {
	// Select the correct block reward based on chain progression
	blockReward := ethash.FrontierBlockReward
	if config.IsByzantium(header.Number) {
		blockReward = ethash.ByzantiumBlockReward
	}
	// Accumulate the rewards for the miner and any included uncles
	reward := new(big.Int).Set(blockReward)
	r := new(big.Int)
	for _, uncle := range uncles {
		r.Add(uncle.Number, big8)
		r.Sub(r, header.Number)
		r.Mul(r, blockReward)
		r.Div(r, big8)
		state.AddBalance(uncle.Coinbase, r)

		r.Div(blockReward, big32)
		reward.Add(reward, r)
	}
	state.AddBalance(header.Coinbase, reward)
}
