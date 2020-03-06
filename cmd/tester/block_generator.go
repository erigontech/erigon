package main

import (
	"context"
	"crypto/ecdsa"
	"encoding/binary"
	"fmt"
	"math/big"
	"math/rand"
	"os"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus"
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
	tdByNumber          map[uint64]*big.Int
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

func (bg *BlockGenerator) GetTdByNumber(number uint64) *big.Int {
	return bg.tdByNumber[number]
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

func generateBlock(
	parent *types.Block,
	extra []byte,
	coinbase common.Address,
	chainConfig *params.ChainConfig,
	tds *state.TrieDbState,
	height int,
	r *rand.Rand,
	nonce *uint64,
	amount *big.Int,
	gasPrice *big.Int,
	coinbaseKey *ecdsa.PrivateKey,
	engine consensus.Engine,
) (*types.Block, error) {
	var err error
	num := parent.Number()
	tstamp := parent.Time() + 15
	gasLimit := core.CalcGasLimit(parent, 100000, 8000000)
	header := &types.Header{
		ParentHash: parent.Hash(),
		Number:     num.Add(num, common.Big1),
		GasLimit:   gasLimit,
		Extra:      extra,
		Time:       tstamp,
		Coinbase:   coinbase,
		Difficulty: ethash.CalcDifficulty(chainConfig, tstamp, parent.Header()),
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
		tx := types.NewTransaction(*nonce, to, amount, 21000, gasPrice, []byte{})
		signedTx, err1 := types.SignTx(tx, signer, coinbaseKey)
		if err1 != nil {
			return nil, err1
		}
		signedTxs = append(signedTxs, signedTx)
		receipt, err2 := core.ApplyTransaction(chainConfig, nil, &coinbase, gp, statedb, tds.TrieStateWriter(), header, signedTx, usedGas, vmConfig)
		if err2 != nil {
			return nil, fmt.Errorf("tx %x failed: %v", signedTx.Hash(), err2)
		}
		if !chainConfig.IsByzantium(header.Number) {
			tds.StartNewBuffer()
		}
		receipts = append(receipts, receipt)
		*nonce++
	}

	if _, err = engine.FinalizeAndAssemble(chainConfig, header, statedb, signedTxs, []*types.Header{}, receipts); err != nil {
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
	return block, nil
}

func NewBlockGenerator(outputFile string, initialHeight int) (*BlockGenerator, error) {
	output, err := os.OpenFile(outputFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return nil, err
	}
	defer output.Close()
	db := ethdb.NewMemDatabase()
	genesisBlock, _, tds, err1 := core.DefaultGenesisBlock().ToBlock(db)
	if err1 != nil {
		return nil, err1
	}
	parent := genesisBlock
	extra := []byte("BlockGenerator")
	coinbaseKey, err2 := crypto.HexToECDSA("ad0f3019b6b8634c080b574f3d8a47ef975f0e4b9f63e82893e9a7bb59c2d609")
	if err2 != nil {
		return nil, err2
	}
	fmt.Printf("Generated private key: %x\n", crypto.FromECDSA(coinbaseKey))
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
		tdByNumber:          make(map[uint64]*big.Int),
	}
	bg.headersByHash[genesisBlock.Header().Hash()] = genesisBlock.Header()
	bg.headersByNumber[0] = genesisBlock.Header()
	r := rand.New(rand.NewSource(4589489854))
	var nonce uint64        // nonce of the sender (coinbase)
	amount := big.NewInt(1) // 1 wei
	gasPrice := big.NewInt(10000000)
	engine := ethash.NewFullFaker()

	for height := 1; height <= initialHeight; height++ {
		block, err3 := generateBlock(parent, extra, coinbase, chainConfig, tds, height, r, &nonce, amount, gasPrice, coinbaseKey, engine)
		if err3 != nil {
			return nil, err3
		}
		buffer, err4 := rlp.EncodeToBytes(block)
		if err4 != nil {
			return nil, err4
		}
		if _, err5 := output.Write(buffer); err5 != nil {
			return nil, err5
		}
		header := block.Header()
		hash := header.Hash()
		bg.headersByHash[hash] = header
		bg.headersByNumber[block.NumberU64()] = header
		bg.blockOffsetByHash[hash] = pos
		bg.blockOffsetByNumber[block.NumberU64()] = pos
		pos += uint64(len(buffer))
		td = new(big.Int).Add(td, block.Difficulty())
		bg.tdByNumber[block.NumberU64()] = td
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

// NewForkGenerator Creates a fork from the existing block generator
func NewForkGenerator(base *BlockGenerator, outputFile string, forkBase uint64, forkHeight uint64) (*BlockGenerator, error) {
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
	forkCoinbaseKey, err := crypto.HexToECDSA("048d914460c9b5feb28d79df9c89a44465b1c9c0fa97d4bda32ede37fc178b8d")
	if err != nil {
		return nil, err
	}
	coinbase := crypto.PubkeyToAddress(base.coinbaseKey.PublicKey)
	forkCoinbase := crypto.PubkeyToAddress(forkCoinbaseKey.PublicKey)
	chainConfig := params.MainnetChainConfig
	var pos uint64
	td := new(big.Int)
	bg := &BlockGenerator{
		genesisBlock:        genesisBlock,
		coinbaseKey:         forkCoinbaseKey,
		blockOffsetByHash:   make(map[common.Hash]uint64),
		blockOffsetByNumber: make(map[uint64]uint64),
		headersByHash:       make(map[common.Hash]*types.Header),
		headersByNumber:     make(map[uint64]*types.Header),
		tdByNumber:          make(map[uint64]*big.Int),
	}
	bg.headersByHash[genesisBlock.Header().Hash()] = genesisBlock.Header()
	bg.headersByNumber[0] = genesisBlock.Header()
	r := rand.New(rand.NewSource(4589489854))
	var nonce uint64        // nonce of the sender (coinbase)
	amount := big.NewInt(1) // 1 wei
	gasPrice := big.NewInt(10000000)
	engine := ethash.NewFullFaker()
	for height := 1; height <= int(forkBase+forkHeight); height++ {
		if height >= int(forkBase) {
			coinbase = forkCoinbase
		}
		block, err1 := generateBlock(parent, extra, coinbase, chainConfig, tds, height, r, &nonce, amount, gasPrice, base.coinbaseKey, engine)
		if err1 != nil {
			return nil, err1
		}
		buffer, err2 := rlp.EncodeToBytes(block)
		if err2 != nil {
			return nil, err2
		}
		if _, err3 := output.Write(buffer); err3 != nil {
			return nil, err3
		}
		header := block.Header()
		hash := header.Hash()
		bg.headersByHash[hash] = header
		bg.headersByNumber[block.NumberU64()] = header
		bg.blockOffsetByHash[hash] = pos
		bg.blockOffsetByNumber[block.NumberU64()] = pos
		pos += uint64(len(buffer))
		td = new(big.Int).Add(td, block.Difficulty())
		bg.tdByNumber[block.NumberU64()] = td
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
