package main

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"encoding/binary"
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"time"

	ethereum "github.com/ledgerwatch/turbo-geth"
	"github.com/ledgerwatch/turbo-geth/accounts/abi/bind"
	"github.com/ledgerwatch/turbo-geth/cmd/tester/contracts"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
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
	if bg.input != nil {
		bg.input.Close()
	}
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
	nonce *uint64,
	coinbaseKey *ecdsa.PrivateKey,
	engine consensus.Engine,
	txGen func(ctx context.Context, i int, nonce uint64, gp *core.GasPool, statedb *state.IntraBlockState) *types.Transaction,
) (*types.Block, error) {
	var err error
	num := parent.Number()
	tstamp := parent.Time() + 15
	gasLimit := core.CalcGasLimit(parent, 200000, 8000000)
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

	if height > 1 && gasLimit >= params.TxGas {
		signer := types.MakeSigner(chainConfig, big.NewInt(int64(height)))
		gp := new(core.GasPool).AddGas(header.GasLimit)
		vmConfig := vm.Config{}

		tx := txGen(context.TODO(), height, *nonce, gp, statedb)
		signedTx, err1 := types.SignTx(tx, signer, coinbaseKey)
		if err1 != nil {
			return nil, fmt.Errorf("SignTx: %w", err1)
		}

		signedTxs = append(signedTxs, signedTx)
		receipt, err2 := core.ApplyTransaction(chainConfig, nil, &coinbase, gp, statedb, tds.TrieStateWriter(), header, signedTx, usedGas, vmConfig)
		if err2 != nil {
			return nil, fmt.Errorf("ApplyTransaction: tx %x, height: %d, gasPool: %d, usedGas: %d, failed: %v", signedTx.Hash(), height, gp, *usedGas, err2)
		}
		if !chainConfig.IsByzantium(header.Number) {
			tds.StartNewBuffer()
		}
		receipts = append(receipts, receipt)
		*nonce++
	}

	if _, err = engine.FinalizeAndAssemble(chainConfig, header, statedb, signedTxs, []*types.Header{}, receipts); err != nil {
		return nil, fmt.Errorf("FinalizeAndAssemble: %w", err)
	}
	ctx := chainConfig.WithEIPsFlags(context.Background(), header.Number)
	if err = statedb.FinalizeTx(ctx, tds.TrieStateWriter()); err != nil {
		return nil, fmt.Errorf("FinalizeTx: %w", err)
	}

	var roots []common.Hash
	roots, err = tds.ComputeTrieRoots()
	if err != nil {
		return nil, fmt.Errorf("ComputeTrieRoots: %w", err)
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
		return nil, fmt.Errorf("CommitBlock: %w", err)
	}
	// Generate an empty block
	block := types.NewBlock(header, signedTxs, []*types.Header{}, receipts)
	return block, nil
}

type NoopBackend struct {
	db      ethdb.Database
	genesis *core.Genesis
}

func (*NoopBackend) CallContract(ctx context.Context, call ethereum.CallMsg, blockNumber *big.Int) ([]byte, error) {
	panic("must not be called")
}
func (*NoopBackend) CodeAt(ctx context.Context, contract common.Address, blockNumber *big.Int) ([]byte, error) {
	panic("must not be called")
}

func (*NoopBackend) PendingCodeAt(ctx context.Context, account common.Address) ([]byte, error) {
	panic("must not be called")
}
func (*NoopBackend) PendingNonceAt(ctx context.Context, account common.Address) (uint64, error) {
	panic("must not be called")
}
func (*NoopBackend) SuggestGasPrice(ctx context.Context) (*big.Int, error) {
	panic("must not be called")
}
func (*NoopBackend) EstimateGas(ctx context.Context, call ethereum.CallMsg) (gas uint64, err error) {
	panic("must not be called")
}
func (*NoopBackend) SendTransaction(ctx context.Context, tx *types.Transaction) error {
	return nil // nothing to do
}

func (b *NoopBackend) TransactionReceipt(ctx context.Context, txHash common.Hash) (*types.Receipt, error) {
	receipt, _, _, _ := rawdb.ReadReceipt(b.db, txHash, b.genesis.Config)
	return receipt, nil
}

func (*NoopBackend) FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	panic("must not be called")
}
func (*NoopBackend) SubscribeFilterLogs(ctx context.Context, query ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error) {
	panic("must not be called")
}

func NewBlockGenerator(ctx context.Context, outputFile string, initialHeight int) (*BlockGenerator, error) {
	outputF, err := os.OpenFile(outputFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return nil, err
	}
	defer outputF.Close()
	output := bufio.NewWriterSize(outputF, 128*1024)

	db := ethdb.NewMemDatabase()
	genesis := genesis()
	genesisBlock := genesis.MustCommit(db)
	extra := []byte("BlockGenerator")
	coinbaseKey, err2 := crypto.HexToECDSA("ad0f3019b6b8634c080b574f3d8a47ef975f0e4b9f63e82893e9a7bb59c2d609")
	if err2 != nil {
		return nil, err2
	}
	log.Debug(fmt.Sprintf("Generated private key: %x\n", crypto.FromECDSA(coinbaseKey)))
	coinbase := crypto.PubkeyToAddress(coinbaseKey.PublicKey)

	gasPrice := big.NewInt(1)
	transactOpts := bind.NewKeyedTransactor(coinbaseKey)
	transactOpts.GasPrice = gasPrice
	transactOpts.GasLimit = params.TxGas

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
	amount := big.NewInt(1) // 1 wei
	engine := ethash.NewFullFaker()

	var revive *contracts.Revive2
	var phoenix *contracts.Phoenix
	var reviveAddress common.Address
	var phoenixAddress common.Address
	var nonce uint64 // nonce of the sender (coinbase)

	backend := &NoopBackend{db: db, genesis: genesis}
	genBlock := func(i int, gen *core.BlockGen) {
		select {
		case <-ctx.Done():
			panic(ctx.Err())
		default:
		}

		gen.SetExtra(extra)
		gen.SetCoinbase(coinbase)
		if gen.GetHeader().GasLimit <= params.TxGas {
			return
		}
		gen.SetNonce(types.EncodeNonce(nonce))

		signer := types.MakeSigner(genesis.Config, big.NewInt(int64(nonce)))
		var tx *types.Transaction

		switch nonce {
		case 35001: // deploy factory
			transactOpts.GasLimit = 3 * params.TxGasContractCreation
			transactOpts.Nonce = big.NewInt(int64(nonce))
			reviveAddress, tx, revive, err = contracts.DeployRevive2(transactOpts, backend)
			if err != nil {
				panic(err)
			}

			codeHash, err := common.HashData(common.FromHex(contracts.PhoenixBin))
			if err != nil {
				panic(err)
			}

			phoenixAddress = crypto.CreateAddress2(reviveAddress, [32]byte{}, codeHash.Bytes())
			phoenix, err = contracts.NewPhoenix(phoenixAddress, backend)
			if err != nil {
				panic(err)
			}
			gen.AddTx(tx)
			time.Sleep(10 * time.Second)
		case 36002: // call .deploy() method on factory
			transactOpts.GasLimit = 3 * params.TxGasContractCreation
			transactOpts.Nonce = big.NewInt(int64(nonce))
			tx, err = revive.Deploy(transactOpts, [32]byte{})
			if err != nil {
				panic(err)
			}
			gen.AddTx(tx)
			time.Sleep(10 * time.Second)
		case 37003:
			transactOpts.GasLimit = 3 * params.TxGasContractCreation
			transactOpts.Nonce = big.NewInt(int64(nonce))
			tx, err = phoenix.Store(transactOpts)
			if err != nil {
				panic(err)
			}
			gen.AddTx(tx)
			time.Sleep(10 * time.Second)
		case 38004:
			transactOpts.GasLimit = 3 * params.TxGasContractCreation
			transactOpts.Nonce = big.NewInt(int64(nonce))
			tx, err = phoenix.Die(transactOpts)
			if err != nil {
				panic(err)
			}
			gen.AddTx(tx)
			if len(gen.GetReceipts()) > 0 {
				for _, r := range gen.GetReceipts() {
					for _, l := range r.Logs {
						fmt.Printf("block_generator.go:341 Logs! : %d %x \n", l.BlockNumber, l.Data)
					}
				}
			}
			time.Sleep(10 * time.Second)
		//case 39005:
		//	transactOpts.GasLimit = 3 * params.TxGasContractCreation
		//	transactOpts.Nonce = big.NewInt(int64(nonce))
		//	tx, err = revive.Deploy(transactOpts, [32]byte{})
		//	if err != nil {
		//		panic(err)
		//	}
		//	gen.AddTx(tx)
		default:
			to := randAddress(r)
			tx = types.NewTransaction(nonce, to, amount, params.TxGas, nil, nil)
			signedTx, err1 := types.SignTx(tx, signer, coinbaseKey)
			if err1 != nil {
				panic(err1)
			}
			gen.AddTx(signedTx)
		}
		nonce++
	}

	blocks := make(chan *types.Block, 1000)
	go func() {
		defer close(blocks)
		height := initialHeight
		parent := genesisBlock
		i := 0
		n := 1000
		for height > 0 {
			if height < 1000 {
				n = height
			}

			// Generate a batch of blocks, each properly signed
			blocksSlice, _ := core.GenerateChain(ctx, genesis.Config, parent, engine, db, n, genBlock)
			for _, block := range blocksSlice {
				blocks <- block
				parent = block
			}
			height -= 1000
			i++
			log.Info(fmt.Sprintf("block gen %dK", i))
		}
	}()

	var parent *types.Block
	for block := range blocks {
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

	// Reopen the file for reading
	bg.input, err = os.Open(outputFile)
	if err != nil {
		return nil, err
	}
	return bg, nil
}

// NewForkGenerator Creates a fork from the existing block generator
func NewForkGenerator(ctx context.Context, base *BlockGenerator, outputFile string, forkBase uint64, forkHeight uint64) (*BlockGenerator, error) {
	outputF, err := os.OpenFile(outputFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return nil, err
	}
	defer outputF.Close()
	output := bufio.NewWriterSize(outputF, 128*1024)
	db := ethdb.NewMemDatabase()
	genesis := genesis()

	genesisBlock, _, tds, err := genesis.ToBlock(db)
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
	amount := big.NewInt(1) // 1 wei
	gasPrice := big.NewInt(10000000)
	engine := ethash.NewFullFaker()
	txGen := func(ctx context.Context, i int, nonce uint64, gp *core.GasPool, statedb *state.IntraBlockState) *types.Transaction {
		var tx *types.Transaction
		switch i {
		default:
			to := randAddress(r)
			tx = types.NewTransaction(nonce, to, amount, params.TxGas, gasPrice, []byte{})
		}
		return tx
	}

	var nonce uint64 // nonce of the sender (coinbase)
	for height := 1; height <= int(forkBase+forkHeight); height++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if height >= int(forkBase) {
			coinbase = forkCoinbase
		}
		block, err1 := generateBlock(parent, extra, coinbase, genesis.Config, tds, height, &nonce, base.coinbaseKey, engine, txGen)
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

		if height%1000 == 0 {
			log.Info(fmt.Sprintf("fork gen %dK", height/1000))
		}
	}
	bg.lastBlock = parent
	bg.totalDifficulty = td
	// Reopen the file for reading
	bg.input, err = os.Open(outputFile)
	if err != nil {
		return nil, err
	}
	return bg, nil
}

func genesis() *core.Genesis {
	genesis := core.DefaultGenesisBlock()
	genesis.Config.HomesteadBlock = big.NewInt(0)
	genesis.Config.DAOForkBlock = nil
	genesis.Config.DAOForkSupport = true
	genesis.Config.EIP150Block = big.NewInt(0)
	genesis.Config.EIP150Hash = common.HexToHash("0x41941023680923e0fe4d74a34bdac8141f2540e3ae90623718e47d66d1ca4a2d")
	genesis.Config.EIP155Block = big.NewInt(10)
	genesis.Config.EIP158Block = big.NewInt(10)
	genesis.Config.ByzantiumBlock = big.NewInt(17)
	genesis.Config.ConstantinopleBlock = big.NewInt(18)
	genesis.Config.PetersburgBlock = big.NewInt(19)
	genesis.Config.IstanbulBlock = big.NewInt(20)
	genesis.Config.MuirGlacierBlock = big.NewInt(21)
	return genesis
}
