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
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/forkid"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

const ReduceComplexity = false

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
	forkId              forkid.ID
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
	if _, err := bg.input.Seek(int64(offset), 0); err != nil {
		return nil, err
	}
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

func (bg *BlockGenerator) ForkID() forkid.ID {
	return bg.forkId
}

func randAddress(r *rand.Rand) common.Address {
	if ReduceComplexity {
		return common.BytesToAddress(common.FromHex("80977316944e5942e79b0e3abad38da746086519"))
	}
	var b common.Address
	binary.BigEndian.PutUint64(b[:], r.Uint64())
	binary.BigEndian.PutUint64(b[8:], r.Uint64())
	binary.BigEndian.PutUint32(b[16:], r.Uint32())
	return b
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
	txOpts := bind.NewKeyedTransactor(coinbaseKey)
	txOpts.GasPrice = gasPrice
	txOpts.GasLimit = params.TxGas

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
	//var nonce uint64 // nonce of the sender (coinbase)
	txOpts.Nonce = big.NewInt(0)

	backend := &NoopBackend{db: db, genesis: genesis}
	genBlock := func(i int, gen *core.BlockGen) {
		select {
		case <-ctx.Done():
			panic(ctx.Err())
		default:
		}

		gen.SetExtra(extra)
		gen.SetCoinbase(coinbase)
		if gen.GetHeader().GasLimit <= 15*params.TxGasContractCreation {
			return
		}
		gen.SetNonce(types.EncodeNonce(txOpts.Nonce.Uint64()))
		signer := types.MakeSigner(genesis.Config, txOpts.Nonce)
		var tx *types.Transaction

		txOpts.GasLimit = 3 * params.TxGasContractCreation
		//fmt.Println("Alex")
		//time.Sleep(5 * time.Second)
		switch true {
		case txOpts.Nonce.Uint64() == 1: // create 0 account
			tx = types.NewTransaction(txOpts.Nonce.Uint64(), common.HexToAddress("0000000000000000000000000000000000000000"), amount, params.TxGas, nil, nil)
			signedTx, err1 := types.SignTx(tx, signer, coinbaseKey)
			if err1 != nil {
				panic(err1)
			}
			gen.AddTx(signedTx)
		case txOpts.Nonce.Uint64() == 2: // deploy factory
			reviveAddress, tx, revive, err = contracts.DeployRevive2(txOpts, backend)
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
		case txOpts.Nonce.Uint64() == 3: // call .deploy() method on factory
			tx, err = revive.Deploy(txOpts, [32]byte{})
			if err != nil {
				panic(err)
			}
			gen.AddTx(tx)
		case txOpts.Nonce.Uint64() >= 4 && txOpts.Nonce.Uint64() <= 10000: // gen big storage
			tx, err = phoenix.Store(txOpts)
			if err != nil {
				panic(err)
			}
			gen.AddTx(tx)
		case txOpts.Nonce.Uint64() == 10001: // kill contract with big storage
			tx, err = phoenix.Die(txOpts)
			if err != nil {
				panic(err)
			}
			gen.AddTx(tx)
		case txOpts.Nonce.Uint64() == 10002: // revive Phoenix and add to it some storage in same Tx
			tx, err = revive.Deploy(txOpts, [32]byte{})
			if err != nil {
				panic(err)
			}
			gen.AddTx(tx)
			txOpts.Nonce.Add(txOpts.Nonce, common.Big1)
			tx, err = phoenix.Store(txOpts)
			if err != nil {
				panic(err)
			}
			gen.AddTx(tx)
			txOpts.Nonce.Add(txOpts.Nonce, common.Big1)
			tx, err = phoenix.Store(txOpts)
			if err != nil {
				panic(err)
			}
			gen.AddTx(tx)
		case txOpts.Nonce.Uint64() == 10003: // add some storage and kill Phoenix in same Tx
			tx, err = phoenix.Store(txOpts)
			if err != nil {
				panic(err)
			}
			gen.AddTx(tx)
			txOpts.Nonce.Add(txOpts.Nonce, common.Big1)
			tx, err = phoenix.Store(txOpts)
			if err != nil {
				panic(err)
			}
			gen.AddTx(tx)
			txOpts.Nonce.Add(txOpts.Nonce, common.Big1)
			tx, err = phoenix.Die(txOpts)
			if err != nil {
				panic(err)
			}
			gen.AddTx(tx)
			txOpts.Nonce.Add(txOpts.Nonce, common.Big1)
			tx, err = revive.Deploy(txOpts, [32]byte{})
			if err != nil {
				panic(err)
			}
			gen.AddTx(tx)
			txOpts.Nonce.Add(txOpts.Nonce, common.Big1)
			tx, err = phoenix.Store(txOpts)
			if err != nil {
				panic(err)
			}
			gen.AddTx(tx)
		default:
			to := randAddress(r)
			tx = types.NewTransaction(txOpts.Nonce.Uint64(), to, amount, params.TxGas, nil, nil)
			signedTx, err1 := types.SignTx(tx, signer, coinbaseKey)
			if err1 != nil {
				panic(err1)
			}
			gen.AddTx(signedTx)
		}
		txOpts.Nonce.Add(txOpts.Nonce, common.Big1)
	}

	blocks := make(chan *types.Block, 10000)
	go func() {
		defer close(blocks)
		parent := genesisBlock
		n := 1000
		//if ReduceComplexity {
		//	n = 1 // 1 block per transaction
		//}
		now := time.Now()
		for height, stop := n, false; !stop; height += n {
			if height > initialHeight {
				n = initialHeight + n - height
				stop = true
				if n == 0 {
					break
				}
			}

			// Generate a batch of blocks, each properly signed
			blocksSlice, _ := core.GenerateChain(ctx, genesis.Config, parent, engine, db, n, genBlock)
			parent = blocksSlice[len(blocksSlice)-1]
			for _, block := range blocksSlice {
				blocks <- block
			}
			if height%10000 == 0 {
				log.Info(fmt.Sprintf("block gen %dK, %s", height/1000, time.Since(now)))
			}
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

	blockchain, err := core.NewBlockChain(db, nil, genesis.Config, ethash.NewFullFaker(), vm.Config{}, nil)
	if err != nil {
		return nil, err
	}
	bg.forkId = forkid.NewID(blockchain)

	if err := output.Flush(); err != nil {
		return nil, err
	}
	if err := outputF.Close(); err != nil {
		return nil, err
	}

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
	genesisBlock := genesis.MustCommit(db)
	extra := []byte("BlockGenerator")
	coinbaseKey := base.coinbaseKey
	coinbase := crypto.PubkeyToAddress(coinbaseKey.PublicKey)
	forkCoinbaseKey, err := crypto.HexToECDSA("048d914460c9b5feb28d79df9c89a44465b1c9c0fa97d4bda32ede37fc178b8d")
	if err != nil {
		return nil, err
	}
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

	txOpts := bind.NewKeyedTransactor(coinbaseKey)
	txOpts.GasPrice = gasPrice
	txOpts.GasLimit = params.TxGas
	txOpts.Nonce = big.NewInt(0)

	//backend := &NoopBackend{db: db, genesis: genesis}
	genBlock := func(i int, gen *core.BlockGen) {
		select {
		case <-ctx.Done():
			panic(ctx.Err())
		default:
		}

		gen.SetExtra(extra)
		gen.SetCoinbase(coinbase)
		if gen.GetHeader().GasLimit <= 15*params.TxGasContractCreation {
			return
		}
		gen.SetNonce(types.EncodeNonce(txOpts.Nonce.Uint64()))
		signer := types.MakeSigner(genesis.Config, txOpts.Nonce)
		var tx *types.Transaction

		txOpts.GasLimit = 3 * params.TxGasContractCreation
		switch true {
		default:
			to := randAddress(r)
			tx = types.NewTransaction(txOpts.Nonce.Uint64(), to, amount, params.TxGas, gasPrice, nil)
			signedTx, err1 := types.SignTx(tx, signer, coinbaseKey)
			if err1 != nil {
				panic(err1)
			}
			gen.AddTx(signedTx)
		}
		txOpts.Nonce.Add(txOpts.Nonce, common.Big1)
	}

	blocks := make(chan *types.Block, 10000)
	go func() {
		defer close(blocks)
		parent := genesisBlock
		n := 1000
		//if ReduceComplexity {
		//	n = 1 // 1 block per transaction
		//}
		for height, stop := n, false; !stop; height += n {
			if height > int(forkBase) {
				n = int(forkBase) + n - height
				stop = true
				if n == 0 {
					break
				}
			}

			blocksSlice, _ := core.GenerateChain(ctx, genesis.Config, parent, engine, db, n, genBlock)
			parent = blocksSlice[len(blocksSlice)-1]
			for _, block := range blocksSlice {
				blocks <- block
			}
			if height%10000 == 0 {
				log.Info(fmt.Sprintf("fork gen %dK", (height+1)/1000))
			}
		}

		n = 1000
		coinbase = forkCoinbase
		for height, stop := n, false; !stop; height += n {
			if height > int(forkHeight) {
				n = int(forkHeight) + n - height
				stop = true
				if n == 0 {
					break
				}
			}

			// Generate a batch of blocks, each properly signed
			blocksSlice, _ := core.GenerateChain(ctx, genesis.Config, parent, engine, db, n, genBlock)
			parent = blocksSlice[len(blocksSlice)-1]
			for _, block := range blocksSlice {
				blocks <- block
			}
			if height%10000 == 0 {
				log.Info(fmt.Sprintf("fork reorg gen %dK", (height+1)/1000))
			}
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

	blockchain, err := core.NewBlockChain(db, nil, genesis.Config, ethash.NewFullFaker(), vm.Config{}, nil)
	if err != nil {
		return nil, err
	}
	bg.forkId = forkid.NewID(blockchain)

	if err := output.Flush(); err != nil {
		return nil, err
	}
	if err := outputF.Close(); err != nil {
		return nil, err
	}

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

	if ReduceComplexity {
		a := common.BytesToAddress(common.FromHex("80977316944e5942e79b0e3abad38da746086519"))
		genesis.Alloc = core.GenesisAlloc{
			a: genesis.Alloc[a],
		}
	}
	return genesis
}
