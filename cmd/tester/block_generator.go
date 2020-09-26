package main

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"encoding/binary"
	"math/big"
	"math/rand"
	"os"

	"github.com/holiman/uint256"

	ethereum "github.com/ledgerwatch/turbo-geth"
	"github.com/ledgerwatch/turbo-geth/accounts/abi/bind"
	"github.com/ledgerwatch/turbo-geth/cmd/tester/contracts"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
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
	coinbaseNonce       uint64 // Keep track of this to be able to create more transactions
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

func (bg *BlockGenerator) Genesis() *types.Block {
	return bg.genesisBlock
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
	receipt, _, _, _ := rawdb.ReadReceipt(b.db, txHash)
	return receipt, nil
}

func (*NoopBackend) FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	panic("must not be called")
}
func (*NoopBackend) SubscribeFilterLogs(ctx context.Context, query ethereum.FilterQuery, ch chan<- types.Log) (ethereum.Subscription, error) {
	panic("must not be called")
}

func makeGenBlock(db ethdb.Database,
	genesis *core.Genesis,
	extra []byte,
	coinbaseKey *ecdsa.PrivateKey,
	isFork bool,
	forkBase, forkHeight uint64,
	r *rand.Rand,
	nonce *int64,
) func(coinbase common.Address, i int, gen *core.BlockGen) {
	var err error
	amount := uint256.NewInt().SetUint64(1) // 1 wei

	*nonce = -1
	txOpts := bind.NewKeyedTransactor(coinbaseKey)
	txOpts.GasPrice = big.NewInt(1)
	txOpts.GasLimit = 20 * params.TxGas
	txOpts.Nonce = big.NewInt(0) // nonce of the sender (coinbase)

	var revive *contracts.Revive2
	var phoenix *contracts.Phoenix
	var reviveAddress common.Address
	var phoenixAddress common.Address
	backend := &NoopBackend{db: db, genesis: genesis}

	var store = func() *types.Transaction {
		*nonce++
		txOpts.Nonce.SetInt64(*nonce)
		tx, err := phoenix.Store(txOpts)
		if err != nil {
			panic(err)
		}
		return tx
	}

	var incr = func() *types.Transaction {
		*nonce++
		txOpts.Nonce.SetInt64(*nonce)
		tx, err := phoenix.Increment(txOpts)
		if err != nil {
			panic(err)
		}
		return tx
	}

	var deploy = func() *types.Transaction {
		*nonce++
		txOpts.Nonce.SetInt64(*nonce)
		tx, err := revive.Deploy(txOpts, [32]byte{})
		if err != nil {
			panic(err)
		}
		return tx
	}

	var die = func() *types.Transaction {
		*nonce++
		txOpts.Nonce.SetInt64(*nonce)
		tx, err := phoenix.Die(txOpts)
		if err != nil {
			panic(err)
		}
		return tx
	}

	return func(coinbase common.Address, i int, gen *core.BlockGen) {
		blockNr := gen.Number().Uint64()
		gasPrice, _ := uint256.FromBig(txOpts.GasPrice)

		gen.SetCoinbase(coinbase)
		if gen.GetHeader().GasLimit <= 40*params.TxGas { // ~700 blocks
			return
		}
		gen.SetExtra(extra)
		gen.SetNonce(types.EncodeNonce(txOpts.Nonce.Uint64()))
		var tx *types.Transaction
		switch {
		case blockNr == 10001: // create 0 account
			*nonce++
			txOpts.Nonce.SetInt64(*nonce)
			account0 := common.HexToAddress("0000000000000000000000000000000000000000")
			tx = types.NewTransaction(txOpts.Nonce.Uint64(), account0, amount, params.TxGas, gasPrice, nil)
			signer := types.MakeSigner(genesis.Config, txOpts.Nonce)
			signedTx, err1 := types.SignTx(tx, signer, coinbaseKey)
			if err1 != nil {
				panic(err1)
			}
			gen.AddTx(signedTx)
		case blockNr == 10002: // deploy factory
			*nonce++
			txOpts.Nonce.SetInt64(*nonce)
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
		case blockNr == 10003: // call .deploy() method on factory
			gen.AddTx(deploy())
		case i >= 10004 && i <= 20000: // gen big storage
			gen.AddTx(store())
		case blockNr == 20001: // kill contract with big storage
			gen.AddTx(die())
		case blockNr == forkBase-1: // revive Phoenix and add to it some storage in same Tx
			gen.AddTx(deploy())
			gen.AddTx(store())
			gen.AddTx(incr()) // last increment, set last value to 2
		case !isFork && blockNr == forkBase+1:
			gen.AddTx(die())
			gen.AddTx(deploy())
			gen.AddTx(store())
		case isFork && blockNr == forkBase+1:
			// skip self-destruct, deploy and store steps
			// it means in fork we will have value=2, while in non-fork value=1
		default:
			*nonce++
			txOpts.Nonce.SetInt64(*nonce)
			to := randAddress(r)
			tx = types.NewTransaction(txOpts.Nonce.Uint64(), to, amount, params.TxGas, gasPrice, nil)
			signer := types.MakeSigner(genesis.Config, txOpts.Nonce)
			signedTx, err1 := types.SignTx(tx, signer, coinbaseKey)
			if err1 != nil {
				panic(err1)
			}
			//fmt.Printf("AddTx to block %d\n", i)
			gen.AddTx(signedTx)
		}
	}
}

func (bg *BlockGenerator) blocksToFile(outputFile string, blocks []*types.Block) error {
	outputF, err := os.OpenFile(outputFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return err
	}
	defer outputF.Close()
	output := bufio.NewWriterSize(outputF, 128*1024)
	defer output.Flush()

	var parent *types.Block
	var pos uint64
	td := new(big.Int)
	for _, block := range blocks {
		buffer, err2 := rlp.EncodeToBytes(block)
		if err2 != nil {
			return err2
		}
		if _, err := output.Write(buffer); err != nil {
			return err
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

	return nil
}

func NewBlockGenerator(ctx context.Context, outputFile string, initialHeight int) (*BlockGenerator, error) {
	log.Info("Generating blocks...")
	db, genesis, extra, engine := ethdb.NewMemDatabase(), genesis(), []byte("BlockGenerator"), ethash.NewFullFaker()
	r := rand.New(rand.NewSource(4589489854))

	genesisBlock := genesis.MustCommit(db)
	coinbaseKey, err2 := crypto.HexToECDSA("ad0f3019b6b8634c080b574f3d8a47ef975f0e4b9f63e82893e9a7bb59c2d609")
	if err2 != nil {
		return nil, err2
	}
	coinbase := crypto.PubkeyToAddress(coinbaseKey.PublicKey)

	var nonce int64
	genBlockFunc := makeGenBlock(db, genesis, extra, coinbaseKey, false, 0, 0, r, &nonce)
	genBlock := func(i int, gen *core.BlockGen) {
		genBlockFunc(coinbase, i, gen)
	}

	blocks, _, err := core.GenerateChain(genesis.Config, genesisBlock, engine, db, initialHeight, genBlock, true /* intermediateHashes */)
	if err != nil {
		panic(err)
	}

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

	if err := bg.blocksToFile(outputFile, blocks); err != nil {
		return nil, err
	}

	bg.input, err = os.Open(outputFile)
	// Reopen the file for reading
	if err != nil {
		return nil, err
	}
	bg.coinbaseNonce = uint64(nonce)
	log.Info("Blocks generated")
	return bg, nil
}

// NewForkGenerator Creates a fork from the existing block generator
func NewForkGenerator(ctx context.Context, base *BlockGenerator, outputFile string, forkBase uint64, forkHeight uint64) (*BlockGenerator, error) {
	log.Info("Generating fork...")
	db, genesis, extra, engine := ethdb.NewMemDatabase(), genesis(), []byte("BlockGenerator"), ethash.NewFullFaker()
	r := rand.New(rand.NewSource(4589489854))

	genesisBlock := genesis.MustCommit(db)

	coinbase, coinbaseKey := crypto.PubkeyToAddress(base.coinbaseKey.PublicKey), base.coinbaseKey
	forkCoinbaseKey, err := crypto.HexToECDSA("048d914460c9b5feb28d79df9c89a44465b1c9c0fa97d4bda32ede37fc178b8d")
	if err != nil {
		return nil, err
	}
	forkCoinbase := crypto.PubkeyToAddress(forkCoinbaseKey.PublicKey)

	var nonce int64
	genBlockFunc := makeGenBlock(db, genesis, extra, coinbaseKey, true, forkBase, forkHeight, r, &nonce)
	genBlock := func(i int, gen *core.BlockGen) {
		if gen.Number().Uint64() >= forkBase {
			coinbase = forkCoinbase
		}

		genBlockFunc(coinbase, i, gen)
	}

	blocks, _, err1 := core.GenerateChain(genesis.Config, genesisBlock, engine, db, int(forkBase+forkHeight), genBlock, true /* intermediateHashes */)
	if err1 != nil {
		panic(err1)
	}
	log.Info("fork gen", "height", forkHeight)

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

	if err := bg.blocksToFile(outputFile, blocks); err != nil {
		return nil, err
	}

	bg.input, err = os.Open(outputFile)
	if err != nil {
		return nil, err
	}
	bg.coinbaseNonce = uint64(nonce)
	log.Info("Fork generated")
	return bg, nil
}

func (bg *BlockGenerator) GenerateTx() (*types.Transaction, error) {
	account0 := common.HexToAddress("0000000000000000000000000000000000000000")
	amount := uint256.NewInt().SetUint64(1)       // 1 wei
	gasPrice := uint256.NewInt().SetUint64(10000) // 1 wei
	tx := types.NewTransaction(bg.coinbaseNonce, account0, amount, params.TxGas, gasPrice, nil)
	signer := types.MakeSigner(genesis().Config, big.NewInt(int64(bg.coinbaseNonce)))
	signedTx, err1 := types.SignTx(tx, signer, bg.coinbaseKey)
	return signedTx, err1
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
