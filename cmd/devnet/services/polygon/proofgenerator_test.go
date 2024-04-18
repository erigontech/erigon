package polygon

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"fmt"
	"math"
	"math/big"
	"sync"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/log/v3"
	"github.com/pion/randutil"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/accounts/abi/bind"
	"github.com/ledgerwatch/erigon/cmd/devnet/blocks"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/polygon/bor"
	"github.com/ledgerwatch/erigon/polygon/bor/valset"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/jsonrpc"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/stages/mock"
	"github.com/ledgerwatch/erigon/turbo/transactions"
)

type requestGenerator struct {
	sync.Mutex
	requests.NopRequestGenerator
	sentry     *mock.MockSentry
	bor        *bor.Bor
	chain      *core.ChainPack
	txBlockMap map[libcommon.Hash]*types.Block
}

func newRequestGenerator(sentry *mock.MockSentry, chain *core.ChainPack) (*requestGenerator, error) {
	db := memdb.New("")

	if err := db.Update(context.Background(), func(tx kv.RwTx) error {
		if err := rawdb.WriteHeader(tx, chain.TopBlock.Header()); err != nil {
			return err
		}
		if err := rawdb.WriteHeadHeaderHash(tx, chain.TopBlock.Header().Hash()); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	reader := blockReader{
		chain: chain,
	}

	return &requestGenerator{
		chain:  chain,
		sentry: sentry,
		bor: bor.NewRo(params.BorDevnetChainConfig, db, reader,
			&spanner{
				bor.NewChainSpanner(bor.GenesisContractValidatorSetABI(), params.BorDevnetChainConfig, false, log.Root()),
				libcommon.Address{},
				heimdall.Span{}},
			genesisContract{}, log.Root()),
		txBlockMap: map[libcommon.Hash]*types.Block{},
	}, nil
}

func (rg *requestGenerator) GetRootHash(ctx context.Context, startBlock uint64, endBlock uint64) (libcommon.Hash, error) {
	tx, err := rg.bor.DB.BeginRo(context.Background())
	if err != nil {
		return libcommon.Hash{}, err
	}
	defer tx.Rollback()

	result, err := rg.bor.GetRootHash(ctx, tx, startBlock, endBlock)

	if err != nil {
		return libcommon.Hash{}, err
	}

	return libcommon.HexToHash(result), nil
}

func (rg *requestGenerator) GetBlockByNumber(ctx context.Context, blockNum rpc.BlockNumber, withTxs bool) (*requests.Block, error) {
	if bn := int(blockNum.Uint64()); bn < len(rg.chain.Blocks) {
		block := rg.chain.Blocks[bn]

		transactions := make([]*jsonrpc.RPCTransaction, len(block.Transactions()))

		for i, tx := range block.Transactions() {
			rg.txBlockMap[tx.Hash()] = block
			transactions[i] = jsonrpc.NewRPCTransaction(tx, block.Hash(), blockNum.Uint64(), uint64(i), block.BaseFee())
		}

		return &requests.Block{
			BlockWithTxHashes: requests.BlockWithTxHashes{
				Header: block.Header(),
				Hash:   block.Hash(),
			},
			Transactions: transactions,
		}, nil
	}

	return nil, fmt.Errorf("block %d not found", blockNum.Uint64())
}

func (rg *requestGenerator) GetTransactionReceipt(ctx context.Context, hash libcommon.Hash) (*types.Receipt, error) {
	rg.Lock()
	defer rg.Unlock()

	block, ok := rg.txBlockMap[hash]

	if !ok {
		return nil, fmt.Errorf("can't find block to tx: %s", hash)
	}

	engine := rg.bor
	chainConfig := params.BorDevnetChainConfig

	reader := blockReader{
		chain: rg.chain,
	}

	tx, err := rg.sentry.DB.BeginRo(context.Background())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	_, _, _, ibs, _, err := transactions.ComputeTxEnv(ctx, engine, block, chainConfig, reader, tx, 0, false)

	if err != nil {
		return nil, err
	}

	var usedGas uint64
	var usedBlobGas uint64

	gp := new(core.GasPool).AddGas(block.GasLimit()).AddBlobGas(chainConfig.GetMaxBlobGasPerBlock())

	noopWriter := state.NewNoopWriter()

	getHeader := func(hash common.Hash, number uint64) *types.Header {
		h, e := reader.Header(ctx, tx, hash, number)
		if e != nil {
			log.Error("getHeader error", "number", number, "hash", hash, "err", e)
		}
		return h
	}

	header := block.Header()

	for i, txn := range block.Transactions() {

		ibs.SetTxContext(txn.Hash(), block.Hash(), i)

		receipt, _, err := core.ApplyTransaction(chainConfig, core.GetHashFn(header, getHeader), engine, nil, gp, ibs, noopWriter, header, txn, &usedGas, &usedBlobGas, vm.Config{})

		if err != nil {
			return nil, err
		}

		if txn.Hash() == hash {
			receipt.BlockHash = block.Hash()
			return receipt, nil
		}
	}

	return nil, fmt.Errorf("tx not found in block")
}

type blockReader struct {
	services.FullBlockReader
	chain *core.ChainPack
}

func (reader blockReader) BlockByNumber(ctx context.Context, db kv.Tx, number uint64) (*types.Block, error) {
	if int(number) < len(reader.chain.Blocks) {
		return reader.chain.Blocks[number], nil
	}

	return nil, fmt.Errorf("block not found")
}

func (reader blockReader) HeaderByNumber(ctx context.Context, tx kv.Getter, blockNum uint64) (*types.Header, error) {
	if int(blockNum) < len(reader.chain.Headers) {
		return reader.chain.Headers[blockNum], nil
	}

	return nil, fmt.Errorf("header not found")
}

func TestMerkle(t *testing.T) {
	startBlock := 1600
	endBlock := 3200

	if depth := int(math.Ceil(math.Log2(float64(endBlock - startBlock + 1)))); depth != 11 {
		t.Fatal("Unexpected depth:", depth)
	}

	startBlock = 0
	endBlock = 100000

	if depth := int(math.Ceil(math.Log2(float64(endBlock - startBlock + 1)))); depth != 17 {
		t.Fatal("Unexpected depth:", depth)
	}

	startBlock = 0
	endBlock = 500000

	if depth := int(math.Ceil(math.Log2(float64(endBlock - startBlock + 1)))); depth != 19 {
		t.Fatal("Unexpected depth:", depth)
	}
}

func TestBlockGeneration(t *testing.T) {

	_, chain, err := generateBlocks(t, 1600)

	if err != nil {
		t.Fatal(err)
	}

	reader := blockReader{
		chain: chain,
	}

	for number := uint64(0); number < 1600; number++ {
		_, err = reader.BlockByNumber(context.Background(), nil, number)

		if err != nil {
			t.Fatal(err)
		}

		header, err := reader.HeaderByNumber(context.Background(), nil, number)

		if err != nil {
			t.Fatal(err)
		}

		if header == nil {
			t.Fatalf("block header not found: %d", number)
		}
	}
}

type genesisContract struct {
}

func (g genesisContract) CommitState(event rlp.RawValue, syscall consensus.SystemCall) error {
	return nil
}

func (g genesisContract) LastStateId(syscall consensus.SystemCall) (*big.Int, error) {
	return big.NewInt(0), nil
}

type spanner struct {
	*bor.ChainSpanner
	validatorAddress libcommon.Address
	currentSpan      heimdall.Span
}

func (c spanner) GetCurrentSpan(_ consensus.SystemCall) (*heimdall.Span, error) {
	return &c.currentSpan, nil
}

func (c *spanner) CommitSpan(heimdallSpan heimdall.Span, syscall consensus.SystemCall) error {
	c.currentSpan = heimdallSpan
	return nil
}

func (c *spanner) GetCurrentValidators(spanId uint64, signer libcommon.Address, chain consensus.ChainHeaderReader) ([]*valset.Validator, error) {
	return []*valset.Validator{
		{
			ID:               1,
			Address:          c.validatorAddress,
			VotingPower:      1000,
			ProposerPriority: 1,
		}}, nil
}

func TestBlockProof(t *testing.T) {
	sentry, chain, err := generateBlocks(t, 1600)

	if err != nil {
		t.Fatal(err)
	}

	rg, err := newRequestGenerator(sentry, chain)

	if err != nil {
		t.Fatal(err)
	}

	_, err = rg.GetRootHash(context.Background(), 0, 1599)

	if err != nil {
		t.Fatal(err)
	}

	blockProofs, err := getBlockProofs(context.Background(), rg, 10, 0, 1599)

	if err != nil {
		t.Fatal(err)
	}

	if len := len(blockProofs); len != 11 {
		t.Fatal("Unexpected block depth:", len)
	}

	if len := len(bytes.Join(blockProofs, []byte{})); len != 352 {
		t.Fatal("Unexpected proof len:", len)
	}
}

func TestReceiptProof(t *testing.T) {
	sentry, chain, err := generateBlocks(t, 10)

	if err != nil {
		t.Fatal(err)
	}

	rg, err := newRequestGenerator(sentry, chain)

	if err != nil {
		t.Fatal(err)
	}

	var block *requests.Block
	var blockNo uint64

	for block == nil {
		block, err = rg.GetBlockByNumber(context.Background(), rpc.AsBlockNumber(blockNo), true)

		if err != nil {
			t.Fatal(err)
		}

		if len(block.Transactions) == 0 {
			block = nil
			blockNo++
		}
	}

	receipt, err := rg.GetTransactionReceipt(context.Background(), block.Transactions[len(block.Transactions)-1].Hash)

	if err != nil {
		t.Fatal(err)
	}

	receiptProof, err := getReceiptProof(context.Background(), rg, receipt, block, nil)

	if err != nil {
		t.Fatal(err)
	}

	parentNodesBytes, err := rlp.EncodeToBytes(receiptProof.parentNodes)

	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(hexutility.Encode(parentNodesBytes), hexutility.Encode(append([]byte{0}, receiptProof.path...)))
}

func generateBlocks(t *testing.T, number int) (*mock.MockSentry, *core.ChainPack, error) {

	data := getGenesis(3)

	rand := randutil.NewMathRandomGenerator()

	return blocks.GenerateBlocks(t, data.genesisSpec, number, map[int]blocks.TxGen{
		0: {
			Fn:  getBlockTx(data.addresses[0], data.addresses[1], uint256.NewInt(uint64(rand.Intn(5000))+1)),
			Key: data.keys[0],
		},
		1: {
			Fn:  getBlockTx(data.addresses[1], data.addresses[2], uint256.NewInt(uint64(rand.Intn(5000))+1)),
			Key: data.keys[1],
		},
		2: {
			Fn:  getBlockTx(data.addresses[2], data.addresses[0], uint256.NewInt(uint64(rand.Intn(5000))+1)),
			Key: data.keys[2],
		},
	}, func(_ int) int {
		return rand.Intn(10)
	})
}

func getBlockTx(from libcommon.Address, to libcommon.Address, amount *uint256.Int) blocks.TxFn {
	return func(block *core.BlockGen, _ bind.ContractBackend) (types.Transaction, bool) {
		return types.NewTransaction(block.TxNonce(from), to, amount, 21000, new(uint256.Int), nil), false
	}
}

type initialData struct {
	keys         []*ecdsa.PrivateKey
	addresses    []libcommon.Address
	transactOpts []*bind.TransactOpts
	genesisSpec  *types.Genesis
}

func getGenesis(accounts int, funds ...*big.Int) initialData {
	accountFunds := big.NewInt(1000000000)
	if len(funds) > 0 {
		accountFunds = funds[0]
	}

	keys := make([]*ecdsa.PrivateKey, accounts)

	for i := 0; i < accounts; i++ {
		keys[i], _ = crypto.GenerateKey()
	}

	addresses := make([]libcommon.Address, 0, len(keys))
	transactOpts := make([]*bind.TransactOpts, 0, len(keys))
	allocs := types.GenesisAlloc{}
	for _, key := range keys {
		addr := crypto.PubkeyToAddress(key.PublicKey)
		addresses = append(addresses, addr)
		to, err := bind.NewKeyedTransactorWithChainID(key, big.NewInt(1))
		if err != nil {
			panic(err)
		}
		transactOpts = append(transactOpts, to)

		allocs[addr] = types.GenesisAccount{Balance: accountFunds}
	}

	return initialData{
		keys:         keys,
		addresses:    addresses,
		transactOpts: transactOpts,
		genesisSpec: &types.Genesis{
			Config: &chain.Config{
				ChainID:               big.NewInt(1),
				HomesteadBlock:        new(big.Int),
				TangerineWhistleBlock: new(big.Int),
				SpuriousDragonBlock:   big.NewInt(1),
				ByzantiumBlock:        big.NewInt(1),
				ConstantinopleBlock:   big.NewInt(1),
			},
			Alloc: allocs,
		},
	}
}
