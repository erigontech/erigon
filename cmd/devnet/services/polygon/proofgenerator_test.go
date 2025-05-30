// Copyright 2024 The Erigon Authors
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

package polygon

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sync"
	"testing"

	"github.com/holiman/uint256"
	"github.com/pion/randutil"

	"github.com/erigontech/erigon-db/rawdb"
	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/memdb"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/cmd/devnet/blocks"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/execution/abi/bind"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/polygon/bor"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/requests"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/stages/mock"
	"github.com/erigontech/erigon/turbo/transactions"
)

type requestGenerator struct {
	sync.Mutex
	requests.NopRequestGenerator
	sentry     *mock.MockSentry
	bor        *bor.Bor
	chain      *core.ChainPack
	txBlockMap map[common.Hash]*types.Block
}

func newRequestGenerator(sentry *mock.MockSentry, chain *core.ChainPack) (*requestGenerator, error) {
	db := memdb.New("", kv.ChainDB)
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
		chain:      chain,
		sentry:     sentry,
		bor:        bor.NewRo(params.BorDevnetChainConfig, db, reader, log.Root()),
		txBlockMap: map[common.Hash]*types.Block{},
	}, nil
}

func (rg *requestGenerator) GetRootHash(ctx context.Context, startBlock uint64, endBlock uint64) (common.Hash, error) {
	tx, err := rg.bor.DB.BeginRo(context.Background())
	if err != nil {
		return common.Hash{}, err
	}
	defer tx.Rollback()

	result, err := rg.bor.GetRootHash(ctx, tx, startBlock, endBlock)

	if err != nil {
		return common.Hash{}, err
	}

	return common.HexToHash(result), nil
}

func (rg *requestGenerator) GetBlockByNumber(ctx context.Context, blockNum rpc.BlockNumber, withTxs bool) (*requests.Block, error) {
	if bn := int(blockNum.Uint64()); bn < len(rg.chain.Blocks) {
		block := rg.chain.Blocks[bn]

		transactions := make([]*ethapi.RPCTransaction, len(block.Transactions()))

		for i, txn := range block.Transactions() {
			rg.txBlockMap[txn.Hash()] = block
			transactions[i] = ethapi.NewRPCTransaction(txn, block.Hash(), blockNum.Uint64(), uint64(i), block.BaseFee())
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

func (rg *requestGenerator) GetTransactionReceipt(ctx context.Context, hash common.Hash) (*types.Receipt, error) {
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

	tx, err := rg.sentry.DB.BeginTemporalRo(context.Background())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	ibs, _, _, _, _, err := transactions.ComputeBlockContext(ctx, engine, block.HeaderNoCopy(), chainConfig, reader, rawdbv3.TxNums, tx, 0)
	if err != nil {
		return nil, err
	}

	var gasUsed uint64
	var usedBlobGas uint64

	gp := new(core.GasPool).AddGas(block.GasLimit()).AddBlobGas(chainConfig.GetMaxBlobGasPerBlock(block.Header().Time))

	noopWriter := state.NewNoopWriter()

	getHeader := func(hash common.Hash, number uint64) (*types.Header, error) {
		return reader.Header(ctx, tx, hash, number)
	}

	header := block.Header()

	for i, txn := range block.Transactions() {

		ibs.SetTxContext(block.NumberU64(), i)

		receipt, _, err := core.ApplyTransaction(chainConfig, core.GetHashFn(header, getHeader), engine, nil, gp, ibs, noopWriter, header, txn, &gasUsed, &usedBlobGas, vm.Config{})

		if err != nil {
			return nil, err
		}

		if txn.Hash() == hash {
			receipt.BlockHash = block.Hash()
			return receipt, nil
		}
	}

	return nil, errors.New("tx not found in block")
}

type blockReader struct {
	services.FullBlockReader
	chain *core.ChainPack
}

func (reader blockReader) BlockByNumber(ctx context.Context, db kv.Tx, number uint64) (*types.Block, error) {
	if int(number) < len(reader.chain.Blocks) {
		return reader.chain.Blocks[number], nil
	}

	return nil, errors.New("block not found")
}

func (reader blockReader) HeaderByNumber(ctx context.Context, txn kv.Getter, blockNum uint64) (*types.Header, error) {
	if int(blockNum) < len(reader.chain.Headers) {
		return reader.chain.Headers[blockNum], nil
	}

	return nil, errors.New("header not found")
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
	if testing.Short() {
		t.Skip()
	}

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

func TestBlockProof(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

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

	fmt.Println(hexutil.Encode(parentNodesBytes), hexutil.Encode(append([]byte{0}, receiptProof.path...)))
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

func getBlockTx(from common.Address, to common.Address, amount *uint256.Int) blocks.TxFn {
	return func(block *core.BlockGen, _ bind.ContractBackend) (types.Transaction, bool) {
		return types.NewTransaction(block.TxNonce(from), to, amount, 21000, new(uint256.Int), nil), false
	}
}

type initialData struct {
	keys         []*ecdsa.PrivateKey
	addresses    []common.Address
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

	addresses := make([]common.Address, 0, len(keys))
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
