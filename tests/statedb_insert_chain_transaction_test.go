package tests

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/turbo/stages"

	"github.com/ledgerwatch/erigon/accounts/abi/bind"
	"github.com/ledgerwatch/erigon/accounts/abi/bind/backends"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/tests/contracts"
)

func TestInsertIncorrectStateRootDifferentAccounts(t *testing.T) {
	data := getGenesis()
	from := data.addresses[0]
	fromKey := data.keys[0]
	to := libcommon.Address{1}

	m, chain, err := genBlocks(t, data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(from, to, uint256.NewInt(1000)),
			fromKey,
		},
		1: {
			getBlockTx(from, to, uint256.NewInt(2000)),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// BLOCK 1
	incorrectHeader := *chain.Headers[0] // Copy header, not just pointer
	incorrectHeader.Root = chain.Headers[1].Root

	if chain.Headers[0].Root == incorrectHeader.Root {
		t.Fatal("roots are the same")
	}

	incorrectBlock := types.NewBlock(&incorrectHeader, chain.Blocks[0].Transactions(), chain.Blocks[0].Uncles(), chain.Receipts[0], nil)
	incorrectChain := &core.ChainPack{Blocks: []*types.Block{incorrectBlock}, Headers: []*types.Header{&incorrectHeader}, TopBlock: incorrectBlock}
	if err = m.InsertChain(incorrectChain); err == nil {
		t.Fatal("should fail")
	}

	// insert a correct block
	m, chain, err = genBlocks(t, data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(data.addresses[1], to, uint256.NewInt(5000)),
			data.keys[1],
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err = m.InsertChain(chain); err != nil {
		t.Fatal(err)
	}

	tx, err := m.DB.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	st := state.New(state.NewPlainStateReader(tx))
	if !st.Exist(to) {
		t.Error("expected account to exist")
	}

	if balance := st.GetBalance(from); balance.Uint64() != 1000000000 {
		t.Fatalf("got %v, expected %v", balance, 1000000000)
	}
	if balance := st.GetBalance(data.addresses[1]); balance.Uint64() != 999995000 {
		t.Fatalf("got %v, expected %v", balance, 999995000)
	}
	if balance := st.GetBalance(to); balance.Uint64() != 5000 {
		t.Fatalf("got %v, expected %v", balance, 5000)
	}
}

func TestInsertIncorrectStateRootSameAccount(t *testing.T) {
	data := getGenesis()
	from := data.addresses[0]
	fromKey := data.keys[0]
	to := libcommon.Address{1}

	m, chain, err := genBlocks(t, data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(from, to, uint256.NewInt(1000)),
			fromKey,
		},
		1: {
			getBlockTx(from, to, uint256.NewInt(2000)),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// BLOCK 1
	incorrectHeader := *chain.Headers[0] // Copy header, not just pointer
	incorrectHeader.Root = chain.Headers[1].Root

	if chain.Headers[0].Root == incorrectHeader.Root {
		t.Fatal("roots are the same")
	}

	incorrectBlock := types.NewBlock(&incorrectHeader, chain.Blocks[0].Transactions(), chain.Blocks[0].Uncles(), chain.Receipts[0], nil)
	incorrectChain := &core.ChainPack{Blocks: []*types.Block{incorrectBlock}, Headers: []*types.Header{&incorrectHeader}, TopBlock: incorrectBlock}
	if err = m.InsertChain(incorrectChain); err == nil {
		t.Fatal("should fail")
	}

	// insert a correct block
	m, chain, err = genBlocks(t, data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(from, to, uint256.NewInt(5000)),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err = m.InsertChain(chain); err != nil {
		t.Fatal(err)
	}

	tx, err := m.DB.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	st := state.New(state.NewPlainStateReader(tx))
	if !st.Exist(to) {
		t.Error("expected account to exist")
	}

	if balance := st.GetBalance(from); balance.Uint64() != 999995000 {
		t.Fatalf("got %v, expected %v", balance, 999995000)
	}
	if balance := st.GetBalance(to); balance.Uint64() != 5000 {
		t.Fatalf("got %v, expected %v", balance, 5000)
	}
}

func TestInsertIncorrectStateRootSameAccountSameAmount(t *testing.T) {
	data := getGenesis()
	from := data.addresses[0]
	fromKey := data.keys[0]
	to := libcommon.Address{1}

	m, chain, err := genBlocks(t, data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(from, to, uint256.NewInt(1000)),
			fromKey,
		},
		1: {
			getBlockTx(from, to, uint256.NewInt(2000)),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// BLOCK 1
	incorrectHeader := *chain.Headers[0] // Copy header, not just pointer
	incorrectHeader.Root = chain.Headers[1].Root

	incorrectBlock := types.NewBlock(&incorrectHeader, chain.Blocks[0].Transactions(), chain.Blocks[0].Uncles(), chain.Receipts[0], nil)
	incorrectChain := &core.ChainPack{Blocks: []*types.Block{incorrectBlock}, Headers: []*types.Header{&incorrectHeader}, TopBlock: incorrectBlock}
	if err = m.InsertChain(incorrectChain); err == nil {
		t.Fatal("should fail")
	}

	// insert a correct block
	m, chain, err = genBlocks(t, data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(from, to, uint256.NewInt(1000)),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err = m.InsertChain(chain); err != nil {
		t.Fatal(err)
	}

	tx, err := m.DB.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	st := state.New(state.NewPlainStateReader(tx))
	if !st.Exist(to) {
		t.Error("expected account to exist")
	}

	if balance := st.GetBalance(from); balance.Uint64() != 999999000 {
		t.Fatalf("got %v, expected %v", balance, 999999000)
	}
	if balance := st.GetBalance(to); balance.Uint64() != 1000 {
		t.Fatalf("got %v, expected %v", balance, 1000)
	}
}

func TestInsertIncorrectStateRootAllFundsRoot(t *testing.T) {
	data := getGenesis(big.NewInt(3000))
	from := data.addresses[0]
	fromKey := data.keys[0]
	to := libcommon.Address{1}

	m, chain, err := genBlocks(t, data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(from, to, uint256.NewInt(1000)),
			fromKey,
		},
		1: {
			getBlockTx(from, to, uint256.NewInt(2000)),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// BLOCK 1
	incorrectHeader := *chain.Headers[0] // Copy header, not just pointer
	incorrectHeader.Root = chain.Headers[1].Root

	incorrectBlock := types.NewBlock(&incorrectHeader, chain.Blocks[0].Transactions(), chain.Blocks[0].Uncles(), chain.Receipts[0], nil)
	incorrectChain := &core.ChainPack{Blocks: []*types.Block{incorrectBlock}, Headers: []*types.Header{&incorrectHeader}, TopBlock: incorrectBlock}
	if err = m.InsertChain(incorrectChain); err == nil {
		t.Fatal("should fail")
	}

	// insert a correct block
	m, chain, err = genBlocks(t, data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(from, to, uint256.NewInt(1000)),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err = m.InsertChain(chain); err != nil {
		t.Fatal(err)
	}

	tx, err := m.DB.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	st := state.New(state.NewPlainStateReader(tx))
	if !st.Exist(to) {
		t.Error("expected account to exist")
	}

	if balance := st.GetBalance(from); balance.Uint64() != 2000 {
		t.Fatalf("got %v, expected %v", balance, 2000)
	}
	if balance := st.GetBalance(to); balance.Uint64() != 1000 {
		t.Fatalf("got %v, expected %v", balance, 1000)
	}
}

func TestInsertIncorrectStateRootAllFunds(t *testing.T) {
	data := getGenesis(big.NewInt(3000))
	from := data.addresses[0]
	fromKey := data.keys[0]
	to := libcommon.Address{1}

	m, chain, err := genBlocks(t, data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(from, to, uint256.NewInt(3000)),
			fromKey,
		},
		1: {
			getBlockTx(data.addresses[1], to, uint256.NewInt(2000)),
			data.keys[1],
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// BLOCK 1
	incorrectHeader := *chain.Headers[0] // Copy header, not just pointer
	incorrectHeader.Root = chain.Headers[1].Root
	incorrectBlock := types.NewBlock(&incorrectHeader, chain.Blocks[0].Transactions(), chain.Blocks[0].Uncles(), chain.Receipts[0], nil)
	incorrectChain := &core.ChainPack{Blocks: []*types.Block{incorrectBlock}, Headers: []*types.Header{&incorrectHeader}, TopBlock: incorrectBlock}

	if err = m.InsertChain(incorrectChain); err == nil {
		t.Fatal("should fail")
	}

	// insert a correct block
	m, chain, err = genBlocks(t, data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(from, to, uint256.NewInt(1000)),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if err = m.InsertChain(chain); err != nil {
		t.Fatal(err)
	}

	tx, err := m.DB.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	st := state.New(state.NewPlainStateReader(tx))
	if !st.Exist(to) {
		t.Error("expected account to exist")
	}

	if balance := st.GetBalance(from); balance.Uint64() != 2000 {
		t.Fatalf("got %v, expected %v", balance, 2000)
	}
	if balance := st.GetBalance(to); balance.Uint64() != 1000 {
		t.Fatalf("got %v, expected %v", balance, 1000)
	}
}

func TestAccountDeployIncorrectRoot(t *testing.T) {
	data := getGenesis()
	from := data.addresses[0]
	fromKey := data.keys[0]
	to := libcommon.Address{1}

	var contractAddress libcommon.Address
	eipContract := new(contracts.Testcontract)

	m, chain, err := genBlocks(t, data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(from, to, uint256.NewInt(10)),
			fromKey,
		},
		1: {
			getBlockDeployTestContractTx(data.transactOpts[0], &contractAddress, eipContract),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// BLOCK 1
	if err = m.InsertChain(chain.Slice(0, 1)); err != nil {
		t.Fatal(err)
	}
	err = m.DB.View(context.Background(), func(tx kv.Tx) error {
		st := state.New(state.NewPlainStateReader(tx))
		if !st.Exist(from) {
			t.Error("expected account to exist")
		}

		if st.Exist(contractAddress) {
			t.Error("expected contractAddress to not exist at the block 0", contractAddress.Hash().String())
		}
		return nil
	})
	require.NoError(t, err)

	incorrectHeader := *chain.Headers[1] // Copy header, not just pointer
	incorrectHeader.Root = chain.Headers[0].Root
	incorrectBlock := types.NewBlock(&incorrectHeader, chain.Blocks[1].Transactions(), chain.Blocks[1].Uncles(), chain.Receipts[1], nil)
	incorrectChain := &core.ChainPack{Blocks: []*types.Block{incorrectBlock}, Headers: []*types.Header{&incorrectHeader}, TopBlock: incorrectBlock}

	// BLOCK 2 - INCORRECT
	if err = m.InsertChain(incorrectChain); err == nil {
		t.Fatal("should fail")
	}

	err = m.DB.View(context.Background(), func(tx kv.Tx) error {
		st := state.New(state.NewPlainStateReader(tx))
		if !st.Exist(from) {
			t.Error("expected account to exist")
		}

		if st.Exist(contractAddress) {
			t.Error("expected contractAddress to not exist at the block 1", contractAddress.Hash().String())
		}
		return nil
	})
	require.NoError(t, err)

	// BLOCK 2 - CORRECT
	if err = m.InsertChain(chain.Slice(1, 2)); err != nil {
		t.Fatal(err)
	}

	err = m.DB.View(context.Background(), func(tx kv.Tx) error {
		st := state.New(state.NewPlainStateReader(tx))
		if !st.Exist(from) {
			t.Error("expected account to exist")
		}

		if !st.Exist(contractAddress) {
			t.Error("expected contractAddress to not exist at the block 1", contractAddress.Hash().String())
		}
		return nil
	})
	require.NoError(t, err)
}

func TestAccountCreateIncorrectRoot(t *testing.T) {
	data := getGenesis()
	from := data.addresses[0]
	fromKey := data.keys[0]
	to := libcommon.Address{1}

	var contractAddress libcommon.Address
	eipContract := new(contracts.Testcontract)

	m, chain, err := genBlocks(t, data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(from, to, uint256.NewInt(10)),
			fromKey,
		},
		1: {
			getBlockDeployTestContractTx(data.transactOpts[0], &contractAddress, eipContract),
			fromKey,
		},
		2: {
			getBlockTestContractTx(data.transactOpts[0], eipContract.Create, big.NewInt(2)),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// BLOCK 1
	if err = m.InsertChain(chain.Slice(0, 1)); err != nil {
		t.Fatal(err)
	}

	err = m.DB.View(context.Background(), func(tx kv.Tx) error {
		st := state.New(state.NewPlainStateReader(tx))
		if !st.Exist(from) {
			t.Error("expected account to exist")
		}

		if st.Exist(contractAddress) {
			t.Error("expected contractAddress to not exist at the block 0", contractAddress.Hash().String())
		}

		return nil
	})
	require.NoError(t, err)

	// BLOCK 2
	if err = m.InsertChain(chain.Slice(1, 2)); err != nil {
		t.Fatal(err)
	}
	err = m.DB.View(context.Background(), func(tx kv.Tx) error {
		st := state.New(state.NewPlainStateReader(tx))
		if !st.Exist(from) {
			t.Error("expected account to exist")
		}

		if !st.Exist(contractAddress) {
			t.Error("expected contractAddress to exist at the block 2", contractAddress.Hash().String())
		}

		return nil
	})
	require.NoError(t, err)

	// BLOCK 3 - INCORRECT
	incorrectHeader := *chain.Headers[2] // Copy header, not just pointer
	incorrectHeader.Root = chain.Headers[1].Root
	incorrectBlock := types.NewBlock(&incorrectHeader, chain.Blocks[2].Transactions(), chain.Blocks[2].Uncles(), chain.Receipts[2], nil)
	incorrectChain := &core.ChainPack{Blocks: []*types.Block{incorrectBlock}, Headers: []*types.Header{&incorrectHeader}, TopBlock: incorrectBlock}

	if err = m.InsertChain(incorrectChain); err == nil {
		t.Fatal("should fail")
	}

	// BLOCK 3
	if err = m.InsertChain(chain.Slice(2, 3)); err != nil {
		t.Fatal(err)
	}
}

func TestAccountUpdateIncorrectRoot(t *testing.T) {
	data := getGenesis()
	from := data.addresses[0]
	fromKey := data.keys[0]
	to := libcommon.Address{1}

	var contractAddress libcommon.Address
	eipContract := new(contracts.Testcontract)

	m, chain, err := genBlocks(t, data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(from, to, uint256.NewInt(10)),
			fromKey,
		},
		1: {
			getBlockDeployTestContractTx(data.transactOpts[0], &contractAddress, eipContract),
			fromKey,
		},
		2: {
			getBlockTestContractTx(data.transactOpts[0], eipContract.Create, big.NewInt(2)),
			fromKey,
		},
		3: {
			getBlockTestContractTx(data.transactOpts[0], eipContract.Update, big.NewInt(0)),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// BLOCK 1
	if err = m.InsertChain(chain.Slice(0, 1)); err != nil {
		t.Fatal(err)
	}

	err = m.DB.View(context.Background(), func(tx kv.Tx) error {
		st := state.New(state.NewPlainStateReader(tx))
		if !st.Exist(from) {
			t.Error("expected account to exist")
		}

		if st.Exist(contractAddress) {
			t.Error("expected contractAddress to not exist at the block 0", contractAddress.Hash().String())
		}

		return nil
	})
	require.NoError(t, err)

	// BLOCK 2
	if err = m.InsertChain(chain.Slice(1, 2)); err != nil {
		t.Fatal(err)
	}

	err = m.DB.View(context.Background(), func(tx kv.Tx) error {
		st := state.New(state.NewPlainStateReader(tx))
		if !st.Exist(from) {
			t.Error("expected account to exist")
		}

		if !st.Exist(contractAddress) {
			t.Error("expected contractAddress to exist at the block 2", contractAddress.Hash().String())
		}
		return nil
	})
	require.NoError(t, err)

	// BLOCK 3
	if err = m.InsertChain(chain.Slice(2, 3)); err != nil {
		t.Fatal(err)
	}

	// BLOCK 4 - INCORRECT
	incorrectHeader := *chain.Headers[3] // Copy header, not just pointer
	incorrectHeader.Root = chain.Headers[1].Root
	incorrectBlock := types.NewBlock(&incorrectHeader, chain.Blocks[3].Transactions(), chain.Blocks[3].Uncles(), chain.Receipts[3], nil)
	incorrectChain := &core.ChainPack{Blocks: []*types.Block{incorrectBlock}, Headers: []*types.Header{&incorrectHeader}, TopBlock: incorrectBlock}

	if err = m.InsertChain(incorrectChain); err == nil {
		t.Fatal("should fail")
	}

	// BLOCK 4
	if err = m.InsertChain(chain.Slice(3, 4)); err != nil {
		t.Fatal(err)
	}
}

func TestAccountDeleteIncorrectRoot(t *testing.T) {
	data := getGenesis()
	from := data.addresses[0]
	fromKey := data.keys[0]
	to := libcommon.Address{1}

	var contractAddress libcommon.Address
	eipContract := new(contracts.Testcontract)

	m, chain, err := genBlocks(t, data.genesisSpec, map[int]tx{
		0: {
			getBlockTx(from, to, uint256.NewInt(10)),
			fromKey,
		},
		1: {
			getBlockDeployTestContractTx(data.transactOpts[0], &contractAddress, eipContract),
			fromKey,
		},
		2: {
			getBlockTestContractTx(data.transactOpts[0], eipContract.Create, big.NewInt(2)),
			fromKey,
		},
		3: {
			getBlockTestContractTx(data.transactOpts[0], eipContract.Remove),
			fromKey,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// BLOCK 1
	if err = m.InsertChain(chain.Slice(0, 1)); err != nil {
		t.Fatal(err)
	}

	err = m.DB.View(context.Background(), func(tx kv.Tx) error {
		st := state.New(state.NewPlainStateReader(tx))
		if !st.Exist(from) {
			t.Error("expected account to exist")
		}

		if st.Exist(contractAddress) {
			t.Error("expected contractAddress to not exist at the block 0", contractAddress.Hash().String())
		}
		return nil
	})
	require.NoError(t, err)

	// BLOCK 2
	if err = m.InsertChain(chain.Slice(1, 2)); err != nil {
		t.Fatal(err)
	}

	err = m.DB.View(context.Background(), func(tx kv.Tx) error {
		st := state.New(state.NewPlainStateReader(tx))
		if !st.Exist(from) {
			t.Error("expected account to exist")
		}

		if !st.Exist(contractAddress) {
			t.Error("expected contractAddress to exist at the block 1", contractAddress.Hash().String())
		}
		return nil
	})
	require.NoError(t, err)

	// BLOCK 3
	if err = m.InsertChain(chain.Slice(2, 3)); err != nil {
		t.Fatal(err)
	}

	// BLOCK 4 - INCORRECT
	incorrectHeader := *chain.Headers[3] // Copy header, not just pointer
	incorrectHeader.Root = chain.Headers[1].Root
	incorrectBlock := types.NewBlock(&incorrectHeader, chain.Blocks[3].Transactions(), chain.Blocks[3].Uncles(), chain.Receipts[3], nil)
	incorrectChain := &core.ChainPack{Blocks: []*types.Block{incorrectBlock}, Headers: []*types.Header{&incorrectHeader}, TopBlock: incorrectBlock}
	if err = m.InsertChain(incorrectChain); err == nil {
		t.Fatal("should fail")
	}

	// BLOCK 4
	if err = m.InsertChain(chain.Slice(3, 4)); err != nil {
		t.Fatal(err)
	}
}

type initialData struct {
	keys         []*ecdsa.PrivateKey
	addresses    []libcommon.Address
	transactOpts []*bind.TransactOpts
	genesisSpec  *core.Genesis
}

func getGenesis(funds ...*big.Int) initialData {
	accountFunds := big.NewInt(1000000000)
	if len(funds) > 0 {
		accountFunds = funds[0]
	}

	keys := make([]*ecdsa.PrivateKey, 3)
	keys[0], _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	keys[1], _ = crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
	keys[2], _ = crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")

	addresses := make([]libcommon.Address, 0, len(keys))
	transactOpts := make([]*bind.TransactOpts, 0, len(keys))
	allocs := core.GenesisAlloc{}
	for _, key := range keys {
		addr := crypto.PubkeyToAddress(key.PublicKey)
		addresses = append(addresses, addr)
		to, err := bind.NewKeyedTransactorWithChainID(key, big.NewInt(1))
		if err != nil {
			panic(err)
		}
		transactOpts = append(transactOpts, to)

		allocs[addr] = core.GenesisAccount{Balance: accountFunds}
	}

	return initialData{
		keys:         keys,
		addresses:    addresses,
		transactOpts: transactOpts,
		genesisSpec: &core.Genesis{
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

type tx struct {
	txFn blockTx
	key  *ecdsa.PrivateKey
}

func genBlocks(t *testing.T, gspec *core.Genesis, txs map[int]tx) (*stages.MockSentry, *core.ChainPack, error) {
	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	m := stages.MockWithGenesis(t, gspec, key, false)

	contractBackend := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	defer contractBackend.Close()

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, len(txs), func(i int, block *core.BlockGen) {
		var tx types.Transaction
		var isContractCall bool
		signer := types.LatestSignerForChainID(nil)

		if txToSend, ok := txs[i]; ok {
			tx, isContractCall = txToSend.txFn(block, contractBackend)
			var err error
			tx, err = types.SignTx(tx, *signer, txToSend.key)
			if err != nil {
				return
			}
		}

		if tx != nil {
			if !isContractCall {
				err := contractBackend.SendTransaction(context.Background(), tx)
				if err != nil {
					return
				}
			}

			block.AddTx(tx)
		}

		contractBackend.Commit()
	}, false /* intermediateHashes */)
	if err != nil {
		return nil, nil, fmt.Errorf("generate chain: %w", err)
	}
	return m, chain, err
}

type blockTx func(_ *core.BlockGen, backend bind.ContractBackend) (types.Transaction, bool)

func getBlockTx(from libcommon.Address, to libcommon.Address, amount *uint256.Int) blockTx {
	return func(block *core.BlockGen, _ bind.ContractBackend) (types.Transaction, bool) {
		return types.NewTransaction(block.TxNonce(from), to, amount, 21000, new(uint256.Int), nil), false
	}
}

func getBlockDeployTestContractTx(transactOpts *bind.TransactOpts, contractAddress *libcommon.Address, eipContract *contracts.Testcontract) blockTx {
	return func(_ *core.BlockGen, backend bind.ContractBackend) (types.Transaction, bool) {
		contractAddressRes, tx, eipContractRes, err := contracts.DeployTestcontract(transactOpts, backend)
		if err != nil {
			panic(err)
		}

		*contractAddress = contractAddressRes
		*eipContract = *eipContractRes

		return tx, true
	}
}

func getBlockTestContractTx(transactOpts *bind.TransactOpts, contractCall interface{}, newBalance ...*big.Int) blockTx {
	return func(_ *core.BlockGen, backend bind.ContractBackend) (types.Transaction, bool) {
		var (
			tx  types.Transaction
			err error
		)

		switch fn := contractCall.(type) {
		case func(opts *bind.TransactOpts) (types.Transaction, error):
			tx, err = fn(transactOpts)
		case func(opts *bind.TransactOpts, newBalance *big.Int) (types.Transaction, error):
			if len(newBalance) != 1 {
				panic("*big.Int type new balance is expected")
			}
			tx, err = fn(transactOpts, newBalance[0])
		default:
			panic("non expected function type")
		}

		if err != nil {
			panic(err)
		}

		return tx, true
	}
}
