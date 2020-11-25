package txpoolprovider

import (
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
)

func TestAppliedBlock(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	var (
		funds  = big.NewInt(100000)
		key, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		addr   = crypto.PubkeyToAddress(key.PublicKey)
		gspec  = &core.Genesis{
			Config:   params.TestChainConfig,
			GasLimit: 3141592,
			Alloc: core.GenesisAlloc{
				addr: {Balance: funds},
			},
		}
		genesis = gspec.MustCommit(db)
		signer  = types.NewEIP155Signer(gspec.Config.ChainID)
	)

	genesisDb := db.MemCopy()
	defer genesisDb.Close()

	chain, _, err := core.GenerateChain(gspec.Config, genesis, ethash.NewFaker(), genesisDb, 2, func(i int, gen *core.BlockGen) {
		if i == 0 {
			tx, _ := types.SignTx(types.NewTransaction(gen.TxNonce(addr), addr, uint256.NewInt().SetUint64(1000), params.TxGas, nil, nil), signer, key)
			gen.AddTx(tx)
		}
	}, false)
	if err != nil {
		t.Fatalf("generate chain: %v", err)
	}

	// Import the chain. This runs all block validation rules.
	if _, err1 := stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, gspec.Config, &vm.Config{}, ethash.NewFaker(), chain, true /* checkRoot */); err1 != nil {
		t.Fatalf("failed to insert original chain: %v", err1)
		return
	}

	oldHead := genesis.Header()
	newHead := chain[0].Header()

	included, _ := cmpTxsAcrossFork(oldHead, newHead, db)

	addrs, nonces, balances := touchedAccounts(included, db)

	if len(addrs) != 1 {
		t.Errorf("incorrect number of accounts touched, should be 1")
	}

	// Ensure that all the touched accounts are exposed in the account diff.
	if addrs[0].Hex() != addr.Hex() || nonces[0] != 1 || balances[0].Cmp(funds) != 0 {
		t.Errorf("incorrect account diff: got (%s, %d, %s), expected (%s, %d, %s)", addrs[0].Hex(), nonces[0], balances[0].String(), addr.Hex(), 1, funds)
	}
}

func TestRevertedBlock(t *testing.T) {
	db := ethdb.NewMemDatabase()
	defer db.Close()
	var (
		funds   = big.NewInt(100000)
		key1, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		key2, _ = crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
		key3, _ = crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
		addr1   = crypto.PubkeyToAddress(key1.PublicKey)
		addr2   = crypto.PubkeyToAddress(key2.PublicKey)
		addr3   = crypto.PubkeyToAddress(key3.PublicKey)
		gspec   = &core.Genesis{
			Config:   params.TestChainConfig,
			GasLimit: 3141592,
			Alloc: core.GenesisAlloc{
				addr1: {Balance: funds},
				addr2: {Balance: funds},
				addr3: {Balance: funds},
			},
		}
		genesis = gspec.MustCommit(db)
		signer  = types.NewEIP155Signer(gspec.Config.ChainID)
	)

	genesisDb := db.MemCopy()
	defer genesisDb.Close()

	// Create two transactions shared between the chains:
	//  - postponed: transaction included at a later block in the forked chain
	//  - swapped: transaction included at the same block number in the forked chain
	postponed, _ := types.SignTx(types.NewTransaction(0, addr1, uint256.NewInt().SetUint64(1000), params.TxGas, nil, nil), signer, key1)
	swapped, _ := types.SignTx(types.NewTransaction(1, addr1, uint256.NewInt().SetUint64(1000), params.TxGas, nil, nil), signer, key1)

	// Create two transactions that will be dropped by the forked chain:
	//  - pastDrop: transaction dropped retroactively from a past block
	//  - freshDrop: transaction dropped exactly at the block where the reorg is detected
	var pastDrop, freshDrop *types.Transaction

	// Create three transactions that will be added in the forked chain:
	//  - pastAdd:   transaction added before the reorganization is detected
	//  - freshAdd:  transaction added at the exact block the reorg is detected
	//  - futureAdd: transaction added after the reorg has already finished
	var pastAdd, freshAdd, futureAdd *types.Transaction

	chain, _, err := core.GenerateChain(gspec.Config, genesis, ethash.NewFaker(), genesisDb, 3, func(i int, gen *core.BlockGen) {
		switch i {
		case 0:
			pastDrop, _ = types.SignTx(types.NewTransaction(gen.TxNonce(addr2), addr2, uint256.NewInt().SetUint64(1001), params.TxGas, nil, nil), signer, key2)

			gen.AddTx(pastDrop)  // This transaction will be dropped in the fork from below the split point
			gen.AddTx(postponed) // This transaction will be postponed till block #3 in the fork

		case 2:
			freshDrop, _ = types.SignTx(types.NewTransaction(gen.TxNonce(addr2), addr2, uint256.NewInt().SetUint64(1002), params.TxGas, nil, nil), signer, key2)

			gen.AddTx(freshDrop) // This transaction will be dropped in the fork from exactly at the split point
			gen.AddTx(swapped)   // This transaction will be swapped out at the exact height

			gen.OffsetTime(9) // Lower the block difficulty to simulate a weaker chain
		}
	}, false)
	if err != nil {
		t.Fatalf("generate chain: %v", err)
	}

	// Import the chain. This runs all block validation rules.
	if _, err1 := stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, gspec.Config, &vm.Config{}, ethash.NewFaker(), chain, true /* checkRoot */); err1 != nil {
		t.Fatalf("failed to insert original chain: %v", err1)
	}

	// overwrite the old chain
	fork, _, err := core.GenerateChain(gspec.Config, genesis, ethash.NewFaker(), genesisDb, 5, func(i int, gen *core.BlockGen) {
		switch i {
		case 0:
			pastAdd, _ = types.SignTx(types.NewTransaction(gen.TxNonce(addr3), addr3, uint256.NewInt().SetUint64(1000), params.TxGas, nil, nil), signer, key3)
			gen.AddTx(pastAdd) // This transaction needs to be injected during reorg

		case 2:
			gen.AddTx(postponed) // This transaction was postponed from block #1 in the original chain
			gen.AddTx(swapped)   // This transaction was swapped from the exact current spot in the original chain

			freshAdd, _ = types.SignTx(types.NewTransaction(gen.TxNonce(addr3), addr3, uint256.NewInt().SetUint64(1000), params.TxGas, nil, nil), signer, key3)
			gen.AddTx(freshAdd) // This transaction will be added exactly at reorg time

		case 3:
			futureAdd, _ = types.SignTx(types.NewTransaction(gen.TxNonce(addr3), addr3, uint256.NewInt().SetUint64(1000), params.TxGas, nil, nil), signer, key3)
			gen.AddTx(futureAdd) // This transaction will be added after a full reorg
		}
	}, false)
	if err != nil {
		t.Fatalf("generate chain: %v", err)
	}
	if _, err := stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, gspec.Config, &vm.Config{}, ethash.NewFaker(), fork, true /* checkRoot */); err != nil {
		t.Fatalf("failed to insert forked chain: %v", err)
	}

	oldHead := chain[2].Header()
	newHead := fork[4].Header()

	included, discarded := cmpTxsAcrossFork(oldHead, newHead, db)
	reverted := types.TxDifference(discarded, included)
	expected := types.Transactions{freshDrop, pastDrop}

	// Check that the transactions expected to be reverted from the fork
	// were actually reverted.
	if len(reverted) != len(expected) {
		t.Errorf("incorrect number of reverted transactions: got %d, expected %d", len(reverted), len(expected))
	}
	if reverted[0].Hash() != expected[0].Hash() || reverted[1].Hash() != expected[1].Hash() {
		t.Errorf("incorrect reverted transactions list: %v, %v", reverted, expected)
	}

	all := append(included, discarded...)
	addrs, nonces, balances := touchedAccounts(all, db)

	// This is just the order in which the function returns the account diff.
	expectedAddrs := []common.Address{addr3, addr1, addr2}
	expectedNonces := []uint64{3, 2, 0}

	// Ensure that all the touched accounts are exposed in the account diff.
	for i, addr := range addrs {
		if addr.Hex() != expectedAddrs[i].Hex() || nonces[i] != expectedNonces[i] || balances[i].Cmp(funds) != 0 {
			t.Errorf("incorrect account diff: got (%s, %d, %s), expected (%s, %d, %s)", addr.Hex(), nonces[i], balances[i].String(), expectedAddrs[i].Hex(), expectedNonces[i], funds)
		}
	}
}
