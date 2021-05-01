package commands

import (
	"context"
	"encoding/binary"
	"math/big"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/accounts/abi/bind"
	"github.com/ledgerwatch/turbo-geth/accounts/abi/bind/backends"
	"github.com/ledgerwatch/turbo-geth/cmd/rpcdaemon/commands/contracts"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
)

func createTestDb() (ethdb.Database, error) {
	// Configure and generate a sample block chain
	db := ethdb.NewMemDatabase()
	var (
		key, _   = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		key1, _  = crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
		key2, _  = crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
		address  = crypto.PubkeyToAddress(key.PublicKey)
		address1 = crypto.PubkeyToAddress(key1.PublicKey)
		address2 = crypto.PubkeyToAddress(key2.PublicKey)
		theAddr  = common.Address{1}
		gspec    = &core.Genesis{
			Config: params.AllEthashProtocolChanges,
			Alloc: core.GenesisAlloc{
				address:  {Balance: big.NewInt(9000000000000000000)},
				address1: {Balance: big.NewInt(200000000000000000)},
				address2: {Balance: big.NewInt(300000000000000000)},
			},
		}
		chainId = big.NewInt(1337)
		// this code generates a log
		signer = types.LatestSignerForChainID(nil)
	)
	// Create intermediate hash bucket since it is mandatory now
	_, genesisHash, err := core.SetupGenesisBlock(db, gspec, true, false)
	if err != nil {
		return nil, err
	}
	genesis := rawdb.ReadBlockDeprecated(db, genesisHash, 0)

	engine := ethash.NewFaker()

	contractBackend := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	defer contractBackend.Close()

	transactOpts, _ := bind.NewKeyedTransactorWithChainID(key, chainId)
	transactOpts1, _ := bind.NewKeyedTransactorWithChainID(key1, chainId)
	transactOpts2, _ := bind.NewKeyedTransactorWithChainID(key2, chainId)
	var poly *contracts.Poly

	var tokenContract *contracts.Token
	// We generate the blocks without plainstant because it's not supported in core.GenerateChain
	blocks, _, err := core.GenerateChain(gspec.Config, genesis, engine, db, 10, func(i int, block *core.BlockGen) {
		var (
			tx  types.Transaction
			txs []types.Transaction
		)

		ctx := gspec.Config.WithEIPsFlags(context.Background(), block.Number().Uint64())
		switch i {
		case 0:
			tx, err = types.SignTx(types.NewTransaction(0, theAddr, uint256.NewInt().SetUint64(1000000000000000), 21000, new(uint256.Int), nil), *signer, key)
			err = contractBackend.SendTransaction(ctx, tx)
			if err != nil {
				panic(err)
			}
		case 1:
			tx, err = types.SignTx(types.NewTransaction(1, theAddr, uint256.NewInt().SetUint64(1000000000000000), 21000, new(uint256.Int), nil), *signer, key)
			err = contractBackend.SendTransaction(ctx, tx)
			if err != nil {
				panic(err)
			}
		case 2:
			_, tx, tokenContract, err = contracts.DeployToken(transactOpts, contractBackend, address1)
		case 3:
			tx, err = tokenContract.Mint(transactOpts1, address2, big.NewInt(10))
		case 4:
			tx, err = tokenContract.Transfer(transactOpts2, address, big.NewInt(3))
		case 5:
			// Multiple transactions sending small amounts of ether to various accounts
			var j uint64
			var toAddr common.Address
			nonce := block.TxNonce(address)
			for j = 1; j <= 32; j++ {
				binary.BigEndian.PutUint64(toAddr[:], j)
				tx, err = types.SignTx(types.NewTransaction(nonce, toAddr, uint256.NewInt().SetUint64(1000000000000000), 21000, new(uint256.Int), nil), *signer, key)
				if err != nil {
					panic(err)
				}
				err = contractBackend.SendTransaction(ctx, tx)
				if err != nil {
					panic(err)
				}
				txs = append(txs, tx)
				nonce++
			}
		case 6:
			_, tx, tokenContract, err = contracts.DeployToken(transactOpts, contractBackend, address1)
			if err != nil {
				panic(err)
			}
			txs = append(txs, tx)
			tx, err = tokenContract.Mint(transactOpts1, address2, big.NewInt(100))
			if err != nil {
				panic(err)
			}
			txs = append(txs, tx)
			// Multiple transactions sending small amounts of ether to various accounts
			var j uint64
			var toAddr common.Address
			for j = 1; j <= 32; j++ {
				binary.BigEndian.PutUint64(toAddr[:], j)
				tx, err = tokenContract.Transfer(transactOpts2, toAddr, big.NewInt(1))
				if err != nil {
					panic(err)
				}
				txs = append(txs, tx)
			}
		case 7:
			var toAddr common.Address
			nonce := block.TxNonce(address)
			binary.BigEndian.PutUint64(toAddr[:], 4)
			tx, err = types.SignTx(types.NewTransaction(nonce, toAddr, uint256.NewInt().SetUint64(1000000000000000), 21000, new(uint256.Int), nil), *signer, key)
			if err != nil {
				panic(err)
			}
			err = contractBackend.SendTransaction(ctx, tx)
			if err != nil {
				panic(err)
			}
			txs = append(txs, tx)
			binary.BigEndian.PutUint64(toAddr[:], 12)
			tx, err = tokenContract.Transfer(transactOpts2, toAddr, big.NewInt(1))
			if err != nil {
				panic(err)
			}
			txs = append(txs, tx)
		case 8:
			_, tx, poly, err = contracts.DeployPoly(transactOpts, contractBackend)
			if err != nil {
				panic(err)
			}
			txs = append(txs, tx)
		case 9:
			tx, err = poly.DeployAndDestruct(transactOpts, big.NewInt(0))
			if err != nil {
				panic(err)
			}
			txs = append(txs, tx)
		}

		if err != nil {
			panic(err)
		}
		if txs == nil && tx != nil {
			txs = append(txs, tx)
		}

		for _, tx := range txs {
			block.AddTx(tx)
		}
		contractBackend.Commit()
	}, true)
	if err != nil {
		return nil, err
	}

	if _, err = stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, gspec.Config, &vm.Config{}, engine, blocks, true /* rootCheck */); err != nil {
		return nil, err
	}

	return db, nil
}

func createTestKV() (ethdb.RwKV, error) {
	db, err := createTestDb()

	if err != nil {
		return nil, err
	}

	return db.(ethdb.HasRwKV).RwKV(), nil
}
