package rpcdaemontest

import (
	"context"
	"crypto/ecdsa"
	"encoding/binary"
	"fmt"
	"math/big"
	"net"
	"testing"

	"github.com/holiman/uint256"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/accounts/abi/bind"
	"github.com/ledgerwatch/erigon/accounts/abi/bind/backends"
	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/ethdb/privateapi"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/jsonrpc/contracts"
	"github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/ledgerwatch/log/v3"
)

type testAddresses struct {
	key      *ecdsa.PrivateKey
	key1     *ecdsa.PrivateKey
	key2     *ecdsa.PrivateKey
	address  libcommon.Address
	address1 libcommon.Address
	address2 libcommon.Address
}

func makeTestAddresses() testAddresses {
	var (
		key, _   = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		key1, _  = crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
		key2, _  = crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
		address  = crypto.PubkeyToAddress(key.PublicKey)
		address1 = crypto.PubkeyToAddress(key1.PublicKey)
		address2 = crypto.PubkeyToAddress(key2.PublicKey)
	)

	return testAddresses{
		key:      key,
		key1:     key1,
		key2:     key2,
		address:  address,
		address1: address1,
		address2: address2,
	}
}

func CreateTestSentry(t *testing.T) (*stages.MockSentry, *core.ChainPack, []*core.ChainPack) {
	addresses := makeTestAddresses()
	var (
		key      = addresses.key
		address  = addresses.address
		address1 = addresses.address1
		address2 = addresses.address2
	)

	var (
		gspec = &types.Genesis{
			Config: params.TestChainConfig,
			Alloc: types.GenesisAlloc{
				address:  {Balance: big.NewInt(9000000000000000000)},
				address1: {Balance: big.NewInt(200000000000000000)},
				address2: {Balance: big.NewInt(300000000000000000)},
			},
			GasLimit: 10000000,
		}
	)
	m := stages.MockWithGenesis(t, gspec, key, false)

	contractBackend := backends.NewTestSimulatedBackendWithConfig(t, gspec.Alloc, gspec.Config, gspec.GasLimit)
	defer contractBackend.Close()

	// Generate empty chain to have some orphaned blocks for tests
	orphanedChain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 5, func(i int, block *core.BlockGen) {
	})
	if err != nil {
		t.Fatal(err)
	}

	chain, err := getChainInstance(&addresses, m.ChainConfig, m.Genesis, m.Engine, m.DB, contractBackend)
	if err != nil {
		t.Fatal(err)
	}

	if err = m.InsertChain(orphanedChain, nil); err != nil {
		t.Fatal(err)
	}
	if err = m.InsertChain(chain, nil); err != nil {
		t.Fatal(err)
	}

	return m, chain, []*core.ChainPack{orphanedChain}
}

var chainInstance *core.ChainPack

func getChainInstance(
	addresses *testAddresses,
	config *chain.Config,
	parent *types.Block,
	engine consensus.Engine,
	db kv.RwDB,
	contractBackend *backends.SimulatedBackend,
) (*core.ChainPack, error) {
	var err error
	if chainInstance == nil {
		chainInstance, err = generateChain(addresses, config, parent, engine, db, contractBackend)
	}
	return chainInstance.Copy(), err
}

func generateChain(
	addresses *testAddresses,
	config *chain.Config,
	parent *types.Block,
	engine consensus.Engine,
	db kv.RwDB,
	contractBackend *backends.SimulatedBackend,
) (*core.ChainPack, error) {
	var (
		key      = addresses.key
		key1     = addresses.key1
		key2     = addresses.key2
		address  = addresses.address
		address1 = addresses.address1
		address2 = addresses.address2
		theAddr  = libcommon.Address{1}
		chainId  = big.NewInt(1337)
		// this code generates a log
		signer = types.LatestSignerForChainID(nil)
	)

	transactOpts, _ := bind.NewKeyedTransactorWithChainID(key, chainId)
	transactOpts1, _ := bind.NewKeyedTransactorWithChainID(key1, chainId)
	transactOpts2, _ := bind.NewKeyedTransactorWithChainID(key2, chainId)
	var poly *contracts.Poly
	var tokenContract *contracts.Token

	// We generate the blocks without plain state because it's not supported in core.GenerateChain
	return core.GenerateChain(config, parent, engine, db, 11, func(i int, block *core.BlockGen) {
		var (
			txn types.Transaction
			txs []types.Transaction
			err error
		)

		ctx := context.Background()
		switch i {
		case 0:
			txn, err = types.SignTx(types.NewTransaction(0, theAddr, uint256.NewInt(1000000000000000), 21000, new(uint256.Int), nil), *signer, key)
			if err != nil {
				panic(err)
			}
			err = contractBackend.SendTransaction(ctx, txn)
			if err != nil {
				panic(err)
			}
		case 1:
			txn, err = types.SignTx(types.NewTransaction(1, theAddr, uint256.NewInt(1000000000000000), 21000, new(uint256.Int), nil), *signer, key)
			if err != nil {
				panic(err)
			}
			err = contractBackend.SendTransaction(ctx, txn)
			if err != nil {
				panic(err)
			}
		case 2:
			_, txn, tokenContract, err = contracts.DeployToken(transactOpts, contractBackend, address1)
		case 3:
			txn, err = tokenContract.Mint(transactOpts1, address2, big.NewInt(10))
		case 4:
			txn, err = tokenContract.Transfer(transactOpts2, address, big.NewInt(3))
		case 5:
			// Multiple transactions sending small amounts of ether to various accounts
			var j uint64
			var toAddr libcommon.Address
			nonce := block.TxNonce(address)
			for j = 1; j <= 32; j++ {
				binary.BigEndian.PutUint64(toAddr[:], j)
				txn, err = types.SignTx(types.NewTransaction(nonce, toAddr, uint256.NewInt(1_000_000_000_000_000), 21000, new(uint256.Int), nil), *signer, key)
				if err != nil {
					panic(err)
				}
				err = contractBackend.SendTransaction(ctx, txn)
				if err != nil {
					panic(err)
				}
				txs = append(txs, txn)
				nonce++
			}
		case 6:
			_, txn, tokenContract, err = contracts.DeployToken(transactOpts, contractBackend, address1)
			if err != nil {
				panic(err)
			}
			txs = append(txs, txn)
			txn, err = tokenContract.Mint(transactOpts1, address2, big.NewInt(100))
			if err != nil {
				panic(err)
			}
			txs = append(txs, txn)
			// Multiple transactions sending small amounts of ether to various accounts
			var j uint64
			var toAddr libcommon.Address
			for j = 1; j <= 32; j++ {
				binary.BigEndian.PutUint64(toAddr[:], j)
				txn, err = tokenContract.Transfer(transactOpts2, toAddr, big.NewInt(1))
				if err != nil {
					panic(err)
				}
				txs = append(txs, txn)
			}
		case 7:
			var toAddr libcommon.Address
			nonce := block.TxNonce(address)
			binary.BigEndian.PutUint64(toAddr[:], 4)
			txn, err = types.SignTx(types.NewTransaction(nonce, toAddr, uint256.NewInt(1000000000000000), 21000, new(uint256.Int), nil), *signer, key)
			if err != nil {
				panic(err)
			}
			err = contractBackend.SendTransaction(ctx, txn)
			if err != nil {
				panic(err)
			}
			txs = append(txs, txn)
			binary.BigEndian.PutUint64(toAddr[:], 12)
			txn, err = tokenContract.Transfer(transactOpts2, toAddr, big.NewInt(1))
			if err != nil {
				panic(err)
			}
			txs = append(txs, txn)
		case 8:
			_, txn, poly, err = contracts.DeployPoly(transactOpts, contractBackend)
			if err != nil {
				panic(err)
			}
			txs = append(txs, txn)
		case 9:
			txn, err = poly.DeployAndDestruct(transactOpts, big.NewInt(0))
			if err != nil {
				panic(err)
			}
			txs = append(txs, txn)
		case 10:
			// Empty block
			break
		}

		if err != nil {
			panic(err)
		}
		if txs == nil && txn != nil {
			txs = append(txs, txn)
		}

		for _, txn := range txs {
			block.AddTx(txn)
		}
		contractBackend.Commit()
	})
}

type IsMiningMock struct{}

func (*IsMiningMock) IsMining() bool { return false }

func CreateTestGrpcConn(t *testing.T, m *stages.MockSentry) (context.Context, *grpc.ClientConn) { //nolint
	ctx, cancel := context.WithCancel(context.Background())

	apis := m.Engine.APIs(nil)
	if len(apis) < 1 {
		t.Fatal("couldn't instantiate Engine api")
	}

	ethashApi := apis[1].Service.(*ethash.API)
	server := grpc.NewServer()

	remote.RegisterETHBACKENDServer(server, privateapi.NewEthBackendServer(ctx, nil, m.DB, m.Notifications.Events,
		m.BlockReader, log.New()))
	txpool.RegisterTxpoolServer(server, m.TxPoolGrpcServer)
	txpool.RegisterMiningServer(server, privateapi.NewMiningServer(ctx, &IsMiningMock{}, ethashApi, m.Log))
	listener := bufconn.Listen(1024 * 1024)

	dialer := func() func(context.Context, string) (net.Conn, error) {
		go func() {
			if err := server.Serve(listener); err != nil {
				fmt.Printf("%v\n", err)
			}
		}()
		return func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}
	}

	conn, err := grpc.DialContext(ctx, "", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(dialer()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		conn.Close()
		server.Stop()
	})
	return ctx, conn
}

func CreateTestSentryForTraces(t *testing.T) *stages.MockSentry {
	var (
		a0 = libcommon.HexToAddress("0x00000000000000000000000000000000000000ff")
		a1 = libcommon.HexToAddress("0x00000000000000000000000000000000000001ff")
		a2 = libcommon.HexToAddress("0x00000000000000000000000000000000000002ff")
		// Generate a canonical chain to act as the main dataset

		// A sender who makes transactions, has some funds
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = crypto.PubkeyToAddress(key.PublicKey)
		funds   = big.NewInt(1000000000)
		gspec   = &types.Genesis{
			Config: params.TestChainConfig,
			Alloc: types.GenesisAlloc{
				address: {Balance: funds},
				// The address 0x00ff
				a0: {
					Code: []byte{
						byte(vm.CALLDATASIZE),
						byte(vm.PUSH1), 0x00,
						byte(vm.PUSH1), 0x00,
						byte(vm.CALLDATACOPY), // Copy all call data into memory
						byte(vm.CALLDATASIZE), // call data size becomes length of the return value
						byte(vm.PUSH1), 0x00,
						byte(vm.RETURN),
					},
					Nonce:   1,
					Balance: big.NewInt(0),
				},
				// The address 0x01ff
				a1: {
					Code: []byte{
						byte(vm.CALLDATASIZE),
						byte(vm.PUSH1), 0x00,
						byte(vm.PUSH1), 0x00,
						byte(vm.CALLDATACOPY), // Copy all call data into memory
						// Prepare arguments for the CALL
						byte(vm.CALLDATASIZE), // retLength == call data length
						byte(vm.PUSH1), 0x00,  // retOffset == 0
						byte(vm.PUSH1), 0x01, byte(vm.CALLDATASIZE), byte(vm.SUB), // argLength == call data length - 1
						byte(vm.PUSH1), 0x01, // argOffset == 1
						byte(vm.PUSH1), 0x00, // value == 0
						// take first byte from the input, shift 240 bits to the right, and add 0xff, to form the address
						byte(vm.PUSH1), 0x00, byte(vm.MLOAD), byte(vm.PUSH1), 240, byte(vm.SHR), byte(vm.PUSH1), 0xff, byte(vm.OR),
						byte(vm.GAS),
						byte(vm.CALL),
						byte(vm.RETURNDATASIZE), // return data size becomes length of the return value
						byte(vm.PUSH1), 0x00,
						byte(vm.RETURN),
					},
					Nonce:   1,
					Balance: big.NewInt(0),
				},
				// The address 0x02ff
				a2: {
					Code: []byte{
						byte(vm.CALLDATASIZE),
						byte(vm.PUSH1), 0x00,
						byte(vm.PUSH1), 0x00,
						byte(vm.CALLDATACOPY), // Copy all call data into memory
						// Prepare arguments for the CALL
						byte(vm.CALLDATASIZE), // retLength == call data length
						byte(vm.PUSH1), 0x00,  // retOffset == 0
						byte(vm.PUSH1), 0x01, byte(vm.CALLDATASIZE), byte(vm.SUB), // argLength == call data length - 1
						byte(vm.PUSH1), 0x01, // argOffset == 1
						byte(vm.PUSH1), 0x00, // value == 0
						// take first byte from the input, shift 240 bits to the right, and add 0xff, to form the address
						byte(vm.PUSH1), 0x00, byte(vm.MLOAD), byte(vm.PUSH1), 240, byte(vm.SHR), byte(vm.PUSH1), 0xff, byte(vm.OR),
						byte(vm.GAS),
						byte(vm.CALL),

						// Prepare arguments for the CALL
						byte(vm.RETURNDATASIZE), // retLength == call data length
						byte(vm.PUSH1), 0x00,    // retOffset == 0
						byte(vm.PUSH1), 0x01, byte(vm.RETURNDATASIZE), byte(vm.SUB), // argLength == call data length - 1
						byte(vm.PUSH1), 0x01, // argOffset == 1
						byte(vm.PUSH1), 0x00, // value == 0
						// take first byte from the input, shift 240 bits to the right, and add 0xff, to form the address
						byte(vm.PUSH1), 0x00, byte(vm.MLOAD), byte(vm.PUSH1), 240, byte(vm.SHR), byte(vm.PUSH1), 0xff, byte(vm.OR),
						byte(vm.GAS),
						byte(vm.CALL),

						byte(vm.RETURNDATASIZE), // return data size becomes length of the return value
						byte(vm.PUSH1), 0x00,
						byte(vm.RETURN),
					},
					Nonce:   1,
					Balance: big.NewInt(0),
				},
			},
		}
	)
	m := stages.MockWithGenesis(t, gspec, key, false)
	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, b *core.BlockGen) {
		b.SetCoinbase(libcommon.Address{1})
		// One transaction to AAAA
		tx, _ := types.SignTx(types.NewTransaction(0, a2,
			u256.Num0, 50000, u256.Num1, []byte{0x01, 0x00, 0x01, 0x00}), *types.LatestSignerForChainID(nil), key)
		b.AddTx(tx)
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	if err := m.InsertChain(chain, nil); err != nil {
		t.Fatalf("failed to insert into chain: %v", err)
	}
	return m
}

func CreateTestSentryForTracesCollision(t *testing.T) *stages.MockSentry {
	var (
		// Generate a canonical chain to act as the main dataset
		// A sender who makes transactions, has some funds
		key, _    = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address   = crypto.PubkeyToAddress(key.PublicKey)
		funds     = big.NewInt(1000000000)
		bb        = libcommon.HexToAddress("0x000000000000000000000000000000000000bbbb")
		aaStorage = make(map[libcommon.Hash]libcommon.Hash)    // Initial storage in AA
		aaCode    = []byte{byte(vm.PC), byte(vm.SELFDESTRUCT)} // Code for AA (simple selfdestruct)
	)
	// Populate two slots
	aaStorage[libcommon.HexToHash("01")] = libcommon.HexToHash("01")
	aaStorage[libcommon.HexToHash("02")] = libcommon.HexToHash("02")

	// The bb-code needs to CREATE2 the aa contract. It consists of
	// both initcode and deployment code
	// initcode:
	// 1. Set slots 3=3, 4=4,
	// 2. Return aaCode

	initCode := []byte{
		byte(vm.PUSH1), 0x3, // value
		byte(vm.PUSH1), 0x3, // location
		byte(vm.SSTORE),     // Set slot[3] = 3
		byte(vm.PUSH1), 0x4, // value
		byte(vm.PUSH1), 0x4, // location
		byte(vm.SSTORE), // Set slot[4] = 4
		// Slots are set, now return the code
		byte(vm.PUSH2), byte(vm.PC), byte(vm.SELFDESTRUCT), // Push code on stack
		byte(vm.PUSH1), 0x0, // memory start on stack
		byte(vm.MSTORE),
		// Code is now in memory.
		byte(vm.PUSH1), 0x2, // size
		byte(vm.PUSH1), byte(32 - 2), // offset
		byte(vm.RETURN),
	}
	if l := len(initCode); l > 32 {
		t.Fatalf("init code is too long for a pushx, need a more elaborate deployer")
	}
	bbCode := []byte{
		// Push initcode onto stack
		byte(vm.PUSH1) + byte(len(initCode)-1)}
	bbCode = append(bbCode, initCode...)
	bbCode = append(bbCode, []byte{
		byte(vm.PUSH1), 0x0, // memory start on stack
		byte(vm.MSTORE),
		byte(vm.PUSH1), 0x00, // salt
		byte(vm.PUSH1), byte(len(initCode)), // size
		byte(vm.PUSH1), byte(32 - len(initCode)), // offset
		byte(vm.PUSH1), 0x00, // endowment
		byte(vm.CREATE2),
	}...)

	initHash := crypto.Keccak256Hash(initCode)
	aa := crypto.CreateAddress2(bb, [32]byte{}, initHash[:])
	t.Logf("Destination address: %x\n", aa)

	gspec := &types.Genesis{
		Config: params.TestChainConfig,
		Alloc: types.GenesisAlloc{
			address: {Balance: funds},
			// The address 0xAAAAA selfdestructs if called
			aa: {
				// Code needs to just selfdestruct
				Code:    aaCode,
				Nonce:   1,
				Balance: big.NewInt(0),
				Storage: aaStorage,
			},
			// The contract BB recreates AA
			bb: {
				Code:    bbCode,
				Balance: big.NewInt(1),
			},
			bb: {
				Code:    bbCode,
				Balance: big.NewInt(1),
			},
		},
	}
	m := stages.MockWithGenesis(t, gspec, key, false)
	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, b *core.BlockGen) {
		b.SetCoinbase(libcommon.Address{1})
		// One transaction to AA, to kill it
		tx, _ := types.SignTx(types.NewTransaction(0, aa,
			u256.Num0, 50000, u256.Num1, nil), *types.LatestSignerForChainID(nil), key)
		b.AddTx(tx)
		// One transaction to BB, to recreate AA
		tx, _ = types.SignTx(types.NewTransaction(1, bb,
			u256.Num0, 100000, u256.Num1, nil), *types.LatestSignerForChainID(nil), key)
		b.AddTx(tx)
		tx, _ = types.SignTx(types.NewTransaction(2, bb,
			u256.Num0, 100000, u256.Num1, nil), *types.LatestSignerForChainID(nil), key)
		b.AddTx(tx)
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}
	// Import the canonical chain
	if err := m.InsertChain(chain, nil); err != nil {
		t.Fatalf("failed to insert into chain: %v", err)
	}

	fmt.Println(chain.Blocks[0].Transactions()[2].Hash().Hex())

	return m
}
