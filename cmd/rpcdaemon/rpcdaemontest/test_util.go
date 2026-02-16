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

package rpcdaemontest

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/binary"
	"fmt"
	"math/big"
	"math/rand"
	"net"
	"sync"
	"testing"

	"github.com/holiman/uint256"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/abi/bind"
	"github.com/erigontech/erigon/execution/abi/bind/backends"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/tests/mock"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/node/privateapi"
	"github.com/erigontech/erigon/rpc/jsonrpc/contracts"
)

type testAddresses struct {
	key      *ecdsa.PrivateKey
	key1     *ecdsa.PrivateKey
	key2     *ecdsa.PrivateKey
	address  common.Address
	address1 common.Address
	address2 common.Address
}

var randSrc = rand.New(rand.NewSource(42)) // fixed seed
var randMu sync.Mutex

var sameStoragePrefixAddresses []common.Address // plain keys with same balanceOf mapping storage mapping as address1

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

func CreateTestSentry(t *testing.T) (*mock.MockSentry, *blockgen.ChainPack, []*blockgen.ChainPack) {
	addresses := makeTestAddresses()
	var (
		key      = addresses.key
		address  = addresses.address
		address1 = addresses.address1
		address2 = addresses.address2
	)

	var (
		gspec = &types.Genesis{
			Config: chain.TestChainConfig,
			Alloc: types.GenesisAlloc{
				address:  {Balance: big.NewInt(9000000000000000000)},
				address1: {Balance: big.NewInt(200000000000000000)},
				address2: {Balance: big.NewInt(300000000000000000)},
			},
			GasLimit: 10000000,
		}
	)
	m := mock.MockWithGenesis(t, gspec, key)

	contractBackend := backends.NewSimulatedBackendWithConfig(t, gspec.Alloc, gspec.Config, gspec.GasLimit)

	// Generate empty chain to have some orphaned blocks for tests
	orphanedChain, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 5, func(i int, block *blockgen.BlockGen) {
	})
	if err != nil {
		t.Fatal(err)
	}

	chain, err := getChainInstance(&addresses, m.ChainConfig, m.Genesis, m.Engine, m.DB, contractBackend)
	if err != nil {
		t.Fatal(err)
	}

	if err = m.InsertChain(orphanedChain); err != nil {
		t.Fatal(err)
	}
	if err = m.InsertChain(chain); err != nil {
		t.Fatal(err)
	}

	return m, chain, []*blockgen.ChainPack{orphanedChain}
}

var chainInstance *blockgen.ChainPack

func getChainInstance(
	addresses *testAddresses,
	config *chain.Config,
	parent *types.Block,
	engine rules.Engine,
	db kv.TemporalRwDB,
	contractBackend *backends.SimulatedBackend,
) (*blockgen.ChainPack, error) {
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
	engine rules.Engine,
	db kv.TemporalRwDB,
	contractBackend *backends.SimulatedBackend,
) (*blockgen.ChainPack, error) {
	var (
		key      = addresses.key
		key1     = addresses.key1
		key2     = addresses.key2
		address  = addresses.address
		address1 = addresses.address1
		address2 = addresses.address2
		theAddr  = common.Address{1}
		chainId  = big.NewInt(1337)
		// this code generates a log
		signer = types.LatestSignerForChainID(nil)
	)

	transactOpts, _ := bind.NewKeyedTransactorWithChainID(key, chainId)
	transactOpts1, _ := bind.NewKeyedTransactorWithChainID(key1, chainId)
	transactOpts2, _ := bind.NewKeyedTransactorWithChainID(key2, chainId)
	var poly *contracts.Poly
	var tokenContract *contracts.Token
	var tokenContract2 *contracts.Token
	var tokenContract2Address common.Address

	// We generate the blocks without plain state because it's not supported in blockgen.GenerateChain
	return blockgen.GenerateChain(config, parent, engine, db, 13, func(i int, block *blockgen.BlockGen) {
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
			var toAddr common.Address
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
			var toAddr common.Address
			for j = 1; j <= 32; j++ {
				binary.BigEndian.PutUint64(toAddr[:], j)
				txn, err = tokenContract.Transfer(transactOpts2, toAddr, big.NewInt(1))
				if err != nil {
					panic(err)
				}
				txs = append(txs, txn)
			}
		case 7:
			var toAddr common.Address
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
			break
		case 11:
			// Mint to address so it has a known balance to drain in the next block
			tokenContract2Address, txn, tokenContract2, err = contracts.DeployToken(transactOpts, contractBackend, address)
			if err != nil {
				panic(err)
			}
			txs = append(txs, txn)
			txn, err = tokenContract2.Mint(transactOpts, address1, big.NewInt(1000))
			if err != nil {
				panic(err)
			}
			txs = append(txs, txn)
			tokenContract2AddrHash := crypto.Keccak256(tokenContract2Address[:])
			balanceStorageKeyPath := computeMappingStorageKey(address1, 1) // balance in slot 1
			// The trie path for storage is keccak256(address) + keccak256(storage_slot)
			hashedBalanceKey := crypto.Keccak256(balanceStorageKeyPath[:])
			fullPath := make([]byte, 64)
			copy(fullPath[:32], tokenContract2AddrHash[:])
			copy(fullPath[32:], hashedBalanceKey[:])
			fmt.Printf("FULL PATH of node about to be deleted: %x\n", fullPath)

			sameStoragePrefixAddresses = findAddressesWithMatchingStorageKeyPrefix(balanceStorageKeyPath, 1, 1, 1)
			sameStorageKeyPath := computeMappingStorageKey(sameStoragePrefixAddresses[0], 1)
			hashedSiblingKey := crypto.Keccak256(sameStorageKeyPath[:])

			fullPathSibling := make([]byte, 64)
			copy(fullPathSibling[:32], tokenContract2AddrHash[:])
			copy(fullPathSibling[32:], hashedSiblingKey[:])
			fmt.Printf("FULL PATH of surviving sibling node: %x\n", fullPathSibling)

			// Assert first nibble of the HASHED storage key is the same (trie path)
			if (hashedSiblingKey[0] >> 4) != (hashedBalanceKey[0] >> 4) {
				panic("hashed storage key prefix mismatch in trie")
			}
			txn, err = tokenContract2.Mint(transactOpts, common.Address(sameStoragePrefixAddresses[0]), big.NewInt(500))
			if err != nil {
				panic(err)
			}
			txs = append(txs, txn)

		case 12:
			// transfer everything out of address1
			txn, err = tokenContract2.Transfer(transactOpts1, common.Address(address), big.NewInt(1000))
			if err != nil {
				panic(err)
			}
			txs = append(txs, txn)

		case 13:
			// Empty block after storage deletes
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

func computeMappingStorageKey(addr common.Address, slot uint64) common.Hash {
	// Create 64-byte buffer: address (32 bytes, left-padded) || slot (32 bytes)
	var buf [64]byte

	// Copy address to bytes 12-31 (left-padded with zeros)
	copy(buf[12:32], addr[:])

	// Write slot number to bytes 56-63 (big-endian, left-padded)
	binary.BigEndian.PutUint64(buf[56:64], slot)

	return crypto.Keccak256Hash(buf[:])
}

// findAddressWithMatchingStorageKeyPrefix finds an address whose computeMappingStorageKey
// result shares the first nNibbles with the target storage key.
// This is useful for creating storage entries that share trie paths to test node collapses.
func findAddressWithMatchingStorageKeyPrefix(targetKey common.Hash, slot uint64, nNibbles int) common.Address {
	// The trie path for a storage slot is keccak256(computeMappingStorageKey(addr, slot)).
	// We need to match the first nNibbles of that hashed value.
	targetHashedKey := crypto.Keccak256Hash(targetKey[:])
	targetNibbles := make([]byte, nNibbles)
	for i := 0; i < nNibbles; i++ {
		if i%2 == 0 {
			targetNibbles[i] = targetHashedKey[i/2] >> 4
		} else {
			targetNibbles[i] = targetHashedKey[i/2] & 0x0f
		}
	}

	var addr common.Address
	for {
		randMu.Lock()
		randSrc.Read(addr[:])
		randMu.Unlock()

		storageKey := computeMappingStorageKey(addr, slot)
		hashedStorageKey := crypto.Keccak256Hash(storageKey[:])

		// Compare nibbles of the hashed storage key (the actual trie path)
		match := true
		for i := 0; i < nNibbles; i++ {
			var nibble byte
			if i%2 == 0 {
				nibble = hashedStorageKey[i/2] >> 4
			} else {
				nibble = hashedStorageKey[i/2] & 0x0f
			}
			if nibble != targetNibbles[i] {
				match = false
				break
			}
		}
		if match {
			return addr
		}
	}
}

// findAddressesWithMatchingStorageKeyPrefix finds multiple addresses whose storage keys
// share the first nNibbles with the target, useful for populating a trie subtree.
func findAddressesWithMatchingStorageKeyPrefix(targetKey common.Hash, slot uint64, nNibbles int, count int) []common.Address {
	addresses := make([]common.Address, 0, count)
	seen := make(map[common.Address]bool)

	for len(addresses) < count {
		addr := findAddressWithMatchingStorageKeyPrefix(targetKey, slot, nNibbles)
		if !seen[addr] {
			seen[addr] = true
			addresses = append(addresses, addr)
		}
	}
	return addresses
}
func generateKeyWithHashedPrefix(constHashedPrefixNibbles []byte, keyLen int) (plainKey []byte, hashedKey []byte) {
	plainKey = make([]byte, keyLen)
	for {
		randMu.Lock()
		randSrc.Read(plainKey[:keyLen]) // read random key
		randMu.Unlock()
		hashedKey := commitment.KeyToNibblizedHash(plainKey)
		if bytes.HasPrefix(hashedKey, constHashedPrefixNibbles) {
			// found key with desired hashed prefix, return result
			return plainKey, hashedKey
		}
	}
}

// longer prefixLen - harder to find required keys
func generatePlainKeysWithSameHashPrefix(constPrefixNibbles []byte, keyLen int, prefixLen int, keyCount int) (plainKeys [][]byte, hashedKeys [][]byte) {
	plainKeys = make([][]byte, 0, keyCount)
	hashedKeys = make([][]byte, 0, keyCount)
	for {
		key, hashed := generateKeyWithHashedPrefix(constPrefixNibbles, keyLen)
		if len(plainKeys) == 0 {
			plainKeys = append(plainKeys, key)
			hashedKeys = append(hashedKeys, hashed)
			if keyCount == 1 {
				break
			}
			continue
		}
		if bytes.Equal(hashed[:prefixLen], hashedKeys[0][:prefixLen]) {
			plainKeys = append(plainKeys, key)
			hashedKeys = append(hashedKeys, hashed)
		}
		if len(plainKeys) == keyCount {
			break
		}
	}
	return plainKeys, hashedKeys
}

type IsMiningMock struct{}

func (*IsMiningMock) IsMining() bool { return false }

func CreateTestGrpcConn(t *testing.T, m *mock.MockSentry) (context.Context, *grpc.ClientConn) { //nolint
	ctx, cancel := context.WithCancel(context.Background())

	apis := m.Engine.APIs(nil)
	if len(apis) < 1 {
		t.Fatal("couldn't instantiate Engine api")
	}

	ethashApi := apis[1].Service.(*ethash.API)
	server := grpc.NewServer()

	remoteproto.RegisterETHBACKENDServer(server, privateapi.NewEthBackendServer(ctx, nil, m.DB, m.Notifications,
		m.BlockReader, nil, log.New(), builder.NewLatestBlockBuiltStore(), nil))
	txpoolproto.RegisterTxpoolServer(server, m.TxPoolGrpcServer)
	txpoolproto.RegisterMiningServer(server, privateapi.NewMiningServer(ctx, &IsMiningMock{}, ethashApi, m.Log))
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

func CreateTestSentryForTraces(t *testing.T) *mock.MockSentry {
	var (
		a0 = common.HexToAddress("0x00000000000000000000000000000000000000ff")
		a1 = common.HexToAddress("0x00000000000000000000000000000000000001ff")
		a2 = common.HexToAddress("0x00000000000000000000000000000000000002ff")
		// Generate a canonical chain to act as the main dataset

		// A sender who makes transactions, has some funds
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = crypto.PubkeyToAddress(key.PublicKey)
		funds   = big.NewInt(1000000000)
		gspec   = &types.Genesis{
			Config: chain.TestChainConfig,
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
	m := mock.MockWithGenesis(t, gspec, key)
	chain, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
		// One transaction to AAAA
		tx, _ := types.SignTx(types.NewTransaction(0, a2,
			&u256.Num0, 50000, &u256.Num1, []byte{0x01, 0x00, 0x01, 0x00}), *types.LatestSignerForChainID(nil), key)
		b.AddTx(tx)
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	if err := m.InsertChain(chain); err != nil {
		t.Fatalf("failed to insert into chain: %v", err)
	}
	return m
}

func CreateTestSentryForTracesCollision(t *testing.T) *mock.MockSentry {
	var (
		// Generate a canonical chain to act as the main dataset
		// A sender who makes transactions, has some funds
		key, _    = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address   = crypto.PubkeyToAddress(key.PublicKey)
		funds     = big.NewInt(1000000000)
		bb        = common.HexToAddress("0x000000000000000000000000000000000000bbbb")
		aaStorage = make(map[common.Hash]common.Hash)          // Initial storage in AA
		aaCode    = []byte{byte(vm.PC), byte(vm.SELFDESTRUCT)} // Code for AA (simple selfdestruct)
	)
	// Populate two slots
	aaStorage[common.HexToHash("01")] = common.HexToHash("01")
	aaStorage[common.HexToHash("02")] = common.HexToHash("02")

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
	bbCode := make([]byte, 0, 1+len(initCode)+12)
	bbCode = append(bbCode,
		// Push initcode onto stack
		byte(vm.PUSH1)+byte(len(initCode)-1))
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

	initHash := accounts.InternCodeHash(crypto.Keccak256Hash(initCode))
	aa := types.CreateAddress2(bb, [32]byte{}, initHash)
	t.Logf("Destination address: %x\n", aa)

	gspec := &types.Genesis{
		Config: chain.TestChainConfig,
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
	m := mock.MockWithGenesis(t, gspec, key)
	chain, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
		// One transaction to AA, to kill it
		tx, _ := types.SignTx(types.NewTransaction(0, aa,
			&u256.Num0, 50000, &u256.Num1, nil), *types.LatestSignerForChainID(nil), key)
		b.AddTx(tx)
		// One transaction to BB, to recreate AA
		tx, _ = types.SignTx(types.NewTransaction(1, bb,
			&u256.Num0, 100000, &u256.Num1, nil), *types.LatestSignerForChainID(nil), key)
		b.AddTx(tx)
		tx, _ = types.SignTx(types.NewTransaction(2, bb,
			&u256.Num0, 100000, &u256.Num1, nil), *types.LatestSignerForChainID(nil), key)
		b.AddTx(tx)
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}
	// Import the canonical chain
	if err := m.InsertChain(chain); err != nil {
		t.Fatalf("failed to insert into chain: %v", err)
	}

	fmt.Println(chain.Blocks[0].Transactions()[2].Hash().Hex())

	return m
}
