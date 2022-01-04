package rpcdaemontest

import (
	"context"
	"encoding/binary"
	"math/big"
	"net"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/accounts/abi/bind"
	"github.com/ledgerwatch/erigon/accounts/abi/bind/backends"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/commands/contracts"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/ethdb/privateapi"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/stages"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

func CreateTestKV(t *testing.T) kv.RwDB {
	s, _, _ := CreateTestSentry(t)
	return s.DB
}

func CreateTestSentry(t *testing.T) (*stages.MockSentry, *core.ChainPack, []*core.ChainPack) {
	// Configure and generate a sample block chain
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
			GasLimit: 10000000,
		}
		chainId = big.NewInt(1337)
		// this code generates a log
		signer = types.LatestSignerForChainID(nil)
	)
	m := stages.MockWithGenesis(t, gspec, key)

	contractBackend := backends.NewSimulatedBackendWithConfig(gspec.Alloc, gspec.Config, gspec.GasLimit)
	defer contractBackend.Close()

	transactOpts, _ := bind.NewKeyedTransactorWithChainID(key, chainId)
	transactOpts1, _ := bind.NewKeyedTransactorWithChainID(key1, chainId)
	transactOpts2, _ := bind.NewKeyedTransactorWithChainID(key2, chainId)
	var poly *contracts.Poly

	var err error
	var tokenContract *contracts.Token
	// Generate empty chain to have some orphaned blocks for tests
	orphanedChain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 5, func(i int, block *core.BlockGen) {
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	// We generate the blocks without plainstant because it's not supported in core.GenerateChain
	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 10, func(i int, block *core.BlockGen) {
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
	}, true)
	if err != nil {
		t.Fatal(err)
	}

	if err = m.InsertChain(orphanedChain); err != nil {
		t.Fatal(err)
	}
	if err = m.InsertChain(chain); err != nil {
		t.Fatal(err)
	}

	return m, chain, []*core.ChainPack{orphanedChain}
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

	remote.RegisterETHBACKENDServer(server, privateapi.NewEthBackendServer(ctx, nil, m.DB, m.Notifications.Events, snapshotsync.NewBlockReader(), nil, nil, nil, nil, nil, nil, false))
	txpool.RegisterTxpoolServer(server, m.TxPoolGrpcServer)
	txpool.RegisterMiningServer(server, privateapi.NewMiningServer(ctx, &IsMiningMock{}, ethashApi))
	listener := bufconn.Listen(1024 * 1024)

	dialer := func() func(context.Context, string) (net.Conn, error) {
		go func() {
			if err := server.Serve(listener); err != nil {
				panic(err)
			}
		}()
		return func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}
	}

	conn, err := grpc.DialContext(ctx, "", grpc.WithInsecure(), grpc.WithContextDialer(dialer()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		conn.Close()
	})
	return ctx, conn
}
