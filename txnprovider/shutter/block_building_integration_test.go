// Copyright 2025 The Erigon Authors
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

//go:build integration

package shutter_test

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/downloader/downloadercfg"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	executionclient "github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cmd/devnet/requests"
	"github.com/erigontech/erigon/cmd/rpcdaemon/cli"
	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/node"
	"github.com/erigontech/erigon/node/nodecfg"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/turbo/testlog"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

func TestShutterBlockBuilding(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := testlog.Logger(t, log.LvlDebug)
	dataDir := t.TempDir()
	dirs := datadir.New(dataDir)
	p2pPort, cleanP2pPort := ConsumeFreeTcpPort(t)
	defer cleanP2pPort()
	engineApiPort, cleanEngineApiPort := ConsumeFreeTcpPort(t)
	defer cleanEngineApiPort()
	jsonRpcPort, cleanJsonRpcPort := ConsumeFreeTcpPort(t)
	defer cleanJsonRpcPort()

	const localhost = "127.0.0.1"
	httpConfig := httpcfg.HttpCfg{
		Enabled:                  true,
		HttpServerEnabled:        true,
		HttpListenAddress:        localhost,
		HttpPort:                 jsonRpcPort,
		API:                      []string{"eth"},
		AuthRpcHTTPListenAddress: localhost,
		AuthRpcPort:              engineApiPort,
	}

	nodeConfig := nodecfg.Config{
		Dirs: dirs,
		Http: httpConfig,
		P2P: p2p.Config{
			ListenAddr:      fmt.Sprintf("127.0.0.1:%d", p2pPort),
			MaxPeers:        1,
			MaxPendingPeers: 1,
			NoDiscovery:     true,
			NoDial:          true,
		},
	}

	txPoolConfig := txpoolcfg.DefaultConfig
	txPoolConfig.DBDir = dirs.TxPool

	ethConfig := ethconfig.Config{
		Dirs: dirs,
		Downloader: &downloadercfg.Cfg{
			Dirs: dirs,
		},
		TxPool: txPoolConfig,
	}

	stack, err := node.New(ctx, &nodeConfig, logger)
	require.NoError(t, err)

	bankPrivKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	bank := NativeTokenBank{privKey: bankPrivKey}
	chainConfig := *params.ChiadoChainConfig
	chainConfig.ChainName = "shutter-devnet"
	chainConfig.ChainID = big.NewInt(987656789)
	genesis := core.ChiadoGenesisBlock()
	genesis.Config = &chainConfig
	genesis.Alloc[bank.Address()] = types.GenesisAccount{
		Balance: big.NewInt(100_000_000_000),
	}
	chainDB, err := node.OpenDatabase(ctx, stack.Config(), kv.ChainDB, "", false, logger)
	require.NoError(t, err)
	_, _, err = core.CommitGenesisBlock(chainDB, genesis, stack.Config().Dirs, logger)
	require.NoError(t, err)
	chainDB.Close()

	ethBackend, err := eth.New(ctx, stack, &ethConfig, logger)
	require.NoError(t, err)

	err = ethBackend.Init(stack, &ethConfig, ethBackend.ChainConfig())
	require.NoError(t, err)

	err = ethBackend.Start()
	require.NoError(t, err)

	jwtSecret, err := cli.ObtainJWTSecret(&httpConfig, logger)
	require.NoError(t, err)
	_, err = executionclient.NewExecutionClientRPC(jwtSecret, httpConfig.AuthRpcHTTPListenAddress, httpConfig.AuthRpcPort)
	require.NoError(t, err)

	rpcClient := requests.NewRequestGenerator(fmt.Sprintf("%s:%d", httpConfig.HttpListenAddress, httpConfig.HttpPort), logger)
	_, err = rpcClient.BlockNumber()
	require.NoError(t, err)

	time.Sleep(10 * time.Second)
	err = ethBackend.Stop()
	require.NoError(t, err)
}

var consumedPorts = sync.Map{}

// ConsumeFreeTcpPort can be used by many goroutines at the same time.
// It uses port 0 and the OS to find a random free port. Note it opens the port
// so we have to close it. But closing a port makes it eligible for selection
// by the OS again. So we need to remember which ones have been already touched.
func ConsumeFreeTcpPort(t *testing.T) (int, func()) {
	var port int
	var done bool
	var iterations int
	for !done {
		func() {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer func() {
				err := listener.Close()
				require.NoError(t, err)
			}()

			port = listener.Addr().(*net.TCPAddr).Port
			_, ok := consumedPorts.Swap(port, struct{}{})
			done = !ok
		}()
		iterations++
		if iterations > 1024 {
			require.FailNow(t, "failed to find a free port", "after %d iterations", iterations)
		}
	}
	return port, func() { consumedPorts.Delete(port) }
}

type NativeTokenBank struct {
	privKey        *ecdsa.PrivateKey
	initialBalance *big.Int
}

func (b NativeTokenBank) Address() common.Address {
	return crypto.PubkeyToAddress(b.privKey.PublicKey)
}

func (b NativeTokenBank) InitialBalance() *big.Int {
	return b.initialBalance
}
