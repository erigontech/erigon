// Copyright 2023 The Erigon Authors
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

package bor

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/chain/networkname"
	"github.com/erigontech/erigon-lib/chain/params"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/fdlimit"
	"github.com/erigontech/erigon-lib/common/race"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/gointerfaces"
	"github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	"github.com/erigontech/erigon-lib/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/testlog"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/eth"
	"github.com/erigontech/erigon/node"
	"github.com/erigontech/erigon/tests/bor/helper"
)

const (
	// testCode is the testing contract binary code which will initialises some
	// variables in constructor
	testCode = "0x60806040527fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff0060005534801561003457600080fd5b5060fc806100436000396000f3fe6080604052348015600f57600080fd5b506004361060325760003560e01c80630c4dae8814603757806398a213cf146053575b600080fd5b603d607e565b6040518082815260200191505060405180910390f35b607c60048036036020811015606757600080fd5b81019080803590602001909291905050506084565b005b60005481565b806000819055507fe9e44f9f7da8c559de847a3232b57364adc0354f15a2cd8dc636d54396f9587a6000546040518082815260200191505060405180910390a15056fea265627a7a723058208ae31d9424f2d0bc2a3da1a5dd659db2d71ec322a17db8f87e19e209e3a1ff4a64736f6c634300050a0032"

	// testGas is the gas required for contract deployment.
	testGas = 144109
)

var (
	// addr1 = 0x71562b71999873DB5b286dF957af199Ec94617F7
	pkey1, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	addr1    = crypto.PubkeyToAddress(pkey1.PublicKey)

	// addr2 = 0x9fB29AAc15b9A4B7F17c3385939b007540f4d791
	pkey2, _ = crypto.HexToECDSA("9b28f36fbd67381120752d6172ecdcf10e06ab2d9a1367aac00cdcd6ac7855d3")
	addr2    = crypto.PubkeyToAddress(pkey2.PublicKey)

	pkeys = []*ecdsa.PrivateKey{pkey1, pkey2}
)

// CGO_CFLAGS="-D__BLST_PORTABLE__" : flag required for go test.
// Example : CGO_CFLAGS="-D__BLST_PORTABLE__" go test -run ^TestMiningBenchmark$ github.com/erigontech/erigon/tests/bor -v -count=1
// In TestMiningBenchmark, we will test the mining performance. We will initialize a single node devnet and fire 5000 txs. We will measure the time it takes to include all the txs. This can be made more advcanced by increasing blockLimit and txsInTxpool.
func TestMiningBenchmark(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	//goland:noinspection GoBoolExpressions
	if race.Enabled && runtime.GOOS == "darwin" {
		// We run race detector for medium tests which fails on macOS.
		t.Skip("issue #15007")
	}

	//usually 15sec is enough
	ctx, clean := context.WithTimeout(context.Background(), time.Minute)
	defer clean()

	logger := testlog.Logger(t, log.LvlInfo)
	fdlimit.Raise(2048)

	genesis := helper.InitGenesis("./testdata/genesis_2val.json", 64, networkname.BorE2ETestChain2Val)
	var stacks []*node.Node
	var ethbackends []*eth.Ethereum
	var enodes []string
	var txInTxpool = 5000
	var txs []*types.Transaction

	for i := 0; i < 1; i++ {
		stack, ethBackend, err := helper.InitMiner(ctx, logger, t.TempDir(), &genesis, pkeys[i], true, i)
		if err != nil {
			panic(err)
		}

		t.Cleanup(func() {
			err := stack.Close()
			require.NoError(t, err)
		})

		if err := stack.Start(); err != nil {
			panic(err)
		}

		var nodeInfo *remoteproto.NodesInfoReply

		for nodeInfo == nil || len(nodeInfo.NodesInfo) == 0 {
			nodeInfo, err = ethBackend.NodesInfo(1)

			if err != nil {
				panic(err)
			}

			time.Sleep(200 * time.Millisecond)
		}
		// nolint : staticcheck
		stacks = append(stacks, stack)

		ethbackends = append(ethbackends, ethBackend)

		// nolint : staticcheck
		enodes = append(enodes, nodeInfo.NodesInfo[0].Enode)
	}

	// nonce starts from 0 because have no txs yet
	initNonce := uint64(0)

	for i := 0; i < txInTxpool; i++ {
		txn, err := newRandomTxWithNonce(false, initNonce+uint64(i), ethbackends[0].TxpoolServer())
		if err != nil {
			panic(err)
		}
		txs = append(txs, txn)
	}

	start := time.Now()

	for _, txn := range txs {
		buf := bytes.NewBuffer(nil)
		txV := *txn
		err := txV.MarshalBinary(buf)
		if err != nil {
			panic(err)
		}
		_, err = ethbackends[0].TxpoolServer().Add(ctx, &txpoolproto.AddRequest{RlpTxs: [][]byte{buf.Bytes()}})
		if err != nil {
			panic(err)
		}
	}

	for {
		pendingReply, err := ethbackends[0].TxpoolServer().Status(ctx, &txpoolproto.StatusRequest{})
		if err != nil {
			panic(err)
		}

		logger.Info(
			"Number of txs in the pool:",
			"pending", pendingReply.PendingCount,
			"base_fee", pendingReply.BaseFeeCount,
			"queued", pendingReply.QueuedCount,
		)

		if pendingReply.PendingCount == 0 {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	timeToExecute := time.Since(start)

	fmt.Printf("\n\nTime to execute %d txs: %s\n", txInTxpool, timeToExecute)

}

// newRandomTxWithNonce creates a new transaction with the given nonce.
func newRandomTxWithNonce(creation bool, nonce uint64, txPool txpoolproto.TxpoolServer) (tx *types.Transaction, err error) {
	var txn types.Transaction

	gasPrice := uint256.NewInt(100 * params.InitialBaseFee)

	if creation {
		var nonceReply *txpoolproto.NonceReply
		nonceReply, err = txPool.Nonce(context.Background(), &txpoolproto.NonceRequest{Address: gointerfaces.ConvertAddressToH160(addr1)})
		if err != nil {
			return nil, err
		}
		txn, err = types.SignTx(types.NewContractCreation(nonceReply.Nonce, uint256.NewInt(0), testGas, gasPrice, common.FromHex(testCode)), *types.LatestSignerForChainID(nil), pkey1)
		if err != nil {
			return nil, err
		}
	} else {
		txn, err = types.SignTx(types.NewTransaction(nonce, addr2, uint256.NewInt(1000), params.TxGas, gasPrice, nil), *types.LatestSignerForChainID(nil), pkey1)
		if err != nil {
			return nil, err
		}
	}

	return &txn, nil
}
