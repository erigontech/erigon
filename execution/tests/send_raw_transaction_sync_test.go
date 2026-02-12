// Copyright 2026 The Erigon Authors
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

package executiontests

import (
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

func TestSendRawTransactionSync(t *testing.T) {
	if testing.Short() {
		t.Skip("too slow for testing.Short")
	}

	assert := require.New(t)
	eat := DefaultEngineApiTester(t)

	tx, err := eat.Transactor.CreateSimpleTransfer(eat.CoinbaseKey, common.Address{1}, big.NewInt(1234))
	assert.NoError(err)

	// Subscribe to pending transactions to see the arrival of the transaction at the mempool
	newPendingTxsCh := make(chan string)
	newPendingSubscription, err := eat.RpcApiClient.Subscribe(t.Context(), "eth_newPendingTransactions", newPendingTxsCh)
	assert.NoError(err)
	defer newPendingSubscription.Unsubscribe()

	// Send the transaction in a separate task (eth_sendRawTransactionSync call blocks until the receipt is available)
	var receipt *types.Receipt
	var errSend error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		timeoutMillis := uint64(3600000) // 1hr
		receipt, errSend = eat.RpcApiClient.SendRawTransactionSync(tx, &timeoutMillis)
	}()

	// Wait for the transaction to show up in pending transactions (i.e. arrived at the mempool)
	select {
	case txHash := <-newPendingTxsCh:
		assert.Equal(tx.Hash(), common.HexToHash(txHash))
	case <-time.After(20 * time.Second): // Sometimes the channel times out on GitHub Actions
		t.Log("Timeout waiting for txn from channel")
		jsonTx, err := eat.RpcApiClient.GetTransactionByHash(tx.Hash())
		assert.NoError(err)
		assert.NotNil(jsonTx)
	}

	// Build up a new canonical block including the transaction
	clPayload, err := eat.MockCl.BuildCanonicalBlock(t.Context())
	assert.NoError(err)
	err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(t.Context(), clPayload.ExecutionPayload, tx.Hash())
	assert.NoError(err)
	_, err = eat.MockCl.InsertNewPayload(t.Context(), clPayload)
	assert.NoError(err)

	// Wait for eth_sendRawTransactionSync to return and the expected receipt to show up
	wg.Wait()
	assert.NoError(errSend)
	assert.NotNil(receipt)
	assert.Equal(receipt.TxHash, tx.Hash())
}

func TestSendRawTransactionSyncTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("too slow for testing.Short")
	}

	assert := require.New(t)
	eat := DefaultEngineApiTester(t)

	tx, err := eat.Transactor.CreateSimpleTransfer(eat.CoinbaseKey, common.Address{1}, big.NewInt(1234))
	assert.NoError(err)

	// Send the txn first time and just wait for timeout
	timeoutMillis := uint64(100)
	receipt, err := eat.RpcApiClient.SendRawTransactionSync(tx, &timeoutMillis)
	assert.Error(err)
	assert.Equal("the transaction was added to the mempool but wasn't processed in 100ms", err.Error())
	assert.Nil(receipt)

	// Send the same txn second time and expect an error
	_, err = eat.RpcApiClient.SendRawTransactionSync(tx, &timeoutMillis)
	assert.Error(err)
	expectedErr := txpoolproto.ImportResult_name[int32(txpoolproto.ImportResult_ALREADY_EXISTS)] + ": " + txpoolcfg.AlreadyKnown.String()
	assert.Equal(expectedErr, err.Error())
}
