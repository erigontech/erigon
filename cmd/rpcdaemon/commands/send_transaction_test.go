package commands

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"math/big"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/filters"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/gointerfaces/txpool"
	"github.com/stretchr/testify/require"
)

func TestSendRawTransaction(t *testing.T) {
	t.Skip("Flaky test")
	db := createTestKV(t)
	conn := createTestGrpcConn()
	defer conn.Close()
	txPool := txpool.NewTxpoolClient(conn)
	ff := filters.New(context.Background(), nil, txPool, nil)
	api := NewEthAPI(NewBaseApi(ff), db, nil, txPool, nil, 5000000)

	// Call GetTransactionReceipt for un-protected transaction
	var testKey, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	expect := uint64(40)
	txn := transaction(expect, 1000000, testKey)
	buf := bytes.NewBuffer(nil)
	err := txn.MarshalBinary(buf)
	require.NoError(t, err)

	txsCh := make(chan []types.Transaction, 1)
	defer close(txsCh)
	id := api.filters.SubscribePendingTxs(txsCh)
	defer api.filters.UnsubscribePendingTxs(id)

	_, err = api.SendRawTransaction(context.Background(), buf.Bytes())
	require.NoError(t, err)
	select {
	case got := <-txsCh:
		require.Equal(t, expect, got[0].GetNonce())
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("timeout waiting for  expected notification")
	}
}

func transaction(nonce uint64, gaslimit uint64, key *ecdsa.PrivateKey) types.Transaction {
	return pricedTransaction(nonce, gaslimit, u256.Num1, key)
}

func pricedTransaction(nonce uint64, gaslimit uint64, gasprice *uint256.Int, key *ecdsa.PrivateKey) types.Transaction {
	tx, _ := types.SignTx(types.NewTransaction(nonce, common.Address{}, uint256.NewInt(100), gaslimit, gasprice, nil), *types.LatestSignerForChainID(big.NewInt(1337)), key)
	return tx
}
