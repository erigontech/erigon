package commands

import (
	"bytes"
	"testing"

	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/filters"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/gointerfaces/txpool"
	"github.com/stretchr/testify/require"
)

func TestTxPoolContent(t *testing.T) {
	db := createTestKV(t)
	ctx, conn := createTestGrpcConn(t)
	txPool := txpool.NewTxpoolClient(conn)
	ff := filters.New(ctx, nil, txPool, txpool.NewMiningClient(conn))
	api := NewTxPoolAPI(NewBaseApi(ff), db, txPool)

	// Call GetTransactionReceipt for un-protected transaction
	var testKey, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	expect := uint64(40)
	txn := transaction(expect, 1000000, testKey)
	buf := bytes.NewBuffer(nil)
	err := txn.MarshalBinary(buf)
	require.NoError(t, err)

	_, err = txPool.Add(ctx, &txpool.AddRequest{RlpTxs: [][]byte{buf.Bytes()}})
	require.NoError(t, err)
	content, err := api.Content(ctx)
	require.NoError(t, err)

	sender := (common.Address{}).String()
	require.Equal(t, 1, len(content["pending"][sender]))
	require.Equal(t, expect, uint64(content["pending"][sender]["40"].Nonce))
}
