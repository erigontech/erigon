package commands

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/u256"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/gointerfaces/txpool"
	"github.com/stretchr/testify/require"
)

var testKey, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")

func TestSendRawTransaction(t *testing.T) {
	db, err := createTestKV()
	require.NoError(t, err)
	defer db.Close()
	conn := createTestGrpcConn(db)
	defer conn.Close()

	api := NewEthAPI(db, nil, txpool.NewTxpoolClient(conn), 5000000, nil, nil)
	// Call GetTransactionReceipt for un-protected transaction
	tx := transaction(uint64(1), 0, testKey)
	buf := bytes.NewBuffer(nil)
	err = tx.MarshalBinary(buf)
	require.NoError(t, err)
	_, err = api.SendRawTransaction(context.Background(), buf.Bytes())
	require.NoError(t, err)
}

func transaction(nonce uint64, gaslimit uint64, key *ecdsa.PrivateKey) types.Transaction {
	return pricedTransaction(nonce, gaslimit, u256.Num1, key)
}

func pricedTransaction(nonce uint64, gaslimit uint64, gasprice *uint256.Int, key *ecdsa.PrivateKey) types.Transaction {
	tx, _ := types.SignTx(types.NewTransaction(nonce, common.Address{}, uint256.NewInt().SetUint64(100), gaslimit, gasprice, nil), *types.LatestSignerForChainID(nil), key)
	return tx
}
