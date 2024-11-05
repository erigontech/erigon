package eth1_utils

import (
	"testing"

	"github.com/go-test/deep"
	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
	"github.com/stretchr/testify/require"
)

func makeBlock(txCount, uncleCount, withdrawalCount int) *types.Block {
	var (
		key, _      = crypto.GenerateKey()
		txs         = make([]types.Transaction, txCount)
		receipts    = make([]*types.Receipt, len(txs))
		signer      = types.LatestSigner(params.TestChainConfig)
		uncles      = make([]*types.Header, uncleCount)
		withdrawals = make([]*types.Withdrawal, withdrawalCount)
	)
	header := &types.Header{
		Difficulty: math.BigPow(11, 11),
		Number:     math.BigPow(2, 9),
		GasLimit:   12345678,
		GasUsed:    1476322,
		Time:       9876543,
		Extra:      []byte("test block"),
	}
	for i := range txs {
		amount, _ := uint256.FromBig(math.BigPow(2, int64(i)))
		price := uint256.NewInt(300000)
		data := make([]byte, 100)
		tx := types.NewTransaction(uint64(i), libcommon.Address{}, amount, 123457, price, data)
		signedTx, err := types.SignTx(tx, *signer, key)
		if err != nil {
			panic(err)
		}
		txs[i] = signedTx
		receipts[i] = types.NewReceipt(false, tx.GetGas())
	}
	for i := range uncles {
		uncles[i] = &types.Header{
			Difficulty: math.BigPow(11, 11),
			Number:     math.BigPow(2, 9),
			GasLimit:   12345678,
			GasUsed:    1476322,
			Time:       9876543,
			Extra:      []byte("test uncle"),
		}
	}
	for i := range withdrawals {
		withdrawals[i] = &types.Withdrawal{
			Index:     uint64(i),
			Validator: uint64(i),
			Amount:    uint64(10 * i),
		}
	}
	for i := range withdrawals {
		withdrawals[i] = &types.Withdrawal{
			Index:     uint64(i),
			Validator: uint64(i),
			Amount:    uint64(10 * i),
		}
	}
	return types.NewBlock(header, txs, uncles, receipts, withdrawals)
}

func TestBlockRpcConversion(t *testing.T) {
	t.Parallel()
	require := require.New(t)
	testBlock := makeBlock(50, 2, 3)

	// header conversions
	rpcHeader := HeaderToHeaderRPC(testBlock.Header())
	roundTripHeader, err := HeaderRpcToHeader(rpcHeader)
	if err != nil {
		panic(err)
	}

	deep.CompareUnexportedFields = true
	require.Nil(deep.Equal(testBlock.HeaderNoCopy(), roundTripHeader))

	// body conversions
	rpcBlock := ConvertBlockToRPC(testBlock)
	roundTripBody, err := ConvertRawBlockBodyFromRpc(rpcBlock.Body)
	if err != nil {
		panic(err)
	}
	testBlockRaw := testBlock.RawBody()
	require.Greater(len(testBlockRaw.Transactions), 0)
	require.Greater(len(testBlockRaw.Uncles), 0)
	require.Greater(len(testBlockRaw.Withdrawals), 0)
	require.Nil(deep.Equal(testBlockRaw, roundTripBody))
}

func TestBigIntConversion(t *testing.T) {
	t.Parallel()
	require := require.New(t)

	val := math.BigPow(2, 32)
	rpcVal := ConvertBigIntToRpc(val)
	roundTripVal := ConvertBigIntFromRpc(rpcVal)
	require.Equal(val, roundTripVal)
}
