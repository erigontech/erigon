package ethapi

import (
	"encoding/json"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var (
	pinSender    = common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	pinTo        = common.HexToAddress("0x1234567890123456789012345678901234567890")
	pinBlockHash = common.HexToHash("0xaabbccddeeff00112233445566778899aabbccddeeff001122334455667788aa")
)

func pinCommonTx(nonce uint64) types.CommonTx {
	return types.CommonTx{
		Nonce:    nonce,
		GasLimit: 21000,
		To:       &pinTo,
		Value:    *uint256.NewInt(1_000_000_000),
		Data:     []byte{0xca, 0xfe},
		V:        *uint256.NewInt(1),
		R:        *uint256.NewInt(0x1111),
		S:        *uint256.NewInt(0x2222),
	}
}

// TestRPCTransactionJSONPinned pins the exact wire format of RPCTransaction for
// every transaction type, on both the pre-London (nil baseFee) and post-London
// paths. Any refactoring of RPCTransaction must keep these bytes identical.
func TestRPCTransactionJSONPinned(t *testing.T) {
	baseFee := uint256.NewInt(7)

	legacyPre155 := &types.LegacyTx{CommonTx: pinCommonTx(5), GasPrice: *uint256.NewInt(100)}
	legacyPre155.CommonTx.V = *uint256.NewInt(28)

	legacy155 := &types.LegacyTx{CommonTx: pinCommonTx(6), GasPrice: *uint256.NewInt(100)}
	legacy155.CommonTx.V = *uint256.NewInt(38) // EIP-155, chain id 1

	legacyUnsigned := &types.LegacyTx{CommonTx: pinCommonTx(7), GasPrice: *uint256.NewInt(100)}
	legacyUnsigned.CommonTx.V, legacyUnsigned.CommonTx.R, legacyUnsigned.CommonTx.S = uint256.Int{}, uint256.Int{}, uint256.Int{}

	accessList := &types.AccessListTx{
		LegacyTx: types.LegacyTx{CommonTx: pinCommonTx(8), GasPrice: *uint256.NewInt(100)},
		ChainID:  *uint256.NewInt(1),
		AccessList: types.AccessList{{
			Address:     pinTo,
			StorageKeys: []common.Hash{common.HexToHash("0x01")},
		}},
	}

	dynamicFee := &types.DynamicFeeTransaction{
		CommonTx: pinCommonTx(9),
		ChainID:  *uint256.NewInt(1),
		TipCap:   *uint256.NewInt(10),
		FeeCap:   *uint256.NewInt(1000),
	}

	blob := &types.BlobTx{
		DynamicFeeTransaction: types.DynamicFeeTransaction{
			CommonTx: pinCommonTx(10),
			ChainID:  *uint256.NewInt(1),
			TipCap:   *uint256.NewInt(10),
			FeeCap:   *uint256.NewInt(1000),
		},
		MaxFeePerBlobGas:    *uint256.NewInt(50),
		BlobVersionedHashes: []common.Hash{common.HexToHash("0x0102")},
	}

	setCode := &types.SetCodeTransaction{
		DynamicFeeTransaction: types.DynamicFeeTransaction{
			CommonTx: pinCommonTx(11),
			ChainID:  *uint256.NewInt(1),
			TipCap:   *uint256.NewInt(10),
			FeeCap:   *uint256.NewInt(1000),
		},
		Authorizations: []types.Authorization{{
			ChainID: *uint256.NewInt(1),
			Address: pinTo,
			Nonce:   12,
			YParity: 1,
			R:       *uint256.NewInt(0x3333),
			S:       *uint256.NewInt(0x4444),
		}},
	}

	for _, tt := range []struct {
		name    string
		txn     types.Transaction
		block   bool
		baseFee *uint256.Int
		want    string
	}{
		{name: "legacy-pre155-in-block", txn: legacyPre155, block: true, baseFee: baseFee,
			want: `{"blockHash":"0xaabbccddeeff00112233445566778899aabbccddeeff001122334455667788aa","blockNumber":"0x64","blockTimestamp":"0x3e8","from":"0x71562b71999873db5b286df957af199ec94617f7","gas":"0x5208","gasPrice":"0x64","hash":"0x8ef46ab1d208f19757d251208b6c266203a5d840db17dd5391cad7cedec1c6d5","input":"0xcafe","nonce":"0x5","to":"0x1234567890123456789012345678901234567890","transactionIndex":"0x3","value":"0x3b9aca00","type":"0x0","v":"0x1c","r":"0x1111","s":"0x2222"}`},
		{name: "legacy-eip155-pending", txn: legacy155, block: false, baseFee: nil,
			want: `{"blockHash":null,"blockNumber":null,"blockTimestamp":null,"from":"0x71562b71999873db5b286df957af199ec94617f7","gas":"0x5208","gasPrice":"0x64","hash":"0x3a5149d3c004d26e7aa7c02252e40ec269e2b26da5a020c3c158e242d85c2add","input":"0xcafe","nonce":"0x6","to":"0x1234567890123456789012345678901234567890","transactionIndex":null,"value":"0x3b9aca00","type":"0x0","chainId":"0x1","v":"0x26","r":"0x1111","s":"0x2222"}`},
		{name: "legacy-unsigned-pending", txn: legacyUnsigned, block: false, baseFee: nil,
			want: `{"blockHash":null,"blockNumber":null,"blockTimestamp":null,"from":"0x71562b71999873db5b286df957af199ec94617f7","gas":"0x5208","gasPrice":"0x64","hash":"0xe771ac00824457ce02bd60ab3b9b0bf87fcef50de4b08c6fb1880b14e5566ee5","input":"0xcafe","nonce":"0x7","to":"0x1234567890123456789012345678901234567890","transactionIndex":null,"value":"0x3b9aca00","type":"0x0","v":null,"r":null,"s":null}`},
		{name: "accesslist-in-block", txn: accessList, block: true, baseFee: baseFee,
			want: `{"blockHash":"0xaabbccddeeff00112233445566778899aabbccddeeff001122334455667788aa","blockNumber":"0x64","blockTimestamp":"0x3e8","from":"0x71562b71999873db5b286df957af199ec94617f7","gas":"0x5208","gasPrice":"0x64","hash":"0xabb4f9a8c5e58fb1824763b2a85066b60e92ffd0563c62811bff5acd049e821a","input":"0xcafe","nonce":"0x8","to":"0x1234567890123456789012345678901234567890","transactionIndex":"0x3","value":"0x3b9aca00","type":"0x1","accessList":[{"address":"0x1234567890123456789012345678901234567890","storageKeys":["0x0000000000000000000000000000000000000000000000000000000000000001"]}],"chainId":"0x1","v":"0x1","yParity":"0x1","r":"0x1111","s":"0x2222"}`},
		{name: "dynamicfee-in-block", txn: dynamicFee, block: true, baseFee: baseFee,
			want: `{"blockHash":"0xaabbccddeeff00112233445566778899aabbccddeeff001122334455667788aa","blockNumber":"0x64","blockTimestamp":"0x3e8","from":"0x71562b71999873db5b286df957af199ec94617f7","gas":"0x5208","gasPrice":"0x11","maxPriorityFeePerGas":"0xa","maxFeePerGas":"0x3e8","hash":"0xd9bdf6e2ecb2b07bb8076a7079b99c8fb6c45bf613c6c479687d078c6c437fe2","input":"0xcafe","nonce":"0x9","to":"0x1234567890123456789012345678901234567890","transactionIndex":"0x3","value":"0x3b9aca00","type":"0x2","accessList":null,"chainId":"0x1","v":"0x1","yParity":"0x1","r":"0x1111","s":"0x2222"}`},
		{name: "dynamicfee-pending-no-basefee", txn: dynamicFee, block: false, baseFee: nil,
			want: `{"blockHash":null,"blockNumber":null,"blockTimestamp":null,"from":"0x71562b71999873db5b286df957af199ec94617f7","gas":"0x5208","maxPriorityFeePerGas":"0xa","maxFeePerGas":"0x3e8","hash":"0xd9bdf6e2ecb2b07bb8076a7079b99c8fb6c45bf613c6c479687d078c6c437fe2","input":"0xcafe","nonce":"0x9","to":"0x1234567890123456789012345678901234567890","transactionIndex":null,"value":"0x3b9aca00","type":"0x2","accessList":null,"chainId":"0x1","v":"0x1","yParity":"0x1","r":"0x1111","s":"0x2222"}`},
		{name: "blob-in-block", txn: blob, block: true, baseFee: baseFee,
			want: `{"blockHash":"0xaabbccddeeff00112233445566778899aabbccddeeff001122334455667788aa","blockNumber":"0x64","blockTimestamp":"0x3e8","from":"0x71562b71999873db5b286df957af199ec94617f7","gas":"0x5208","gasPrice":"0x11","maxPriorityFeePerGas":"0xa","maxFeePerGas":"0x3e8","hash":"0xb7df8bc4dc66f7f371245812036aefff8b6c35abc3998a9dde1171187fb12e07","input":"0xcafe","nonce":"0xa","to":"0x1234567890123456789012345678901234567890","transactionIndex":"0x3","value":"0x3b9aca00","type":"0x3","accessList":null,"chainId":"0x1","maxFeePerBlobGas":"0x32","blobVersionedHashes":["0x0000000000000000000000000000000000000000000000000000000000000102"],"v":"0x1","yParity":"0x1","r":"0x1111","s":"0x2222"}`},
		{name: "setcode-in-block", txn: setCode, block: true, baseFee: baseFee,
			want: `{"blockHash":"0xaabbccddeeff00112233445566778899aabbccddeeff001122334455667788aa","blockNumber":"0x64","blockTimestamp":"0x3e8","from":"0x71562b71999873db5b286df957af199ec94617f7","gas":"0x5208","gasPrice":"0x11","maxPriorityFeePerGas":"0xa","maxFeePerGas":"0x3e8","hash":"0x590610d2b442819852a4bfe6058fcddd202107fd2084c6dd56947aab6b372284","input":"0xcafe","nonce":"0xb","to":"0x1234567890123456789012345678901234567890","transactionIndex":"0x3","value":"0x3b9aca00","type":"0x4","accessList":null,"chainId":"0x1","authorizationList":[{"chainId":"0x1","address":"0x1234567890123456789012345678901234567890","nonce":"0xc","yParity":"0x1","r":"0x3333","s":"0x4444"}],"v":"0x1","yParity":"0x1","r":"0x1111","s":"0x2222"}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tt.txn.SetSender(accounts.InternAddress(pinSender))
			blockHash, blockTime, blockNumber, index := common.Hash{}, uint64(0), uint64(0), uint64(0)
			if tt.block {
				blockHash, blockTime, blockNumber, index = pinBlockHash, 1000, 100, 3
			}
			got, err := json.Marshal(NewRPCTransaction(tt.txn, blockHash, blockTime, blockNumber, index, tt.baseFee))
			require.NoError(t, err)
			require.Equal(t, tt.want, string(got))
		})
	}
}

func BenchmarkNewRPCTransaction(b *testing.B) {
	baseFee := uint256.NewInt(7)

	legacy := &types.LegacyTx{CommonTx: pinCommonTx(5), GasPrice: *uint256.NewInt(100)}
	legacy.CommonTx.V = *uint256.NewInt(38)

	dynamicFee := &types.DynamicFeeTransaction{
		CommonTx: pinCommonTx(9),
		ChainID:  *uint256.NewInt(1),
		TipCap:   *uint256.NewInt(10),
		FeeCap:   *uint256.NewInt(1000),
	}

	blob := &types.BlobTx{
		DynamicFeeTransaction: types.DynamicFeeTransaction{
			CommonTx: pinCommonTx(10),
			ChainID:  *uint256.NewInt(1),
			TipCap:   *uint256.NewInt(10),
			FeeCap:   *uint256.NewInt(1000),
		},
		MaxFeePerBlobGas:    *uint256.NewInt(50),
		BlobVersionedHashes: []common.Hash{common.HexToHash("0x0102")},
	}

	for _, bb := range []struct {
		name string
		txn  types.Transaction
	}{
		{name: "legacy", txn: legacy},
		{name: "dynamicfee", txn: dynamicFee},
		{name: "blob", txn: blob},
	} {
		bb.txn.SetSender(accounts.InternAddress(pinSender))
		bb.txn.Hash()
		b.Run(bb.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				out, err := json.Marshal(NewRPCTransaction(bb.txn, pinBlockHash, 1000, 100, 3, baseFee))
				if err != nil || len(out) == 0 {
					b.Fatal(err)
				}
			}
		})
	}
}
