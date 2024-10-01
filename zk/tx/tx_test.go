package tx

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"testing"

	zkhex "github.com/ledgerwatch/erigon/zkevm/hex"

	"encoding/binary"

	"github.com/holiman/uint256"
	constants "github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeRandomBatchL2Data(t *testing.T) {
	randomData := []byte("Random data")
	blocks, err := DecodeBatchL2Blocks(randomData, uint64(constants.ForkID5Dragonfruit))
	require.Error(t, err)
	assert.Equal(t, 0, len(blocks))

	randomData = []byte("Esto es autentica basura")
	blocks, err = DecodeBatchL2Blocks(randomData, uint64(constants.ForkID5Dragonfruit))
	require.Error(t, err)
	assert.Equal(t, 0, len(blocks))

	randomData = []byte("beef")
	blocks, err = DecodeBatchL2Blocks(randomData, uint64(constants.ForkID5Dragonfruit))
	require.Error(t, err)
	assert.Equal(t, 0, len(blocks))
}

func TestDecodePre155BatchL2DataPreForkID5(t *testing.T) {
	pre155, err := hex.DecodeString("e480843b9aca00826163941275fbb540c8efc58b812ba83b0d0b8b9917ae98808464fbb77cb7d2a666860f3c6b8f5ef96f86c7ec5562e97fd04c2e10f3755ff3a0456f9feb246df95217bf9082f84f9e40adb0049c6664a5bb4c9cbe34ab1a73e77bab26ed1b")
	require.NoError(t, err)
	blocks, err := DecodeBatchL2Blocks(pre155, uint64(constants.ForkID4))
	require.NoError(t, err)
	assert.Equal(t, 1, len(blocks))
	v, r, s := blocks[0].Transactions[0].RawSignatureValues()
	assert.Equal(t, "0x1275fbb540c8efC58b812ba83B0D0B8b9917AE98", blocks[0].Transactions[0].GetTo().String())
	assert.Equal(t, "1b", fmt.Sprintf("%x", v))
	assert.Equal(t, "b7d2a666860f3c6b8f5ef96f86c7ec5562e97fd04c2e10f3755ff3a0456f9feb", fmt.Sprintf("%x", r))
	assert.Equal(t, "246df95217bf9082f84f9e40adb0049c6664a5bb4c9cbe34ab1a73e77bab26ed", fmt.Sprintf("%x", s))
	assert.Equal(t, uint64(24931), blocks[0].Transactions[0].GetGas())
	assert.Equal(t, "64fbb77c", hex.EncodeToString(blocks[0].Transactions[0].GetData()))
	assert.Equal(t, uint64(0), blocks[0].Transactions[0].GetNonce())
	assert.Equal(t, uint256.NewInt(1000000000), blocks[0].Transactions[0].GetPrice())

	pre155, err = hex.DecodeString("e580843b9aca00830186a0941275fbb540c8efc58b812ba83b0d0b8b9917ae988084159278193d7bcd98c00060650f12c381cc2d4f4cc8abf54059aecd2c7aabcfcdd191ba6827b1e72f0eb0b8d5daae64962f4aafde7853e1c102de053edbedf066e6e3c2dc1b")
	require.NoError(t, err)
	blocks, err = DecodeBatchL2Blocks(pre155, uint64(constants.ForkID4))
	require.NoError(t, err)
	assert.Equal(t, 1, len(blocks))
	assert.Equal(t, "0x1275fbb540c8efC58b812ba83B0D0B8b9917AE98", blocks[0].Transactions[0].GetTo().String())
	assert.Equal(t, uint64(0), blocks[0].Transactions[0].GetNonce())
	assert.Equal(t, uint256.NewInt(0), blocks[0].Transactions[0].GetValue())
	assert.Equal(t, "15927819", hex.EncodeToString(blocks[0].Transactions[0].GetData()))
	assert.Equal(t, uint64(100000), blocks[0].Transactions[0].GetGas())
	assert.Equal(t, uint256.NewInt(1000000000), blocks[0].Transactions[0].GetPrice())
}

func TestDecodePre155Tx(t *testing.T) {
	pre155 := "0xf86780843b9aca00826163941275fbb540c8efc58b812ba83b0d0b8b9917ae98808464fbb77c1ba0b7d2a666860f3c6b8f5ef96f86c7ec5562e97fd04c2e10f3755ff3a0456f9feba0246df95217bf9082f84f9e40adb0049c6664a5bb4c9cbe34ab1a73e77bab26ed"
	pre155Bytes, err := hex.DecodeString(pre155[2:])
	require.NoError(t, err)
	tx, _, err := DecodeTx(pre155Bytes, 0, uint64(constants.ForkID5Dragonfruit))
	require.NoError(t, err)
	v, r, s := tx.RawSignatureValues()
	assert.Equal(t, "0x1275fbb540c8efC58b812ba83B0D0B8b9917AE98", tx.GetTo().String())
	assert.Equal(t, "1b", fmt.Sprintf("%x", v))
	assert.Equal(t, "b7d2a666860f3c6b8f5ef96f86c7ec5562e97fd04c2e10f3755ff3a0456f9feb", fmt.Sprintf("%x", r))
	assert.Equal(t, "246df95217bf9082f84f9e40adb0049c6664a5bb4c9cbe34ab1a73e77bab26ed", fmt.Sprintf("%x", s))
	assert.Equal(t, uint64(24931), tx.GetGas())
	assert.Equal(t, "64fbb77c", hex.EncodeToString(tx.GetData()))
	assert.Equal(t, uint64(0), tx.GetNonce())
	assert.Equal(t, uint256.NewInt(1000000000), tx.GetPrice())
}

func TestDecodePost155Tx(t *testing.T) {
	post155 := "0xf86780843b9aca00826163941275fbb540c8efc58b812ba83b0d0b8b9917ae98808464fbb77c1ba0b7d2a666860f3c6b8f5ef96f86c7ec5562e97fd04c2e10f3755ff3a0456f9feba0246df95217bf9082f84f9e40adb0049c6664a5bb4c9cbe34ab1a73e77bab26ed"
	post155Bytes, err := hex.DecodeString(post155[2:])
	require.NoError(t, err)
	tx, pct, err := DecodeTx(post155Bytes, 75, uint64(constants.ForkID5Dragonfruit))
	require.NoError(t, err)
	v, r, s := tx.RawSignatureValues()
	assert.Equal(t, "0x1275fbb540c8efC58b812ba83B0D0B8b9917AE98", tx.GetTo().String())
	assert.Equal(t, "1b", fmt.Sprintf("%x", v))
	assert.Equal(t, "b7d2a666860f3c6b8f5ef96f86c7ec5562e97fd04c2e10f3755ff3a0456f9feb", fmt.Sprintf("%x", r))
	assert.Equal(t, "246df95217bf9082f84f9e40adb0049c6664a5bb4c9cbe34ab1a73e77bab26ed", fmt.Sprintf("%x", s))
	assert.Equal(t, uint64(24931), tx.GetGas())
	assert.Equal(t, "64fbb77c", hex.EncodeToString(tx.GetData()))
	assert.Equal(t, uint64(0), tx.GetNonce())
	assert.Equal(t, uint256.NewInt(1000000000), tx.GetPrice())
	assert.Equal(t, pct, uint8(0x4b))
}

func TestDecodePre155BatchL2DataForkID5(t *testing.T) {
	pre155, err := hex.DecodeString("e480843b9aca00826163941275fbb540c8efc58b812ba83b0d0b8b9917ae98808464fbb77cb7d2a666860f3c6b8f5ef96f86c7ec5562e97fd04c2e10f3755ff3a0456f9feb246df95217bf9082f84f9e40adb0049c6664a5bb4c9cbe34ab1a73e77bab26ed1bff")
	require.NoError(t, err)
	blocks, err := DecodeBatchL2Blocks(pre155, uint64(constants.ForkID5Dragonfruit))
	require.NoError(t, err)
	assert.Equal(t, 1, len(blocks))
	v, r, s := blocks[0].Transactions[0].RawSignatureValues()
	assert.Equal(t, "0x1275fbb540c8efC58b812ba83B0D0B8b9917AE98", blocks[0].Transactions[0].GetTo().String())
	assert.Equal(t, "1b", fmt.Sprintf("%x", v))
	assert.Equal(t, "b7d2a666860f3c6b8f5ef96f86c7ec5562e97fd04c2e10f3755ff3a0456f9feb", fmt.Sprintf("%x", r))
	assert.Equal(t, "246df95217bf9082f84f9e40adb0049c6664a5bb4c9cbe34ab1a73e77bab26ed", fmt.Sprintf("%x", s))
	assert.Equal(t, uint64(24931), blocks[0].Transactions[0].GetGas())
	assert.Equal(t, "64fbb77c", hex.EncodeToString(blocks[0].Transactions[0].GetData()))
	assert.Equal(t, uint64(0), blocks[0].Transactions[0].GetNonce())
	assert.Equal(t, uint256.NewInt(1000000000), blocks[0].Transactions[0].GetPrice())

	pre155, err = hex.DecodeString("e580843b9aca00830186a0941275fbb540c8efc58b812ba83b0d0b8b9917ae988084159278193d7bcd98c00060650f12c381cc2d4f4cc8abf54059aecd2c7aabcfcdd191ba6827b1e72f0eb0b8d5daae64962f4aafde7853e1c102de053edbedf066e6e3c2dc1b")
	require.NoError(t, err)
	blocks, err = DecodeBatchL2Blocks(pre155, uint64(constants.ForkID4))
	require.NoError(t, err)
	assert.Equal(t, 1, len(blocks))
	assert.Equal(t, "0x1275fbb540c8efC58b812ba83B0D0B8b9917AE98", blocks[0].Transactions[0].GetTo().String())
	assert.Equal(t, uint64(0), blocks[0].Transactions[0].GetNonce())
	assert.Equal(t, uint256.NewInt(0), blocks[0].Transactions[0].GetValue())
	assert.Equal(t, "15927819", hex.EncodeToString(blocks[0].Transactions[0].GetData()))
	assert.Equal(t, uint64(100000), blocks[0].Transactions[0].GetGas())
	assert.Equal(t, uint256.NewInt(1000000000), blocks[0].Transactions[0].GetPrice())
}

func TestComputeL2TxHashScenarios(t *testing.T) {
	tests := []struct {
		chainId        *big.Int
		nonce          uint64
		gasPrice       *uint256.Int
		gasLimit       uint64
		value          *uint256.Int
		data           string
		to             string
		from           string
		expectedTxHash string
	}{
		{
			chainId:        big.NewInt(1000),
			nonce:          0,
			gasPrice:       uint256.NewInt(1000000000),
			gasLimit:       30000000,
			value:          uint256.NewInt(0),
			data:           "0x188ec356",
			to:             "0x1275fbb540c8efc58b812ba83b0d0b8b9917ae98",
			from:           "0x4d5Cf5032B2a844602278b01199ED191A86c93ff",
			expectedTxHash: "0xf3de9c9f50d72933104d5bb109915d93e4958117de78c9a7d1a58b5c6e4cbb77",
		},
		{
			chainId:        big.NewInt(1700),
			nonce:          0,
			gasPrice:       uint256.NewInt(1000000000),
			gasLimit:       100000,
			value:          uint256.NewInt(0),
			data:           "0x56d5be740000000000000000000000001275fbb540c8efc58b812ba83b0d0b8b9917ae98",
			to:             "0x005Cf5032B2a844602278b01199ED191A86c93ff",
			from:           "0x4d5Cf5032B2a844602278b01199ED191A86c93ff",
			expectedTxHash: "0x42e14eabd58bb4f26e928cada9a74081343e9ca0aad0d4f3f4e6254cb3a805ca",
		},
		{
			chainId:        big.NewInt(1700),
			nonce:          0,
			gasPrice:       uint256.NewInt(1000000000),
			gasLimit:       100000,
			value:          uint256.NewInt(0),
			data:           "0x56d5be740000000000000000000000001275fbb540c8efc58b812ba83b0d0b8b9917ae98",
			to:             "",
			from:           "0x4d5Cf5032B2a844602278b01199ED191A86c93ff",
			expectedTxHash: "0x8f9cfb43c0f6bc7ce9f9e43e8761776a2ef9657ccf87318e2487c313d119b8cf",
		}, {
			chainId:        big.NewInt(4096),
			nonce:          0,
			gasPrice:       uint256.NewInt(1000000000),
			gasLimit:       100000,
			value:          uint256.NewInt(0),
			data:           "0x56d5be740000000000000000000000001275fbb540c8efc58b812ba83b0d0b8b9917ae98",
			to:             "",
			from:           "0x4d5Cf5032B2a844602278b01199ED191A86c93ff",
			expectedTxHash: "0xe93d9aadf9ec7453204b7f26380472820729cb401e371b473132cc3ea27d2eef",
		}, {
			chainId:        big.NewInt(1700),
			nonce:          0,
			gasPrice:       uint256.NewInt(1000000000),
			gasLimit:       100000,
			value:          uint256.NewInt(0),
			data:           "0x",
			to:             "",
			from:           "0x4d5Cf5032B2a844602278b01199ED191A86c93ff",
			expectedTxHash: "0xe8cd2bb2321ae825c970cb1b8ffd3ba6fb28488ca2a8003f9622d07d0cb2b63c",
		}, {
			chainId:        big.NewInt(1700),
			nonce:          0,
			gasPrice:       uint256.NewInt(1000000000),
			gasLimit:       100000,
			value:          uint256.NewInt(0),
			data:           "0x",
			to:             "",
			from:           "0x4d5Cf5032B2a844602278b01199ED191A86c93ff",
			expectedTxHash: "0xe8cd2bb2321ae825c970cb1b8ffd3ba6fb28488ca2a8003f9622d07d0cb2b63c",
		}, {
			chainId:        big.NewInt(2442),
			nonce:          50534,
			gasPrice:       uint256.NewInt(105300000),
			gasLimit:       30000000,
			value:          uint256.NewInt(10000000000000),
			data:           "",
			to:             "0x417a7BA2d8d0060ae6c54fd098590DB854B9C1d5",
			from:           "0x9AF3049dD15616Fd627A35563B5282bEA5C32E20",
			expectedTxHash: "0x26460f7fa46b88e6a383a496e567ba76cb307ccaa82b64fc739bfeebbef8d747",
		}, {
			chainId:        big.NewInt(2442),
			nonce:          50534,
			gasPrice:       uint256.NewInt(105300000),
			gasLimit:       21000,
			value:          uint256.NewInt(10000000000000),
			data:           "",
			to:             "0x417a7BA2d8d0060ae6c54fd098590DB854B9C1d5",
			from:           "0x9af3049dd15616fd627a35563b5282bea5c32e20",
			expectedTxHash: "0x0a3b9eafc5562a432f25398a849fd2296c717e0d9e90189d1c41e7b6ddcaa3dd",
		}, {
			chainId:        big.NewInt(2440),
			nonce:          84,
			gasPrice:       uint256.NewInt(493000000),
			gasLimit:       100000,
			value:          uint256.NewInt(1000000000000000000),
			data:           "",
			to:             "0x0000000000000000000000000000000000000000",
			from:           "0x5751D5b29dA14d5C334A9453cF04181f417aBe4c",
			expectedTxHash: "0x02c2b4bbe2d7e6a236b0c5c25f89dc729e1a2df1363912f966662759b6edab33",
		}, {
			chainId:        big.NewInt(2440),
			nonce:          87,
			gasPrice:       uint256.NewInt(493000000),
			gasLimit:       100000,
			value:          uint256.NewInt(100),
			data:           "",
			to:             "0x0000000000000000000000000000000000000003",
			from:           "0x5751D5b29dA14d5C334A9453cF04181f417aBe4c",
			expectedTxHash: "0x2d9ecd316c7106602210b7ccc290f1744308a4661bfd5f484020d5a193c63d78",
		}, {
			chainId:        big.NewInt(2440),
			nonce:          88,
			gasPrice:       uint256.NewInt(493000000),
			gasLimit:       100000,
			value:          uint256.NewInt(1000),
			data:           "",
			to:             "0x0000000000000000000000000000000000000004",
			from:           "0x5751D5b29dA14d5C334A9453cF04181f417aBe4c",
			expectedTxHash: "0x3fc26004cfe8bc6c3078fddace50c5d073109d18fc9095d37e60c471ffa1a075",
		}, {
			chainId:        big.NewInt(0),
			nonce:          1559,
			gasPrice:       uint256.NewInt(127000000),
			gasLimit:       21000,
			value:          uint256.NewInt(1309095483099999),
			data:           "",
			to:             "0xf71dbFcE95e6093b4876482A215b6C94a4787C3B",
			from:           "0x229A5bDBb09d8555f9214F7a6784804999BA4E0D",
			expectedTxHash: "0x6dc15c9aca6b03d7326ea26a9850fda56f75c1c4a560b63a9fa08cc5283e050b",
		}, {
			chainId:        big.NewInt(-1),
			nonce:          1559,
			gasPrice:       uint256.NewInt(127000000),
			gasLimit:       21000,
			value:          uint256.NewInt(1309095483099999),
			data:           "",
			to:             "0xf71dbFcE95e6093b4876482A215b6C94a4787C3B",
			from:           "0x229A5bDBb09d8555f9214F7a6784804999BA4E0D",
			expectedTxHash: "0x6dc15c9aca6b03d7326ea26a9850fda56f75c1c4a560b63a9fa08cc5283e050b",
		},
	}

	for i, test := range tests {
		dataBytes, err := zkhex.DecodeHex(test.data)
		if err != nil {
			t.Fatalf("Test %d: unexpected error: %v", i+1, err)
		}
		var to, from *common.Address

		if test.to != "" {
			a := common.HexToAddress(test.to)
			to = &a
		}
		if test.from != "" {
			a := common.HexToAddress(test.from)
			from = &a
		}

		var chainId *big.Int
		if test.chainId.Cmp(big.NewInt(-1)) != 0 {
			chainId = test.chainId
		}

		result, err := ComputeL2TxHash(
			chainId,
			test.value,
			test.gasPrice,
			test.nonce,
			test.gasLimit,
			to,
			from,
			dataBytes,
		)
		if err != nil {
			t.Fatalf("Test %d: unexpected error: %v", i+1, err)
		}

		resultString := result.Hex()
		if resultString != test.expectedTxHash {
			t.Fatalf("Test %d: expected tx hash %s, got %s", i+1, test.expectedTxHash, resultString)
		}
	}

}

func BenchmarkComputeL2TxHashSt(b *testing.B) {
	chainId := big.NewInt(2440)
	nonce := uint64(87)
	gasPrice := uint256.NewInt(493000000)
	gasLimit := uint64(100000)
	value := uint256.NewInt(100)
	data := []byte{}
	to := common.HexToAddress("0x5751D5b29dA14d5C334A9453cF04181f417aBe4c")
	from := common.HexToAddress("0x5751D5b29dA14d5C334A9453cF04181f417aBe4c")

	for i := 0; i < b.N; i++ {
		_, _ = ComputeL2TxHash(chainId, value, gasPrice, nonce, gasLimit, &to, &from, data)
	}
}

type testCase struct {
	param       interface{}
	paramLength int
	expected    string
	expectError bool
}

func TestFormatL2TxHashParam(t *testing.T) {
	cases := map[string]testCase{
		"int":           {param: 0, paramLength: 8, expected: "0000000000000000", expectError: false},
		"int64":         {param: int64(123), paramLength: 3, expected: "00007b", expectError: false},
		"uint":          {param: uint(456), paramLength: 2, expected: "01c8", expectError: false},
		"string":        {param: "abcdef", paramLength: 4, expected: "00abcdef", expectError: false},
		"big":           {param: big.NewInt(789), paramLength: 2, expected: "0315", expectError: false},
		"uint8 slice":   {param: []uint8{0xab, 0xcd, 0xef}, paramLength: 2, expected: "abcdef", expectError: false},
		"uint8 slice 2": {param: []uint8{24, 142, 195, 86}, paramLength: 4, expected: "188ec356", expectError: false},
		"hex string":    {param: "0x00", paramLength: 8, expected: "0000000000000000", expectError: false},
		"more hex":      {param: "0x0186a0", paramLength: 8, expected: "00000000000186a0", expectError: false},
		"address hex":   {param: "0x1275fbb540c8efc58b812ba83b0d0b8b9917ae98", paramLength: 20, expected: "1275fbb540c8efc58b812ba83b0d0b8b9917ae98", expectError: false},
		"int 4":         {param: 4, paramLength: 3, expected: "000004", expectError: false},
		"uint265":       {param: uint256.NewInt(1000), paramLength: 8, expected: "00000000000003e8", expectError: false},
		"invalid hex":   {param: "0xzz", paramLength: 8, expected: "", expectError: true},
	}

	for n, tc := range cases {
		t.Run(n, func(t *testing.T) {
			result, err := formatL2TxHashParam(tc.param, tc.paramLength)
			if tc.expectError {
				if err == nil {
					t.Errorf("Expected an error for param %v but got none", tc.param)
				}
			} else {
				if err != nil {
					t.Errorf("Unexpected error for param %v: %v", tc.param, err)
				}
				if result != tc.expected {
					t.Errorf("Expected %v, got %v for param %v", tc.expected, result, tc.param)
				}
			}
		})
	}
}

func BenchmarkFormatL2TxHashParam(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, _ = formatL2TxHashParam(uint256.NewInt(1000), 8)
	}
}

func Test_EncodeToBatchL2DataAndBack(t *testing.T) {
	toAddress := common.HexToAddress("0x1")
	tx := &types.LegacyTx{
		CommonTx: types.CommonTx{
			TransactionMisc: types.TransactionMisc{},
			ChainID:         uint256.NewInt(987),
			Nonce:           2,
			Gas:             3,
			To:              &toAddress,
			Value:           uint256.NewInt(4),
			Data:            []byte{5},
			V:               *uint256.NewInt(2009),
			R:               *uint256.NewInt(7),
			S:               *uint256.NewInt(8),
		},
		GasPrice: uint256.NewInt(100),
	}

	encoded, err := TransactionToL2Data(tx, 7, 255)
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := DecodeBatchL2Blocks(encoded, 7)
	if err != nil {
		t.Fatal(err)
	}

	if len(decoded) != 1 {
		t.Errorf("expected 1 block but found %v", len(decoded))
	}
	if len(decoded[0].Transactions) != 1 {
		t.Errorf("expected 1 transaction but found %v", len(decoded[0].Transactions))
	}

	toCompare := decoded[0].Transactions[0]
	require.Equal(t, tx, toCompare)
}

func Test_BlockBatchL2DataEncode(t *testing.T) {
	toAddress := common.HexToAddress("0x1")
	tx := &types.LegacyTx{
		CommonTx: types.CommonTx{
			TransactionMisc: types.TransactionMisc{},
			ChainID:         uint256.NewInt(5),
			Nonce:           2,
			Gas:             3,
			To:              &toAddress,
			Value:           uint256.NewInt(4),
			Data:            []byte{5},
			V:               *uint256.NewInt(19),
			R:               *uint256.NewInt(7),
			S:               *uint256.NewInt(8),
		},
		GasPrice: uint256.NewInt(100),
	}

	expected, err := TransactionToL2Data(tx, 7, 255)
	if err != nil {
		t.Fatal(err)
	}

	batchTransactionData := []BatchTxData{
		{
			Transaction:                 tx,
			EffectiveGasPricePercentage: 255,
		},
	}

	batchL2Data, err := GenerateBlockBatchL2Data(7, 1, 2, batchTransactionData)
	if err != nil {
		t.Fatal(err)
	}

	// get the tx part
	txBytes := batchL2Data[9:]
	require.Equal(t, expected, txBytes, "transaction bytes mismatch")

	// tx type 11 for the start
	require.Equal(t, byte(11), batchL2Data[0], "expected change l2 block transaction type in first position")

	// delta and info tree as expected
	expectedDeltaBytes := make([]byte, 0)
	expectedDeltaBytes = binary.BigEndian.AppendUint32(expectedDeltaBytes, 1)
	require.Equal(t, expectedDeltaBytes, batchL2Data[1:5], "mismatch in delta timestamp")

	expectedInfoTreeBytes := make([]byte, 0)
	expectedInfoTreeBytes = binary.BigEndian.AppendUint32(expectedInfoTreeBytes, 2)
	require.Equal(t, expectedInfoTreeBytes, batchL2Data[5:9], "mismatch in l1 info tree")
}

func Test_BatchL2DataWithMultipleEmptyBlocks(t *testing.T) {
	testData := "0b000001f4000000000b000001f400000000"
	decoded, err := hex.DecodeString(testData)
	if err != nil {
		t.Fatal(err)
	}
	blocks, err := DecodeBatchL2Blocks(decoded, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 2 {
		t.Fatalf("expected 2 blocks but found %v", len(blocks))
	}
}

func Test_OnlyOneBlockReturnedWithOneBlockInData(t *testing.T) {
	testData := "0b0000005b00000001ed0985119a1c74008252089451c06a3e11b3b9540dbd3d1697508af105a4dd15880de0b6b3a76400008081ea80805949d75c266f9ac75b829c9cc0d5ce6519f7ef042c3f230589d56a43ca89c8240d69ce59e021384ad6c0c3c44c9008ea6a7e041a475364e5ec7c253e3bf4840b1bff"
	decoded, err := hex.DecodeString(testData)
	if err != nil {
		t.Fatal(err)
	}
	blocks, err := DecodeBatchL2Blocks(decoded, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 1 {
		t.Fatalf("expected 1 blocks but found %v", len(blocks))
	}
}

func Test_ComplexMixOfBlocks(t *testing.T) {
	testData := "0b00000001000000010b00000002000000020b0000000300000003ed0985119a1c74008252089451c06a3e11b3b9540dbd3d1697508af105a4dd15880de0b6b3a76400008081ea80805949d75c266f9ac75b829c9cc0d5ce6519f7ef042c3f230589d56a43ca89c8240d69ce59e021384ad6c0c3c44c9008ea6a7e041a475364e5ec7c253e3bf4840b1bff0b0000000400000004"
	decoded, err := hex.DecodeString(testData)
	if err != nil {
		t.Fatal(err)
	}
	blocks, err := DecodeBatchL2Blocks(decoded, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 4 {
		t.Fatalf("expected 4 blocks but found %v", len(blocks))
	}

	// test data increments the l1 info index and delta for each block
	for idx, block := range blocks {
		if block.L1InfoTreeIndex != uint32(idx)+1 {
			t.Errorf("block %v expected l1 info tree index %v but got %v", idx, idx+1, block.L1InfoTreeIndex)
		}
		if block.DeltaTimestamp != uint32(idx)+1 {
			t.Errorf("block %v expected delta timestamp %v but got %v", idx, idx+1, block.DeltaTimestamp)
		}
	}
}
