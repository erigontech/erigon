package tx

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"testing"

	zkhex "github.com/ledgerwatch/erigon/zkevm/hex"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeRandomBatchL2Data(t *testing.T) {
	randomData := []byte("Random data")
	txs, _, _, err := DecodeTxs(randomData, forkID5)
	require.Error(t, err)
	assert.Equal(t, []types.Transaction{}, txs)
	t.Log("Txs decoded 1: ", txs)

	randomData = []byte("Esto es autentica basura")
	txs, _, _, err = DecodeTxs(randomData, forkID5)
	require.Error(t, err)
	assert.Equal(t, []types.Transaction{}, txs)
	t.Log("Txs decoded 2: ", txs)

	randomData = []byte("beef")
	txs, _, _, err = DecodeTxs(randomData, forkID5)
	require.Error(t, err)
	assert.Equal(t, []types.Transaction{}, txs)
	t.Log("Txs decoded 3: ", txs)
}

func TestDecodePre155BatchL2DataPreForkID5(t *testing.T) {
	pre155, err := hex.DecodeString("e480843b9aca00826163941275fbb540c8efc58b812ba83b0d0b8b9917ae98808464fbb77cb7d2a666860f3c6b8f5ef96f86c7ec5562e97fd04c2e10f3755ff3a0456f9feb246df95217bf9082f84f9e40adb0049c6664a5bb4c9cbe34ab1a73e77bab26ed1b")
	require.NoError(t, err)
	txs, _, _, err := DecodeTxs(pre155, forkID4)
	require.NoError(t, err)
	t.Log("Txs decoded: ", txs, len(txs))
	assert.Equal(t, 1, len(txs))
	v, r, s := txs[0].RawSignatureValues()
	assert.Equal(t, "0x1275fbb540c8efC58b812ba83B0D0B8b9917AE98", txs[0].GetTo().String())
	assert.Equal(t, "1b", fmt.Sprintf("%x", v))
	assert.Equal(t, "b7d2a666860f3c6b8f5ef96f86c7ec5562e97fd04c2e10f3755ff3a0456f9feb", fmt.Sprintf("%x", r))
	assert.Equal(t, "246df95217bf9082f84f9e40adb0049c6664a5bb4c9cbe34ab1a73e77bab26ed", fmt.Sprintf("%x", s))
	assert.Equal(t, uint64(24931), txs[0].GetGas())
	assert.Equal(t, "64fbb77c", hex.EncodeToString(txs[0].GetData()))
	assert.Equal(t, uint64(0), txs[0].GetNonce())
	assert.Equal(t, uint256.NewInt(1000000000), txs[0].GetPrice())

	pre155, err = hex.DecodeString("e580843b9aca00830186a0941275fbb540c8efc58b812ba83b0d0b8b9917ae988084159278193d7bcd98c00060650f12c381cc2d4f4cc8abf54059aecd2c7aabcfcdd191ba6827b1e72f0eb0b8d5daae64962f4aafde7853e1c102de053edbedf066e6e3c2dc1b")
	require.NoError(t, err)
	txs, _, _, err = DecodeTxs(pre155, forkID4)
	require.NoError(t, err)
	t.Log("Txs decoded: ", txs)
	assert.Equal(t, 1, len(txs))
	assert.Equal(t, "0x1275fbb540c8efC58b812ba83B0D0B8b9917AE98", txs[0].GetTo().String())
	assert.Equal(t, uint64(0), txs[0].GetNonce())
	assert.Equal(t, uint256.NewInt(0), txs[0].GetValue())
	assert.Equal(t, "15927819", hex.EncodeToString(txs[0].GetData()))
	assert.Equal(t, uint64(100000), txs[0].GetGas())
	assert.Equal(t, uint256.NewInt(1000000000), txs[0].GetPrice())
}

func TestDecodePre155Tx(t *testing.T) {
	pre155 := "0xf86780843b9aca00826163941275fbb540c8efc58b812ba83b0d0b8b9917ae98808464fbb77c1ba0b7d2a666860f3c6b8f5ef96f86c7ec5562e97fd04c2e10f3755ff3a0456f9feba0246df95217bf9082f84f9e40adb0049c6664a5bb4c9cbe34ab1a73e77bab26ed"
	pre155Bytes, err := hex.DecodeString(pre155[2:])
	require.NoError(t, err)
	tx, _, err := DecodeTx(pre155Bytes, 0, forkID4)
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
	tx, pct, err := DecodeTx(post155Bytes, 75, forkID5)
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
	txs, _, _, err := DecodeTxs(pre155, forkID5)
	require.NoError(t, err)
	t.Log("Txs decoded: ", txs, len(txs))
	assert.Equal(t, 1, len(txs))
	v, r, s := txs[0].RawSignatureValues()
	assert.Equal(t, "0x1275fbb540c8efC58b812ba83B0D0B8b9917AE98", txs[0].GetTo().String())
	assert.Equal(t, "1b", fmt.Sprintf("%x", v))
	assert.Equal(t, "b7d2a666860f3c6b8f5ef96f86c7ec5562e97fd04c2e10f3755ff3a0456f9feb", fmt.Sprintf("%x", r))
	assert.Equal(t, "246df95217bf9082f84f9e40adb0049c6664a5bb4c9cbe34ab1a73e77bab26ed", fmt.Sprintf("%x", s))
	assert.Equal(t, uint64(24931), txs[0].GetGas())
	assert.Equal(t, "64fbb77c", hex.EncodeToString(txs[0].GetData()))
	assert.Equal(t, uint64(0), txs[0].GetNonce())
	assert.Equal(t, uint256.NewInt(1000000000), txs[0].GetPrice())

	pre155, err = hex.DecodeString("e580843b9aca00830186a0941275fbb540c8efc58b812ba83b0d0b8b9917ae988084159278193d7bcd98c00060650f12c381cc2d4f4cc8abf54059aecd2c7aabcfcdd191ba6827b1e72f0eb0b8d5daae64962f4aafde7853e1c102de053edbedf066e6e3c2dc1b")
	require.NoError(t, err)
	txs, _, _, err = DecodeTxs(pre155, forkID4)
	require.NoError(t, err)
	t.Log("Txs decoded: ", txs)
	assert.Equal(t, 1, len(txs))
	assert.Equal(t, "0x1275fbb540c8efC58b812ba83B0D0B8b9917AE98", txs[0].GetTo().String())
	assert.Equal(t, uint64(0), txs[0].GetNonce())
	assert.Equal(t, uint256.NewInt(0), txs[0].GetValue())
	assert.Equal(t, "15927819", hex.EncodeToString(txs[0].GetData()))
	assert.Equal(t, uint64(100000), txs[0].GetGas())
	assert.Equal(t, uint256.NewInt(1000000000), txs[0].GetPrice())
}

func createTx(nonce, gasPrice, gasLimit, from, to, value, data string, chainID uint64) types.LegacyTx {
	nonceUint, _ := hexutil.DecodeUint64(nonce)
	gasPriceInt, _ := uint256.FromHex(gasPrice)
	gasLimitUint, _ := hexutil.DecodeUint64(gasLimit)
	valueInt, _ := uint256.FromHex(value)
	fromAddr := libcommon.HexToAddress(from)

	var toAddress *libcommon.Address
	if to != "" {
		addr := libcommon.HexToAddress(to)
		toAddress = &addr
	}

	tx := types.LegacyTx{
		CommonTx: types.CommonTx{
			ChainID: uint256.NewInt(chainID),
			Nonce:   nonceUint,
			Gas:     gasLimitUint,
			To:      toAddress,
			Value:   valueInt,
			Data:    hexutil.MustDecode(data),
		},
		GasPrice: gasPriceInt,
	}
	tx.SetSender(fromAddr)

	return tx
}

func TestComputeL2TxHashScenarios(t *testing.T) {
	tests := []struct {
		chainId        *big.Int
		nonce          uint64
		gasPrice       *uint256.Int
		gasLimit       uint64
		value          *uint256.Int
		data           string
		to             libcommon.Address
		from           libcommon.Address
		expectedTxHash string
	}{
		{
			chainId:        big.NewInt(1000),
			nonce:          0,
			gasPrice:       uint256.NewInt(1000000000),
			gasLimit:       30000000,
			value:          uint256.NewInt(0),
			data:           "0x188ec356",
			to:             libcommon.HexToAddress("0x1275fbb540c8efc58b812ba83b0d0b8b9917ae98"),
			from:           libcommon.HexToAddress("0x4d5Cf5032B2a844602278b01199ED191A86c93ff"),
			expectedTxHash: "0xf3de9c9f50d72933104d5bb109915d93e4958117de78c9a7d1a58b5c6e4cbb77",
		},
		{
			chainId:        big.NewInt(1700),
			nonce:          0,
			gasPrice:       uint256.NewInt(1000000000),
			gasLimit:       100000,
			value:          uint256.NewInt(0),
			data:           "0x56d5be740000000000000000000000001275fbb540c8efc58b812ba83b0d0b8b9917ae98",
			to:             libcommon.HexToAddress("0x005Cf5032B2a844602278b01199ED191A86c93ff"),
			from:           libcommon.HexToAddress("0x4d5Cf5032B2a844602278b01199ED191A86c93ff"),
			expectedTxHash: "0x42e14eabd58bb4f26e928cada9a74081343e9ca0aad0d4f3f4e6254cb3a805ca",
		},
		{
			chainId:        big.NewInt(1700),
			nonce:          0,
			gasPrice:       uint256.NewInt(1000000000),
			gasLimit:       100000,
			value:          uint256.NewInt(0),
			data:           "0x56d5be740000000000000000000000001275fbb540c8efc58b812ba83b0d0b8b9917ae98",
			to:             common.HexToAddress("0x0"),
			from:           common.HexToAddress("0x4d5Cf5032B2a844602278b01199ED191A86c93ff"),
			expectedTxHash: "0x8f9cfb43c0f6bc7ce9f9e43e8761776a2ef9657ccf87318e2487c313d119b8cf",
		}, {
			chainId:        big.NewInt(4096),
			nonce:          0,
			gasPrice:       uint256.NewInt(1000000000),
			gasLimit:       100000,
			value:          uint256.NewInt(0),
			data:           "0x56d5be740000000000000000000000001275fbb540c8efc58b812ba83b0d0b8b9917ae98",
			to:             common.HexToAddress(""),
			from:           common.HexToAddress("0x4d5Cf5032B2a844602278b01199ED191A86c93ff"),
			expectedTxHash: "0xe93d9aadf9ec7453204b7f26380472820729cb401e371b473132cc3ea27d2eef",
		}, {
			chainId:        big.NewInt(1700),
			nonce:          0,
			gasPrice:       uint256.NewInt(1000000000),
			gasLimit:       100000,
			value:          uint256.NewInt(0),
			data:           "0x",
			to:             common.HexToAddress(""),
			from:           common.HexToAddress("0x4d5Cf5032B2a844602278b01199ED191A86c93ff"),
			expectedTxHash: "0xe8cd2bb2321ae825c970cb1b8ffd3ba6fb28488ca2a8003f9622d07d0cb2b63c",
		}, {
			chainId:        big.NewInt(1700),
			nonce:          0,
			gasPrice:       uint256.NewInt(1000000000),
			gasLimit:       100000,
			value:          uint256.NewInt(0),
			data:           "0x",
			to:             common.HexToAddress(""),
			from:           common.HexToAddress("0x4d5Cf5032B2a844602278b01199ED191A86c93ff"),
			expectedTxHash: "0xe8cd2bb2321ae825c970cb1b8ffd3ba6fb28488ca2a8003f9622d07d0cb2b63c",
		}, {
			chainId:        big.NewInt(2442),
			nonce:          50534,
			gasPrice:       uint256.NewInt(105300000),
			gasLimit:       30000000,
			value:          uint256.NewInt(10000000000000),
			data:           "",
			to:             common.HexToAddress("0x417a7BA2d8d0060ae6c54fd098590DB854B9C1d5"),
			from:           common.HexToAddress("0x9AF3049dD15616Fd627A35563B5282bEA5C32E20"),
			expectedTxHash: "0x26460f7fa46b88e6a383a496e567ba76cb307ccaa82b64fc739bfeebbef8d747",
		}, {
			chainId:        big.NewInt(2442),
			nonce:          50534,
			gasPrice:       uint256.NewInt(105300000),
			gasLimit:       21000,
			value:          uint256.NewInt(10000000000000),
			data:           "",
			to:             common.HexToAddress("0x417a7BA2d8d0060ae6c54fd098590DB854B9C1d5"),
			from:           common.HexToAddress("0x9af3049dd15616fd627a35563b5282bea5c32e20"),
			expectedTxHash: "0x0a3b9eafc5562a432f25398a849fd2296c717e0d9e90189d1c41e7b6ddcaa3dd",
		},
	}

	for i, test := range tests {
		dataBytes, err := zkhex.DecodeHex(test.data)
		if err != nil {
			t.Fatalf("Test %d: unexpected error: %v", i+1, err)
		}
		result, err := ComputeL2TxHash(
			test.chainId,
			test.value,
			test.gasPrice,
			test.nonce,
			test.gasLimit,
			&test.to,
			&test.from,
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
