package txn

import (
	"bytes"
	"testing"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
)

func TestArbitrumLegacyTxData_RLPEncodeDecode(t *testing.T) {
	to := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb7")
	senderOverride := common.HexToAddress("0x1234567890123456789012345678901234567890")
	legacyTx := &types.LegacyTx{
		CommonTx: types.CommonTx{
			Nonce:    42,
			GasLimit: 50000,
			To:       &to,
			Value:    uint256.NewInt(1000000),
			Data:     []byte{0x01, 0x02, 0x03, 0x04},
			V:        *uint256.NewInt(28),
			R:        *uint256.NewInt(100),
			S:        *uint256.NewInt(200),
		},
		GasPrice: uint256.NewInt(20000000000), // 20 gwei
	}

	arbLegacyTx := &ArbitrumLegacyTxData{
		LegacyTx:          legacyTx,
		HashOverride:      common.HexToHash("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"),
		EffectiveGasPrice: 15000000000, // 15 gwei
		L1BlockNumber:     1234567,
		OverrideSender:    &senderOverride,
	}

	t.Run("RLP Encode and Decode from bytes", func(t *testing.T) {
		var buf bytes.Buffer
		err := arbLegacyTx.EncodeRLP(&buf)
		require.NoError(t, err)

		encodedBytes := buf.Bytes()
		require.Equal(t, ArbitrumLegacyTxType, encodedBytes[0])

		decodedTx := &ArbitrumLegacyTxData{
			LegacyTx: &types.LegacyTx{},
		}
		stream := rlp.NewStream(bytes.NewReader(encodedBytes[1:]), uint64(len(encodedBytes)-1))
		err = decodedTx.DecodeRLP(stream)
		require.NoError(t, err)
		require.Equal(t, arbLegacyTx.Nonce, decodedTx.Nonce)
		require.Equal(t, arbLegacyTx.GasLimit, decodedTx.GasLimit)
		require.Equal(t, arbLegacyTx.To, decodedTx.To)
		require.True(t, arbLegacyTx.Value.Eq(decodedTx.Value))
		require.Equal(t, arbLegacyTx.Data, decodedTx.Data)
		require.True(t, arbLegacyTx.V.Eq(&decodedTx.V))
		require.True(t, arbLegacyTx.R.Eq(&decodedTx.R))
		require.True(t, arbLegacyTx.S.Eq(&decodedTx.S))
		require.True(t, arbLegacyTx.GasPrice.Eq(decodedTx.GasPrice))
		require.Equal(t, arbLegacyTx.HashOverride, decodedTx.HashOverride)
		require.Equal(t, arbLegacyTx.EffectiveGasPrice, decodedTx.EffectiveGasPrice)
		require.Equal(t, arbLegacyTx.L1BlockNumber, decodedTx.L1BlockNumber)
		require.Equal(t, arbLegacyTx.OverrideSender, decodedTx.OverrideSender)
	})

	t.Run("RLP Decode from Stream", func(t *testing.T) {
		var buf bytes.Buffer
		err := arbLegacyTx.EncodeRLP(&buf)
		require.NoError(t, err)

		encodedBytes := buf.Bytes()
		stream := rlp.NewStream(bytes.NewReader(encodedBytes[1:]), uint64(len(encodedBytes)-1))

		decodedTx := &ArbitrumLegacyTxData{
			LegacyTx: &types.LegacyTx{},
		}
		err = decodedTx.DecodeRLP(stream)
		require.NoError(t, err)

		require.Equal(t, arbLegacyTx.Nonce, decodedTx.Nonce)
		require.Equal(t, arbLegacyTx.GasLimit, decodedTx.GasLimit)
		require.Equal(t, arbLegacyTx.To, decodedTx.To)
		require.True(t, arbLegacyTx.Value.Eq(decodedTx.Value))
		require.Equal(t, arbLegacyTx.Data, decodedTx.Data)
		require.True(t, arbLegacyTx.V.Eq(&decodedTx.V))
		require.True(t, arbLegacyTx.R.Eq(&decodedTx.R))
		require.True(t, arbLegacyTx.S.Eq(&decodedTx.S))
		require.True(t, arbLegacyTx.GasPrice.Eq(decodedTx.GasPrice))
		require.Equal(t, arbLegacyTx.HashOverride, decodedTx.HashOverride)
		require.Equal(t, arbLegacyTx.EffectiveGasPrice, decodedTx.EffectiveGasPrice)
		require.Equal(t, arbLegacyTx.L1BlockNumber, decodedTx.L1BlockNumber)
		require.Equal(t, arbLegacyTx.OverrideSender, decodedTx.OverrideSender)
	})

	t.Run("RLP with nil OverrideSender", func(t *testing.T) {
		arbLegacyTxNoSender := &ArbitrumLegacyTxData{
			LegacyTx:          legacyTx,
			HashOverride:      common.HexToHash("0xdeadbeef"),
			EffectiveGasPrice: 25000000000,
			L1BlockNumber:     999999,
			OverrideSender:    nil,
		}

		var buf bytes.Buffer
		err := arbLegacyTxNoSender.EncodeRLP(&buf)
		require.NoError(t, err)

		decodedTx := &ArbitrumLegacyTxData{
			LegacyTx: &types.LegacyTx{},
		}
		encodedBytes := buf.Bytes()
		stream := rlp.NewStream(bytes.NewReader(encodedBytes[1:]), uint64(len(encodedBytes)-1))
		err = decodedTx.DecodeRLP(stream)
		require.NoError(t, err)
		require.Nil(t, decodedTx.OverrideSender)
		require.Equal(t, arbLegacyTxNoSender.HashOverride, decodedTx.HashOverride)
		require.Equal(t, arbLegacyTxNoSender.EffectiveGasPrice, decodedTx.EffectiveGasPrice)
		require.Equal(t, arbLegacyTxNoSender.L1BlockNumber, decodedTx.L1BlockNumber)
	})

	t.Run("Type byte verification", func(t *testing.T) {
		require.Equal(t, ArbitrumLegacyTxType, arbLegacyTx.Type())

		var buf bytes.Buffer
		err := arbLegacyTx.EncodeRLP(&buf)
		require.NoError(t, err)

		bytes := buf.Bytes()
		require.Greater(t, len(bytes), 0)
		require.Equal(t, ArbitrumLegacyTxType, bytes[0])
	})

	t.Run("LegacyTx embedding verification", func(t *testing.T) {
		var buf bytes.Buffer
		err := arbLegacyTx.EncodeRLP(&buf)
		require.NoError(t, err)

		decodedTx := &ArbitrumLegacyTxData{
			LegacyTx: &types.LegacyTx{},
		}
		encodedBytes := buf.Bytes()
		stream := rlp.NewStream(bytes.NewReader(encodedBytes[1:]), uint64(len(encodedBytes)-1))
		err = decodedTx.DecodeRLP(stream)
		require.NoError(t, err)

		require.NotNil(t, decodedTx.LegacyTx)
		require.Equal(t, legacyTx.Nonce, decodedTx.LegacyTx.Nonce)
		require.True(t, legacyTx.GasPrice.Eq(decodedTx.LegacyTx.GasPrice))
	})
}

func TestArbitrumLegacyTxData_ComplexScenarios(t *testing.T) {
	t.Run("Contract creation transaction", func(t *testing.T) {
		legacyTx := &types.LegacyTx{
			CommonTx: types.CommonTx{
				Nonce:    1,
				GasLimit: 1000000,
				To:       nil, // Contract creation
				Value:    uint256.NewInt(0),
				Data:     []byte{0x60, 0x80, 0x60, 0x40},
				V:        *uint256.NewInt(27),
				R:        *uint256.NewInt(1),
				S:        *uint256.NewInt(2),
			},
			GasPrice: uint256.NewInt(1000000000),
		}

		arbLegacyTx := &ArbitrumLegacyTxData{
			LegacyTx:          legacyTx,
			HashOverride:      common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111"),
			EffectiveGasPrice: 900000000,
			L1BlockNumber:     100,
			OverrideSender:    nil,
		}

		var buf bytes.Buffer
		err := arbLegacyTx.EncodeRLP(&buf)
		require.NoError(t, err)

		decodedTx := &ArbitrumLegacyTxData{
			LegacyTx: &types.LegacyTx{},
		}
		encodedBytes := buf.Bytes()
		stream := rlp.NewStream(bytes.NewReader(encodedBytes[1:]), uint64(len(encodedBytes)-1))
		err = decodedTx.DecodeRLP(stream)
		require.NoError(t, err)

		require.Nil(t, decodedTx.To)
		require.Equal(t, arbLegacyTx.Data, decodedTx.Data)
	})

	t.Run("Large values", func(t *testing.T) {
		maxUint256 := new(uint256.Int)
		maxUint256.SetAllOne()

		to := common.HexToAddress("0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")
		legacyTx := &types.LegacyTx{
			CommonTx: types.CommonTx{
				Nonce:    ^uint64(0),
				GasLimit: ^uint64(0),
				To:       &to,
				Value:    maxUint256,
				Data:     make([]byte, 1000),
				V:        *maxUint256,
				R:        *maxUint256,
				S:        *maxUint256,
			},
			GasPrice: maxUint256,
		}

		arbLegacyTx := &ArbitrumLegacyTxData{
			LegacyTx:          legacyTx,
			HashOverride:      common.HexToHash("0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"),
			EffectiveGasPrice: ^uint64(0),
			L1BlockNumber:     ^uint64(0),
			OverrideSender:    &to,
		}

		var buf bytes.Buffer
		err := arbLegacyTx.EncodeRLP(&buf)
		require.NoError(t, err)

		decodedTx := &ArbitrumLegacyTxData{
			LegacyTx: &types.LegacyTx{},
		}
		encodedBytes := buf.Bytes()
		stream := rlp.NewStream(bytes.NewReader(encodedBytes[1:]), uint64(len(encodedBytes)-1))
		err = decodedTx.DecodeRLP(stream)
		require.NoError(t, err)

		require.Equal(t, arbLegacyTx.Nonce, decodedTx.Nonce)
		require.Equal(t, arbLegacyTx.GasLimit, decodedTx.GasLimit)
		require.True(t, arbLegacyTx.Value.Eq(decodedTx.Value))
		require.Equal(t, arbLegacyTx.EffectiveGasPrice, decodedTx.EffectiveGasPrice)
		require.Equal(t, arbLegacyTx.L1BlockNumber, decodedTx.L1BlockNumber)
	})
}

func TestArbitrumLegacyTxData_TypeByteHandling(t *testing.T) {
	to := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb7")
	legacyTx := &types.LegacyTx{
		CommonTx: types.CommonTx{
			Nonce:    100,
			GasLimit: 21000,
			To:       &to,
			Value:    uint256.NewInt(1000000),
			Data:     []byte{0x12, 0x34},
			V:        *uint256.NewInt(28),
			R:        *uint256.NewInt(1),
			S:        *uint256.NewInt(2),
		},
		GasPrice: uint256.NewInt(30000000000),
	}

	arbLegacyTx := &ArbitrumLegacyTxData{
		LegacyTx:          legacyTx,
		HashOverride:      common.HexToHash("0xabcdef"),
		EffectiveGasPrice: 25000000000,
		L1BlockNumber:     999999,
		OverrideSender:    nil,
	}

	t.Run("EncodeRLP writes type byte first", func(t *testing.T) {
		var buf bytes.Buffer
		err := arbLegacyTx.EncodeRLP(&buf)
		require.NoError(t, err)

		encoded := buf.Bytes()
		require.Greater(t, len(encoded), 1)
		require.Equal(t, ArbitrumLegacyTxType, encoded[0])

		decoded := &ArbitrumLegacyTxData{
			LegacyTx: &types.LegacyTx{},
		}
		stream := rlp.NewStream(bytes.NewReader(encoded[1:]), uint64(len(encoded)-1))
		err = decoded.DecodeRLP(stream)
		require.NoError(t, err)

		require.Equal(t, arbLegacyTx.HashOverride, decoded.HashOverride)
		require.Equal(t, arbLegacyTx.EffectiveGasPrice, decoded.EffectiveGasPrice)
		require.Equal(t, arbLegacyTx.L1BlockNumber, decoded.L1BlockNumber)
		require.Equal(t, arbLegacyTx.Nonce, decoded.Nonce)
	})

	t.Run("Round-trip with type byte", func(t *testing.T) {
		var buf bytes.Buffer
		err := arbLegacyTx.EncodeRLP(&buf)
		require.NoError(t, err)

		encoded := buf.Bytes()
		require.Equal(t, ArbitrumLegacyTxType, encoded[0])

		// Decode skipping type byte
		decoded := &ArbitrumLegacyTxData{
			LegacyTx: &types.LegacyTx{},
		}
		stream := rlp.NewStream(bytes.NewReader(encoded[1:]), uint64(len(encoded)-1))
		err = decoded.DecodeRLP(stream)
		require.NoError(t, err)

		// Re-encode and compare
		var buf2 bytes.Buffer
		err = decoded.EncodeRLP(&buf2)
		require.NoError(t, err)

		require.Equal(t, encoded, buf2.Bytes())
	})
}

func TestArbitrumLegacyTxData_ArbTxIntegration(t *testing.T) {
	to := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb7")

	legacyTx := &types.LegacyTx{
		CommonTx: types.CommonTx{
			Nonce:    10,
			GasLimit: 21000,
			To:       &to,
			Value:    uint256.NewInt(1000),
			Data:     []byte{},
			V:        *uint256.NewInt(28),
			R:        *uint256.NewInt(1000),
			S:        *uint256.NewInt(2000),
		},
		GasPrice: uint256.NewInt(10000000000),
	}

	arbLegacyTxData := &ArbitrumLegacyTxData{
		LegacyTx:          legacyTx,
		HashOverride:      common.HexToHash("0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"),
		EffectiveGasPrice: 9000000000,
		L1BlockNumber:     500000,
		OverrideSender:    nil,
	}

	arbTx := NewArbTx(arbLegacyTxData)
	require.Equal(t, ArbitrumLegacyTxType, arbTx.Type())

	// Encode using the inner transaction's EncodeRLP (which includes type byte)
	var buf bytes.Buffer
	err := arbLegacyTxData.EncodeRLP(&buf)
	require.NoError(t, err)

	encodedBytes := buf.Bytes()

	// Verify first byte is the type
	require.Equal(t, ArbitrumLegacyTxType, encodedBytes[0])

	// Decode using ArbTx's decodeTyped (skip the type byte)
	newArbTx := &ArbTx{}
	decoded, err := newArbTx.decodeTyped(encodedBytes, true)
	require.NoError(t, err)

	decodedArbLegacy, ok := decoded.(*ArbitrumLegacyTxData)
	require.True(t, ok, "Decoded transaction should be ArbitrumLegacyTxData")

	// Verify all fields
	require.Equal(t, arbLegacyTxData.HashOverride, decodedArbLegacy.HashOverride)
	require.Equal(t, arbLegacyTxData.EffectiveGasPrice, decodedArbLegacy.EffectiveGasPrice)
	require.Equal(t, arbLegacyTxData.L1BlockNumber, decodedArbLegacy.L1BlockNumber)
	require.Equal(t, arbLegacyTxData.Nonce, decodedArbLegacy.Nonce)
	require.Equal(t, arbLegacyTxData.GasLimit, decodedArbLegacy.GasLimit)
}

func TestArbitrumLegacyTxData_TypeBasedDecodingPattern(t *testing.T) {
	to := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb7")
	legacyTx := &types.LegacyTx{
		CommonTx: types.CommonTx{
			Nonce:    42,
			GasLimit: 50000,
			To:       &to,
			Value:    uint256.NewInt(1000000),
			Data:     []byte{0x01, 0x02, 0x03, 0x04},
			V:        *uint256.NewInt(28),
			R:        *uint256.NewInt(100),
			S:        *uint256.NewInt(200),
		},
		GasPrice: uint256.NewInt(20000000000),
	}

	arbLegacyTx := &ArbitrumLegacyTxData{
		LegacyTx:          legacyTx,
		HashOverride:      common.HexToHash("0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"),
		EffectiveGasPrice: 15000000000,
		L1BlockNumber:     1234567,
		OverrideSender:    nil,
	}

	var buf bytes.Buffer
	err := arbLegacyTx.EncodeRLP(&buf)
	require.NoError(t, err)

	encoded := buf.Bytes()
	require.Greater(t, len(encoded), 0)

	txType := encoded[0]
	require.Equal(t, ArbitrumLegacyTxType, txType)

	var decodedTx types.Transaction
	switch txType {
	case ArbitrumLegacyTxType:
		decodedTx = &ArbitrumLegacyTxData{
			LegacyTx: &types.LegacyTx{},
		}
	default:
		t.Fatalf("Unknown transaction type: 0x%x", txType)
	}

	stream := rlp.NewStream(bytes.NewReader(encoded[1:]), uint64(len(encoded)-1))
	err = decodedTx.(*ArbitrumLegacyTxData).DecodeRLP(stream)
	require.NoError(t, err)

	decoded := decodedTx.(*ArbitrumLegacyTxData)
	require.Equal(t, arbLegacyTx.HashOverride, decoded.HashOverride)
	require.Equal(t, arbLegacyTx.EffectiveGasPrice, decoded.EffectiveGasPrice)
	require.Equal(t, arbLegacyTx.L1BlockNumber, decoded.L1BlockNumber)
	require.Equal(t, arbLegacyTx.Nonce, decoded.Nonce)
}
