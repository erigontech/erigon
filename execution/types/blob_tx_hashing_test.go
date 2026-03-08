package types

import (
	"bytes"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

func newTestBlobTx() *BlobTx {
	to := common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678")
	return &BlobTx{
		DynamicFeeTransaction: DynamicFeeTransaction{
			CommonTx: CommonTx{
				Nonce:    1,
				GasLimit: 100000,
				To:       &to,
				Value:    uint256.NewInt(1000),
				Data:     []byte("test"),
			},
			ChainID:    uint256.NewInt(1),
			TipCap:     uint256.NewInt(1000000000),
			FeeCap:     uint256.NewInt(2000000000),
			AccessList: AccessList{},
		},
		MaxFeePerBlobGas:    uint256.NewInt(3000000000),
		BlobVersionedHashes: []common.Hash{common.HexToHash("0x01abcdef")},
	}
}

func TestBlobTx_MarshalBinaryForHashing_TypeByte(t *testing.T) {
	tx := newTestBlobTx()
	var buf bytes.Buffer
	require.NoError(t, tx.MarshalBinaryForHashing(&buf))
	require.Equal(t, byte(BlobTxType), buf.Bytes()[0], "first byte must be BlobTxType (0x03)")
}

func TestBlobTx_MarshalBinaryForHashing_IncludesBlobFields(t *testing.T) {
	blobTx := newTestBlobTx()
	var blobBuf bytes.Buffer
	require.NoError(t, blobTx.MarshalBinaryForHashing(&blobBuf))

	dynTx := &blobTx.DynamicFeeTransaction
	var dynBuf bytes.Buffer
	require.NoError(t, dynTx.MarshalBinaryForHashing(&dynBuf))

	require.Greater(t, blobBuf.Len(), dynBuf.Len(),
		"BlobTx encoding must be larger than DynamicFeeTransaction (includes MaxFeePerBlobGas + BlobVersionedHashes)")
}

func TestBlobTx_MarshalBinaryForHashing_ExcludesTimeboosted(t *testing.T) {
	tx1 := newTestBlobTx()
	tb := true
	tx1.Timeboosted = &tb

	tx2 := newTestBlobTx()
	tx2.Timeboosted = nil

	var buf1, buf2 bytes.Buffer
	require.NoError(t, tx1.MarshalBinaryForHashing(&buf1))
	require.NoError(t, tx2.MarshalBinaryForHashing(&buf2))

	require.Equal(t, buf1.Bytes(), buf2.Bytes(),
		"MarshalBinaryForHashing output must be identical regardless of Timeboosted value")
	// Verify Timeboosted is restored after encoding
	require.NotNil(t, tx1.Timeboosted)
	require.True(t, *tx1.Timeboosted)
}

func TestBlobTx_MarshalBinaryForHashing_RoundTrip(t *testing.T) {
	tx := newTestBlobTx()
	var buf bytes.Buffer
	require.NoError(t, tx.MarshalBinaryForHashing(&buf))

	data := buf.Bytes()
	require.True(t, len(data) > 1, "output must not be empty")
	require.Equal(t, byte(BlobTxType), data[0], "type byte must match BlobTxType")
}

func TestAllTxTypes_MarshalBinaryForHashing_TypeByteCorrect(t *testing.T) {
	to := common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678")

	tests := []struct {
		name         string
		tx           Transaction
		expectedType byte
		hasTypePrefix bool
	}{
		{
			name: "LegacyTx",
			tx: &LegacyTx{
				CommonTx: CommonTx{
					Nonce:    1,
					GasLimit: 21000,
					To:       &to,
					Value:    uint256.NewInt(100),
				},
				GasPrice: uint256.NewInt(1000000000),
			},
			expectedType:  LegacyTxType,
			hasTypePrefix: false, // Legacy txs use RLP directly, no type prefix
		},
		{
			name: "AccessListTx",
			tx: &AccessListTx{
				LegacyTx: LegacyTx{
					CommonTx: CommonTx{
						Nonce:    1,
						GasLimit: 21000,
						To:       &to,
						Value:    uint256.NewInt(100),
					},
					GasPrice: uint256.NewInt(1000000000),
				},
				ChainID:    uint256.NewInt(1),
				AccessList: AccessList{},
			},
			expectedType:  AccessListTxType,
			hasTypePrefix: true,
		},
		{
			name: "DynamicFeeTx",
			tx: &DynamicFeeTransaction{
				CommonTx: CommonTx{
					Nonce:    1,
					GasLimit: 21000,
					To:       &to,
					Value:    uint256.NewInt(100),
				},
				ChainID:    uint256.NewInt(1),
				TipCap:     uint256.NewInt(1000000000),
				FeeCap:     uint256.NewInt(2000000000),
				AccessList: AccessList{},
			},
			expectedType:  DynamicFeeTxType,
			hasTypePrefix: true,
		},
		{
			name:         "BlobTx",
			tx:           newTestBlobTx(),
			expectedType: BlobTxType,
			hasTypePrefix: true,
		},
		{
			name: "SetCodeTx",
			tx: &SetCodeTransaction{
				DynamicFeeTransaction: DynamicFeeTransaction{
					CommonTx: CommonTx{
						Nonce:    1,
						GasLimit: 21000,
						To:       &to,
						Value:    uint256.NewInt(100),
					},
					ChainID:    uint256.NewInt(1),
					TipCap:     uint256.NewInt(1000000000),
					FeeCap:     uint256.NewInt(2000000000),
					AccessList: AccessList{},
				},
				Authorizations: []Authorization{},
			},
			expectedType:  SetCodeTxType,
			hasTypePrefix: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			marshaller, ok := tt.tx.(TransactionForHashMarshaller)
			require.True(t, ok, "%s must implement TransactionForHashMarshaller", tt.name)
			err := marshaller.MarshalBinaryForHashing(&buf)
			require.NoError(t, err)
			data := buf.Bytes()
			require.True(t, len(data) > 0, "encoding must not be empty")

			if tt.hasTypePrefix {
				require.Equal(t, byte(tt.expectedType), data[0],
					"first byte must be the tx type byte for %s", tt.name)
			}
		})
	}
}
