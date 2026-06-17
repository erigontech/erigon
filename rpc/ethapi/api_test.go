package ethapi

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

func TestNewRPCTransaction_NullSignature(t *testing.T) {
	to := common.HexToAddress("0x1234567890123456789012345678901234567890")

	tx := &types.LegacyTx{
		CommonTx: types.CommonTx{
			Nonce:    1,
			GasLimit: 21000,
			To:       &to,
		},
	}
	result := NewRPCTransaction(tx, common.Hash{}, 0, 0, 0, nil)
	require.Nil(t, result.V)
	require.Nil(t, result.R)
	require.Nil(t, result.S)
}

func TestNewRPCTransaction_SignedLegacy(t *testing.T) {
	to := common.HexToAddress("0x1234567890123456789012345678901234567890")

	tx := &types.LegacyTx{
		CommonTx: types.CommonTx{
			Nonce:    1,
			GasLimit: 21000,
			To:       &to,
			V:        *uint256.NewInt(27),
			R:        *uint256.NewInt(1),
			S:        *uint256.NewInt(2),
		},
	}
	result := NewRPCTransaction(tx, common.Hash{}, 0, 0, 0, nil)
	require.NotNil(t, result.V)
	require.NotNil(t, result.R)
	require.NotNil(t, result.S)
}

func TestNewRPCTransaction_EIP1559_YParityZero(t *testing.T) {
	to := common.HexToAddress("0x1234567890123456789012345678901234567890")
	chainID := uint256.NewInt(1)

	tx := &types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{
			Nonce:    1,
			GasLimit: 21000,
			To:       &to,
			V:        *uint256.NewInt(0), // yParity=0
			R:        *uint256.NewInt(1),
			S:        *uint256.NewInt(2),
		},
		ChainID: *chainID,
	}
	result := NewRPCTransaction(tx, common.Hash{}, 0, 0, 0, nil)
	require.NotNil(t, result.V)
	require.NotNil(t, result.R)
	require.NotNil(t, result.S)
	require.EqualValues(t, 0, result.V.ToInt().Int64())
}

func TestNewRPCTransaction_EIP1559_AllZeroSig(t *testing.T) {
	to := common.HexToAddress("0x1234567890123456789012345678901234567890")
	chainID := uint256.NewInt(1)

	tx := &types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{
			Nonce:    0,
			GasLimit: 21000,
			To:       &to,
		},
		ChainID: *chainID,
	}
	result := NewRPCTransaction(tx, common.Hash{}, 0, 0, 0, nil)
	require.NotNil(t, result.V)
	require.NotNil(t, result.R)
	require.NotNil(t, result.S)
	require.EqualValues(t, 0, result.V.ToInt().Int64())
	require.EqualValues(t, 0, result.R.ToInt().Int64())
	require.EqualValues(t, 0, result.S.ToInt().Int64())
}

func txFields(t *testing.T, r SignTransactionResult) map[string]json.RawMessage {
	t.Helper()
	data, err := json.Marshal(r)
	require.NoError(t, err)
	var outer struct {
		Tx json.RawMessage `json:"tx"`
	}
	require.NoError(t, json.Unmarshal(data, &outer))
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(outer.Tx, &fields))
	return fields
}

func TestSignTransactionResultMarshalJSON_EIP1559(t *testing.T) {
	to := common.HexToAddress("0x1234567890123456789012345678901234567890")
	chainID := uint256.NewInt(1)
	tx := &types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{
			Nonce:    1,
			GasLimit: 21000,
			To:       &to,
		},
		ChainID: *chainID,
		TipCap:  *uint256.NewInt(1e9),
		FeeCap:  *uint256.NewInt(2e9),
	}
	var buf bytes.Buffer
	require.NoError(t, tx.MarshalBinary(&buf))

	fields := txFields(t, SignTransactionResult{Raw: buf.Bytes(), Tx: NewRPCTransaction(tx, common.Hash{}, 0, 0, 0, nil)})

	// EIP-1559: gasPrice must be explicit null.
	gasPrice, ok := fields["gasPrice"]
	require.True(t, ok, "gasPrice must be present")
	require.Equal(t, "null", string(gasPrice))

	// maxFeePerGas and maxPriorityFeePerGas must be set.
	require.NotEqual(t, "null", string(fields["maxFeePerGas"]))
	require.NotEqual(t, "null", string(fields["maxPriorityFeePerGas"]))

	// unsigned tx: v/r/s must be "0x0", not null.
	require.Equal(t, `"0x0"`, string(fields["v"]))
	require.Equal(t, `"0x0"`, string(fields["r"]))
	require.Equal(t, `"0x0"`, string(fields["s"]))

	// block-placement and sender fields must be stripped.
	for _, k := range []string{"from", "blockHash", "blockNumber", "blockTimestamp", "transactionIndex"} {
		_, present := fields[k]
		require.False(t, present, "%s must be absent", k)
	}
}

func TestSignTransactionResultMarshalJSON_Legacy(t *testing.T) {
	to := common.HexToAddress("0x1234567890123456789012345678901234567890")
	tx := &types.LegacyTx{
		CommonTx: types.CommonTx{
			Nonce:    1,
			GasLimit: 21000,
			To:       &to,
		},
		GasPrice: *uint256.NewInt(1e9),
	}
	var buf bytes.Buffer
	require.NoError(t, tx.MarshalBinary(&buf))

	fields := txFields(t, SignTransactionResult{Raw: buf.Bytes(), Tx: NewRPCTransaction(tx, common.Hash{}, 0, 0, 0, nil)})

	// Legacy: gasPrice must be set.
	gasPrice, ok := fields["gasPrice"]
	require.True(t, ok, "gasPrice must be present")
	require.NotEqual(t, "null", string(gasPrice))

	// maxFeePerGas and maxPriorityFeePerGas must be explicit null.
	maxFee, ok := fields["maxFeePerGas"]
	require.True(t, ok, "maxFeePerGas must be present")
	require.Equal(t, "null", string(maxFee))
	maxPrio, ok := fields["maxPriorityFeePerGas"]
	require.True(t, ok, "maxPriorityFeePerGas must be present")
	require.Equal(t, "null", string(maxPrio))

	// unsigned tx: v/r/s must be "0x0", not null.
	require.Equal(t, `"0x0"`, string(fields["v"]))
	require.Equal(t, `"0x0"`, string(fields["r"]))
	require.Equal(t, `"0x0"`, string(fields["s"]))
}

func TestNewRPCTransaction_AccessList_AllZeroSig(t *testing.T) {
	to := common.HexToAddress("0x1234567890123456789012345678901234567890")
	chainID := uint256.NewInt(1)

	tx := &types.AccessListTx{
		LegacyTx: types.LegacyTx{
			CommonTx: types.CommonTx{
				Nonce:    0,
				GasLimit: 21000,
				To:       &to,
			},
		},
		ChainID: *chainID,
	}
	result := NewRPCTransaction(tx, common.Hash{}, 0, 0, 0, nil)
	require.NotNil(t, result.V)
	require.NotNil(t, result.R)
	require.NotNil(t, result.S)
	require.EqualValues(t, 0, result.V.ToInt().Int64())
	require.EqualValues(t, 0, result.R.ToInt().Int64())
	require.EqualValues(t, 0, result.S.ToInt().Int64())
}
