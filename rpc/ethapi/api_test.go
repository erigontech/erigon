package ethapi

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

func TestNewRPCTransaction_NullSignature(t *testing.T) {
	to := common.HexToAddress("0x1234567890123456789012345678901234567890")

	// v=r=s=0: unsigned transaction — V/R/S must be nil (JSON null), compatible with geth
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
	// yParity=0, r≠0, s≠0: valid EIP-1559 transaction — V must be "0x0", not nil
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
