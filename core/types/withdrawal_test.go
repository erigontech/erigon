package types

import (
	"testing"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/u256"
)

func TestWithdrawalsHash(t *testing.T) {
	w := &Withdrawal{
		Index:     0,
		Validator: 0,
		Address:   libcommon.HexToAddress("0x6295ee1b4f6dd65047762f924ecd367c17eabf8f"),
		Amount:    *u256.Num1,
	}
	withdrawals := Withdrawals([]*Withdrawal{w})
	hash := DeriveSha(withdrawals)
	// The only trie node is short (its RLP < 32 bytes).
	// Its Keccak should be returned, not the node itself.
	assert.Equal(t, libcommon.HexToHash("82cc6fbe74c41496b382fcdf25216c5af7bdbb5a3929e8f2e61bd6445ab66436"), hash)
}

// Test taken from: https://github.com/ethereum/consensus-spec-tests/tree/master/tests/mainnet/capella/ssz_static/Withdrawal/ssz_random/case_1
var testWithdrawalEncodedSSZ = common.Hex2Bytes("09b99ded9629457f21c3c177a3cf80dedbbcbcbeee17b2395d5d3f839fc1ba3559d1a73ef53b8a5325e25ad2")
var testWithdrawalsSSZHash = libcommon.HexToHash("c1ec17957781f09ab3d8dbfcdfaa6c3b40a1679d3d124588f77a2da5ebb3555f")
var testWithdrawal = &Withdrawal{
	Index:     9170781944418253065,
	Validator: 16033042974434771745,
	Address:   libcommon.HexToAddress("0xdbbcbcbeee17b2395d5d3f839fc1ba3559d1a73e"),
	Amount:    *uint256.NewInt(15157676145812061173),
}

func TestWithdrawalSSZ(t *testing.T) {
	withdrawal := &Withdrawal{}
	require.NoError(t, withdrawal.DecodeSSZ(testWithdrawalEncodedSSZ))
	require.Equal(t, withdrawal, testWithdrawal)
	require.Equal(t, withdrawal.EncodeSSZ(), testWithdrawalEncodedSSZ)
	hashSSZ, err := withdrawal.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, libcommon.Hash(hashSSZ), testWithdrawalsSSZHash)
}
