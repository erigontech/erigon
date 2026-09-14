package types

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestBlockAccessListRejectsAddresslessAccount(t *testing.T) {
	t.Parallel()
	// c1 = list(len 1) containing c0 = empty list (an account with no address).
	input := common.FromHex("0xc1c0")
	if _, err := DecodeBlockAccessListBytes(input); err == nil {
		t.Fatal("expected error decoding addressless account, got nil")
	}
}

func TestBlockAccessListRejectsMissingCodeChanges(t *testing.T) {
	t.Parallel()
	canonical := BlockAccessList{{
		Address:      accounts.InternAddress(common.HexToAddress("0xaa")),
		NonceChanges: []*NonceChange{{Index: 1, Value: 7}},
	}}
	encoded, err := EncodeBlockAccessListBytes(canonical)
	require.NoError(t, err)
	require.Equal(t, "dedd9400000000000000000000000000000000000000aac0c0c0c3c20107c0", common.Bytes2Hex(encoded))
	decoded, err := DecodeBlockAccessListBytes(encoded)
	require.NoError(t, err)
	require.Equal(t, canonical, decoded)

	truncated := common.Hex2Bytes("dddc9400000000000000000000000000000000000000aac0c0c0c3c20107")
	_, err = DecodeBlockAccessListBytes(truncated)
	require.Error(t, err, "missing code changes must be rejected")
	require.NotErrorIs(t, err, ErrInvalidBlockAccessList)
}

func TestBlockAccessListRequiresAccountFields(t *testing.T) {
	t.Parallel()
	fields := []rlp.RawValue{
		common.Hex2Bytes("9400000000000000000000000000000000000000aa"),
		{0xc0}, {0xc0}, {0xc0}, {0xc0}, {0xc0},
	}
	for i, name := range []string{"storage changes", "storage reads", "balance changes", "nonce changes", "code changes"} {
		t.Run(name, func(t *testing.T) {
			encoded, err := rlp.EncodeToBytes([][]rlp.RawValue{fields[:i+1]})
			require.NoError(t, err)

			t.Run("BAL decoder", func(t *testing.T) {
				_, err := DecodeBlockAccessListBytes(encoded)
				require.Error(t, err)
				require.NotErrorIs(t, err, ErrInvalidBlockAccessList)
			})
			t.Run("RLP decoder", func(t *testing.T) {
				var decoded BlockAccessList
				require.Error(t, rlp.DecodeBytes(encoded, &decoded))
			})
		})
	}
}

func TestSlotChangesRequiresChanges(t *testing.T) {
	t.Parallel()
	truncated := rlp.RawValue{0xc1, 0x01}
	t.Run("slot", func(t *testing.T) {
		var decoded SlotChanges
		require.Error(t, rlp.DecodeBytes(truncated, &decoded))
	})

	slots, err := rlp.EncodeToBytes([]rlp.RawValue{truncated})
	require.NoError(t, err)
	t.Run("slot list", func(t *testing.T) {
		var decoded []SlotChanges
		require.Error(t, rlp.DecodeBytes(slots, &decoded))
	})

	fields := []rlp.RawValue{
		common.Hex2Bytes("9400000000000000000000000000000000000000aa"),
		slots, {0xc0}, {0xc0}, {0xc0}, {0xc0},
	}
	encoded, err := rlp.EncodeToBytes([][]rlp.RawValue{fields})
	require.NoError(t, err)
	t.Run("BAL decoder", func(t *testing.T) {
		_, err := DecodeBlockAccessListBytes(encoded)
		require.Error(t, err)
		require.NotErrorIs(t, err, ErrInvalidBlockAccessList)
	})
	t.Run("RLP decoder", func(t *testing.T) {
		var decoded BlockAccessList
		require.Error(t, rlp.DecodeBytes(encoded, &decoded))
	})
}
