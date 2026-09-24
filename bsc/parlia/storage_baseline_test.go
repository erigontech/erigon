package parlia

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var _ rules.StorageBaselineEngine = (*Parlia)(nil)

// TestChapelHertzFixBaselines pins that the Chapel chain spec selects Chapel's
// hertzfix entries and no others.
func TestChapelHertzFixBaselines(t *testing.T) {
	t.Parallel()

	spec, err := chainspec.ChainSpecByName(networkname.Chapel)
	require.NoError(t, err)
	p := New(spec.Config, log.New())

	patched := accounts.InternAddress(common.HexToAddress("0x89791428868131eb109e42340ad01eb8987526b2"))
	key := accounts.InternKey(common.HexToHash("0xf1e9242398de526b8dd9c25d38e65fbb01926b8940377762d7884b8b0dcdc3b0"))

	for _, tt := range []struct {
		blockNum uint64
		txIndex  int
		value    string
	}{
		{35547779, 196, "0xf6a7831804efd2cd0a"},
		{35548081, 486, "0x114be8ecea72b64003"},
	} {
		require.Equal(t, []rules.StorageBaseline{{
			Address: patched,
			Key:     key,
			Value:   *uint256.MustFromHex(tt.value),
		}}, p.StorageBaselines(tt.blockNum, tt.txIndex))

		require.Nil(t, p.StorageBaselines(tt.blockNum, tt.txIndex-1))
		require.Nil(t, p.StorageBaselines(tt.blockNum, tt.txIndex+1))
		require.Nil(t, p.StorageBaselines(tt.blockNum+1, tt.txIndex))
	}

	// Mainnet's entries must not fire on Chapel.
	require.Nil(t, p.StorageBaselines(33851236, 89))
	require.Nil(t, p.StorageBaselines(33851236, 90))
}

func TestMainnetHertzFixBaselines(t *testing.T) {
	t.Parallel()

	p := New(&chain.Config{ChainID: uint256.NewInt(bscMainnetChainID)}, log.New())
	patched := accounts.InternAddress(common.HexToAddress("0x00000000001f8b68515EfB546542397d3293CCfd"))

	for txIndex, want := range map[int]int{89: 11, 90: 22} {
		baselines := p.StorageBaselines(33851236, txIndex)
		require.Len(t, baselines, want)
		keys := map[accounts.StorageKey]struct{}{}
		for _, baseline := range baselines {
			require.Equal(t, patched, baseline.Address)
			keys[baseline.Key] = struct{}{}
		}
		require.Len(t, keys, want)
	}

	require.Nil(t, p.StorageBaselines(35547779, 196))
}
