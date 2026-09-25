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
		txHash   common.Hash
		value    string
	}{
		{35547779, 196, common.HexToHash("0x7ce9a3cf77108fcc85c1e84e88e363e3335eca515dfcf2feb2011729878b13a7"), "0xf6a7831804efd2cd0a"},
		{35548081, 486, common.HexToHash("0xe3895eb95605d6b43ceec7876e6ff5d1c903e572bf83a08675cb684c047a695c"), "0x114be8ecea72b64003"},
	} {
		require.Equal(t, []rules.StorageBaseline{{
			Address: patched,
			Key:     key,
			Value:   *uint256.MustFromHex(tt.value),
		}}, p.StorageBaselines(tt.blockNum, tt.txIndex, tt.txHash))

		require.Nil(t, p.StorageBaselines(tt.blockNum, tt.txIndex-1, tt.txHash))
		require.Nil(t, p.StorageBaselines(tt.blockNum, tt.txIndex+1, tt.txHash))
		require.Nil(t, p.StorageBaselines(tt.blockNum+1, tt.txIndex, tt.txHash))
		require.Nil(t, p.StorageBaselines(tt.blockNum, tt.txIndex, common.Hash{}), "a different transaction at the patched position")
	}

	// Mainnet's entries must not fire on Chapel.
	require.Nil(t, p.StorageBaselines(33851236, 89, mainnetPatchedTx89))
	require.Nil(t, p.StorageBaselines(33851236, 90, mainnetPatchedTx90))
}

var (
	mainnetPatchedTx89 = common.HexToHash("0x7eba4edc7c1806d6ee1691d43513838931de5c94f9da56ec865721b402f775b0")
	mainnetPatchedTx90 = common.HexToHash("0x5217324f0711af744fe8e12d73f13fdb11805c8e29c0c095ac747b7e4563e935")
)

func TestMainnetHertzFixBaselines(t *testing.T) {
	t.Parallel()

	p := New(&chain.Config{ChainID: uint256.NewInt(bscMainnetChainID)}, log.New())
	patched := accounts.InternAddress(common.HexToAddress("0x00000000001f8b68515EfB546542397d3293CCfd"))

	for _, tt := range []struct {
		txIndex int
		txHash  common.Hash
		slots   int
	}{
		{89, mainnetPatchedTx89, 11},
		{90, mainnetPatchedTx90, 22},
	} {
		baselines := p.StorageBaselines(33851236, tt.txIndex, tt.txHash)
		require.Len(t, baselines, tt.slots)
		keys := map[accounts.StorageKey]struct{}{}
		for _, baseline := range baselines {
			require.Equal(t, patched, baseline.Address)
			keys[baseline.Key] = struct{}{}
		}
		require.Len(t, keys, tt.slots)

		require.Nil(t, p.StorageBaselines(33851236, tt.txIndex, common.Hash{}), "a different transaction at the patched position")
	}
	require.Nil(t, p.StorageBaselines(33851236, 89, mainnetPatchedTx90))

	require.Nil(t, p.StorageBaselines(35547779, 196, common.HexToHash("0x7ce9a3cf77108fcc85c1e84e88e363e3335eca515dfcf2feb2011729878b13a7")))
}
