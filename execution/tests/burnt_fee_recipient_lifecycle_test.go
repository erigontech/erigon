// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package executiontests

import (
	"crypto/ecdsa"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var burntRecipient = common.HexToAddress("0xb0b0")

// burntFeeConfig sends the base fee to burntRecipient, as Gnosis does.
func burntFeeConfig(base *chain.Config) *chain.Config {
	config := base.Copy()
	config.BurntContract = map[string]common.Address{"0": burntRecipient}
	return config
}

func londonConfig() *chain.Config {
	config := chain.TestChainBerlinConfig.Copy()
	config.LondonBlock = common.NewUint64(0)
	return config
}

func pragueConfig() *chain.Config {
	config := chain.TestChainOsakaConfig.Copy()
	config.OsakaTime = nil
	return config
}

// baseFeePaid is the base fee paid by the tx at index i of the block.
func baseFeePaid(chainPack *blockgen.ChainPack, i int) uint256.Int {
	return *uint256.NewInt(chainPack.Receipts[0][i].GasUsed * chainPack.TopBlock.BaseFee().Uint64())
}

// importSerially imports the block with serial execution and checks the resulting state.
func importSerially(t *testing.T, gspec *types.Genesis, key *ecdsa.PrivateKey, chainPack *blockgen.ChainPack, check func(t *testing.T, st *state.IntraBlockState)) {
	t.Helper()
	prevParallel := dbg.Exec3Parallel
	t.Cleanup(func() { dbg.Exec3Parallel = prevParallel })
	t.Run("serial", func(t *testing.T) {
		dbg.Exec3Parallel = false
		m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(key))
		require.NoError(t, m.InsertChain(chainPack))
		require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
			st := state.New(m.NewStateReader(tx))
			defer st.Close()
			check(t, st)
			return nil
		}))
	})
}

// An empty burnt-fee recipient is touched by a zero-value CALL in a tx that pays
// it a base fee. The fee is credited before the end-of-tx EIP-161 sweep, so the
// account survives with its storage.
func TestBurntFeeKeepsTouchedEmptyRecipient(t *testing.T) {
	toucher := common.HexToAddress("0x7007")
	for _, tc := range []struct {
		name    string
		config  *chain.Config
		storage map[common.Hash]common.Hash
	}{
		{"london", londonConfig(), map[common.Hash]common.Hash{common.BytesToHash([]byte{1}): common.BytesToHash([]byte{1})}},
		{"prague", pragueConfig(), map[common.Hash]common.Hash{common.BytesToHash([]byte{1}): common.BytesToHash([]byte{1})}},
		{"prague without storage", pragueConfig(), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			alloc := types.GenesisAlloc{
				toucher:        {Code: callCode(burntRecipient, 0)},
				burntRecipient: {Balance: big.NewInt(0), Storage: tc.storage},
			}
			keys := revivalKeys(t, alloc, 1)
			gspec := &types.Genesis{Config: burntFeeConfig(tc.config), GasLimit: 30_000_000, Alloc: alloc}
			chainPack := buildBlockSerially(t, gspec, keys, common.Address{1}, 10_000_000_000, []revivalCall{{to: toucher}})
			check := func(t *testing.T, st *state.IntraBlockState) {
				balance, err := st.GetBalance(accounts.InternAddress(burntRecipient))
				require.NoError(t, err)
				require.Equal(t, baseFeePaid(chainPack, 0), balance)
				if tc.storage != nil {
					require.Equal(t, *uint256.NewInt(1), storageAt(t, st, burntRecipient, 1))
				}
			}
			importSerially(t, gspec, keys[0], chainPack, check)
			importInParallel(t, gspec, keys[0], chainPack, check)
		})
	}
}
