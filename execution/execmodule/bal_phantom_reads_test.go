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

package execmodule_test

import (
	"math/big"
	"slices"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestBALSystemReadsAfterFullBlock(t *testing.T) {
	for _, name := range []string{"default contracts", "custom contracts"} {
		t.Run(name, func(t *testing.T) {
			config := chain.AllProtocolChanges.Copy()
			if name == "custom contracts" {
				config.WithdrawalRequestContract = new(common.HexToAddress("0x1001"))
				config.ConsolidationRequestContract = new(common.HexToAddress("0x1002"))
				config.BuilderDepositContract = new(common.HexToAddress("0x1003"))
				config.BuilderExitContract = new(common.HexToAddress("0x1004"))
			}
			m, pack := generateFullBALBlock(t, config)
			require.NoError(t, m.InsertChain(pack))
		})
	}
}

func TestBALPhantomReadsAfterFullBlock(t *testing.T) {
	for _, systemContract := range []bool{false, true} {
		name := "ordinary contract"
		wantError := "phantom declared reads"
		if systemContract {
			name = "system contract"
			wantError = "block access list mismatch"
		}
		t.Run(name, func(t *testing.T) {
			m, pack := generateFullBALBlock(t, chain.AllProtocolChanges)
			addr := common.HexToAddress("0x1234")
			if systemContract {
				addr = m.ChainConfig.GetWithdrawalRequestContract().Value()
			}
			bal := pack.TopBlock.BlockAccessList().Copy()
			idx := slices.IndexFunc(bal, func(ac types.AccountChanges) bool { return ac.Address == addr })
			require.NotEqual(t, -1, idx)
			bal[idx].StorageReads = append(bal[idx].StorageReads, accounts.InternKey(common.HexToHash("0xffff")))
			require.NoError(t, bal.Validate())
			require.NoError(t, bal.ValidateMaxItems(pack.TopBlock.GasLimit()))

			header := pack.TopBlock.Header()
			header.BlockAccessListHash = new(bal.Hash())
			block := types.NewBlockFromNetwork(header, pack.TopBlock.Body(), types.NewBlockAccessListSidecar(bal))
			err := m.InsertValidateAndUfc1By1(t.Context(), []*types.Block{block})
			require.ErrorContains(t, err, wantError)
		})
	}
}

func generateFullBALBlock(t *testing.T, config *chain.Config) (*execmoduletester.ExecModuleTester, *blockgen.ChainPack) {
	t.Helper()
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	burner := common.HexToAddress("0x1234")
	genesis := &types.Genesis{
		Config:   config,
		GasLimit: 100_000,
		Alloc: types.GenesisAlloc{
			sender: {Balance: big.NewInt(common.Ether)},
			burner: {Balance: new(big.Int), Code: []byte{0xfe}},
		},
	}
	m := execmoduletester.New(t,
		execmoduletester.WithGenesisSpec(genesis),
		execmoduletester.WithKey(key),
		execmoduletester.WithExperimentalBAL(),
	)
	pack, err := m.GenerateChain(1, func(_ int, gen *blockgen.BlockGen) {
		tx, err := types.SignTx(types.NewTransaction(0, burner, uint256.NewInt(0), genesis.GasLimit,
			uint256.NewInt(m.Genesis.BaseFee().Uint64()*2), nil),
			*types.LatestSignerForChainID(m.ChainConfig.ChainID), key)
		require.NoError(t, err)
		gen.AddTx(tx)
	})
	require.NoError(t, err)
	require.Equal(t, pack.TopBlock.GasLimit(), pack.TopBlock.GasUsed())
	bal := pack.TopBlock.BlockAccessList()
	require.NoError(t, bal.ValidateMaxItems(pack.TopBlock.GasLimit()))
	require.Positive(t, bal.CountDeclaredReads())
	return m, pack
}
