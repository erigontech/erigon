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
	"context"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/db/kv"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/txnprovider"
)

type pbtBuilderTransactions struct{ txns []types.Transaction }

func (p *pbtBuilderTransactions) ProvideTxns(_ context.Context, _ ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	txns := p.txns
	p.txns = nil
	return txns, nil
}

func TestPBTBuilderCanonicalCommitment(t *testing.T) {
	t.Run("dual", func(t *testing.T) { testPBTBuilderCanonicalCommitment(t, true) })
	t.Run("binary_only", func(t *testing.T) { testPBTBuilderCanonicalCommitment(t, false) })
}

func testPBTBuilderCanonicalCommitment(t *testing.T, dual bool) {
	previousBin, previousDual := statecfg.ExperimentalBinCommitment, statecfg.ExperimentalHexBinCommitment
	previousParallel, previousHash := statecfg.ExperimentalParallelCommitment, statecfg.BinCommitmentHash
	previousSuite := commitment.PBinHashSuiteName()
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment, statecfg.ExperimentalHexBinCommitment = previousBin, previousDual
		statecfg.ExperimentalParallelCommitment, statecfg.BinCommitmentHash = previousParallel, previousHash
		require.NoError(t, commitment.SetPBinHashSuite(previousSuite))
	})
	statecfg.ExperimentalBinCommitment, statecfg.ExperimentalHexBinCommitment = true, dual
	statecfg.ExperimentalParallelCommitment, statecfg.BinCommitmentHash = false, ""
	config := chain.AllProtocolChanges.Copy()
	amsterdam, activation := uint64(0), uint64(30)
	if !dual {
		activation = 0
	}
	config.AmsterdamTime, config.BinaryTrieTime = &amsterdam, &activation
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	from := crypto.PubkeyToAddress(key.PublicKey)
	genesis := &types.Genesis{Config: config, GasLimit: 30_000_000, BaseFee: uint256.NewInt(0),
		Alloc: types.GenesisAlloc{from: {Balance: new(big.Int).SetUint64(common.Ether)}}}
	options := []execmoduletester.Option{execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(key)}
	if dual {
		options = append(options, execmoduletester.WithEnableDomain(kv.CommitmentBinDomain))
	}
	m := execmoduletester.New(t, options...)
	seedDualGenesis(t, m, genesis)
	require.NoError(t, m.ExecModule.ResetCurrentContext(t.Context()))
	parent := m.Genesis
	cases := []string{"before_activation", "before_activation_again", "at_activation", "after_activation", "frozen_hex"}
	if !dual {
		cases = []string{"first_block", "second_block"}
	}
	for i, name := range cases {
		if !t.Run(name, func(t *testing.T) {
			if name == "frozen_hex" {
				tx, err := m.DB.BeginTemporalRo(t.Context())
				require.NoError(t, err)
				defer tx.Rollback()
				value, _, err := tx.GetLatest(kv.CommitmentDomain, commitment.KeyCommitmentState, kv.GetLatestOptions{})
				require.NoError(t, err)
				txNum, _ := commitmentdb.DecodeTxBlockNums(value)
				require.NoError(t, m.DB.(dbstate.HasAgg).Agg().(*dbstate.Aggregator).FreezeDomain(kv.CommitmentDomain, txNum))
				tx.Rollback()
			}
			txn, err := types.SignTx(types.NewTransaction(uint64(i), common.Address{1}, uint256.NewInt(1), 2_000_000, uint256.NewInt(0), nil), *types.LatestSignerForChainID(config.ChainID), key)
			require.NoError(t, err)
			txn.SetSender(accounts.InternAddress(from))
			slot := uint64(i + 1)
			built, err := m.BlockBuilder.Build(t.Context(), &builder.Parameters{
				ParentHash: parent.Hash(), Timestamp: slot * 10, Withdrawals: []*types.Withdrawal{},
				ParentBeaconBlockRoot: &common.Hash{}, SlotNumber: &slot,
				CustomTxnProvider: &pbtBuilderTransactions{txns: []types.Transaction{txn}},
			}, nil)
			require.NoError(t, err)
			require.Len(t, built.Block.Transactions(), 1)
			require.Len(t, built.Receipts, 1)
			require.Equal(t, uint64(1), built.Receipts[0].Status)
			pack := &blockgen.ChainPack{Headers: []*types.Header{built.Block.Header()}, Blocks: []*types.Block{built.Block}, Receipts: []types.Receipts{built.Receipts}, TopBlock: built.Block}
			require.NoError(t, m.InsertChain(pack))
			require.NoError(t, m.ExecModule.ResetCurrentContext(t.Context()))
			parent = built.Block
		}) {
			break
		}
	}
}
