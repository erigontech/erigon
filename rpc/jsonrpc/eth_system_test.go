// Copyright 2024 The Erigon Authors
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

package jsonrpc

import (
	"context"
	"encoding/json"
	"math"
	"math/big"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

func TestCapabilities(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	t.Parallel()

	// Use a small prune distance so tests don't need to generate 100k blocks.
	const chainSize = 20
	const testPruneDistance = uint64(10)

	testFullMode := prune.Mode{
		Initialised: true,
		History:     prune.Distance(testPruneDistance),
		Blocks:      prune.DefaultBlocksPruneMode, // MaxUint64 = keeps all block snapshots
	}
	testMinimalMode := prune.Mode{
		Initialised: true,
		History:     prune.Distance(testPruneDistance),
		Blocks:      prune.Distance(testPruneDistance),
	}

	setupAPI := func(t *testing.T, pruneMode prune.Mode, commitmentHistory bool) (*APIImpl, uint64) {
		t.Helper()
		key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		addr := crypto.PubkeyToAddress(key.PublicKey)
		gspec := &types.Genesis{
			Config: chain.TestChainBerlinConfig,
			Alloc:  types.GenesisAlloc{addr: {Balance: big.NewInt(math.MaxInt64)}},
		}
		m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(key))

		// Generate and insert blocks so Execution stage progress is set.
		signer := types.LatestSigner(gspec.Config)
		c, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, chainSize, func(i int, b *blockgen.BlockGen) {
			b.SetCoinbase(common.Address{1})
			tx, txErr := types.SignTx(types.NewTransaction(b.TxNonce(addr), common.HexToAddress("deadbeef"), uint256.NewInt(1), 21000, uint256.NewInt(uint64(i+1)*common.GWei), nil), *signer, key)
			if txErr != nil {
				t.Fatal(txErr)
			}
			b.AddTx(tx)
		})
		require.NoError(t, err)
		require.NoError(t, m.InsertChain(c))

		// Write prune mode and commitment history flag.
		// prune.EnsureNotChanged writes on empty keys; execmoduletester never pre-populates them.
		ctx := t.Context()
		tx, err := m.DB.BeginTemporalRw(ctx)
		require.NoError(t, err)
		defer tx.Rollback()
		_, err = prune.EnsureNotChanged(tx, pruneMode)
		require.NoError(t, err)
		require.NoError(t, rawdb.WriteDBCommitmentHistoryEnabled(tx, commitmentHistory))
		require.NoError(t, tx.Commit())

		roTx, err := m.DB.BeginTemporalRo(ctx)
		require.NoError(t, err)
		defer roTx.Rollback()
		head, err := stages.GetStageProgress(roTx, stages.Execution)
		require.NoError(t, err)

		return newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil), head
	}

	oldest := func(t *testing.T, f CapabilityField) uint64 {
		t.Helper()
		require.NotNil(t, f.OldestBlock)
		return uint64(*f.OldestBlock)
	}

	t.Run("archive_no_commitment", func(t *testing.T) {
		t.Parallel()
		api, _ := setupAPI(t, prune.ArchiveMode, false)
		result, err := api.Capabilities(t.Context())
		require.NoError(t, err)
		require.Equal(t, uint64(0), oldest(t, result.State))
		require.Equal(t, uint64(0), oldest(t, result.Tx))
		require.Equal(t, uint64(0), oldest(t, result.Logs))
		require.Equal(t, uint64(0), oldest(t, result.Receipts))
		require.Equal(t, uint64(0), oldest(t, result.Blocks))
		require.True(t, result.StateProofs.Disabled)
		require.Nil(t, result.StateProofs.OldestBlock)
	})

	t.Run("archive_with_commitment", func(t *testing.T) {
		t.Parallel()
		api, _ := setupAPI(t, prune.ArchiveMode, true)
		result, err := api.Capabilities(t.Context())
		require.NoError(t, err)
		require.Equal(t, uint64(0), oldest(t, result.State))
		require.False(t, result.StateProofs.Disabled)
		require.Equal(t, uint64(0), oldest(t, result.StateProofs))
	})

	t.Run("full_no_commitment", func(t *testing.T) {
		t.Parallel()
		api, head := setupAPI(t, testFullMode, false)
		result, err := api.Capabilities(t.Context())
		require.NoError(t, err)
		pruned := head - testPruneDistance
		// state/logs/receipts limited to history prune distance
		require.Equal(t, pruned, oldest(t, result.State))
		require.Equal(t, pruned, oldest(t, result.Logs))
		require.Equal(t, pruned, oldest(t, result.Receipts))
		// full keeps all block snapshots: tx and blocks start from 0
		require.Equal(t, uint64(0), oldest(t, result.Tx))
		require.Equal(t, uint64(0), oldest(t, result.Blocks))
		require.True(t, result.StateProofs.Disabled)
	})

	t.Run("full_with_commitment", func(t *testing.T) {
		t.Parallel()
		api, head := setupAPI(t, testFullMode, true)
		result, err := api.Capabilities(t.Context())
		require.NoError(t, err)
		require.Equal(t, head-testPruneDistance, oldest(t, result.StateProofs))
		require.False(t, result.StateProofs.Disabled)
	})

	t.Run("minimal_no_commitment", func(t *testing.T) {
		t.Parallel()
		api, head := setupAPI(t, testMinimalMode, false)
		result, err := api.Capabilities(t.Context())
		require.NoError(t, err)
		pruned := head - testPruneDistance
		// minimal prunes everything including blocks and tx
		require.Equal(t, pruned, oldest(t, result.State))
		require.Equal(t, pruned, oldest(t, result.Tx))
		require.Equal(t, pruned, oldest(t, result.Logs))
		require.Equal(t, pruned, oldest(t, result.Receipts))
		require.Equal(t, pruned, oldest(t, result.Blocks))
		require.True(t, result.StateProofs.Disabled)
	})

	t.Run("minimal_with_commitment", func(t *testing.T) {
		t.Parallel()
		api, head := setupAPI(t, testMinimalMode, true)
		result, err := api.Capabilities(t.Context())
		require.NoError(t, err)
		require.Equal(t, head-testPruneDistance, oldest(t, result.StateProofs))
		require.False(t, result.StateProofs.Disabled)
	})
}

func TestGasPrice(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	cases := []struct {
		description   string
		chainSize     int
		expectedPrice *big.Int
	}{
		{
			description: "standard settings 60 blocks",
			chainSize:   60,
			// New two-phase oracle: phase1 fetches last checkBlocks=20 (blocks 41-60, prices 41-60 GWei),
			// phase2 extends by sparseCount=20 more (blocks 21-40, prices 21-40 GWei).
			// Total 40 prices [21-60], percentile 60 → index 23 → 44 GWei.
			expectedPrice: big.NewInt(common.GWei * int64(44)),
		},
		{
			description:   "standard settings 30 blocks",
			chainSize:     30,
			expectedPrice: big.NewInt(common.GWei * int64(18)),
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.description, func(t *testing.T) {
			m := createGasPriceTestKV(t, testCase.chainSize)
			defer m.DB.Close()
			eth := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)

			ctx := context.Background()
			result, err := eth.GasPrice(ctx)
			if err != nil {
				t.Fatalf("error getting gas price: %s", err)
			}

			if testCase.expectedPrice.Cmp(result.ToInt()) != 0 {
				t.Fatalf("gas price mismatch, want %d, got %d", testCase.expectedPrice, result.ToInt())
			}
		})
	}

}

func TestEthConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	t.Parallel()
	toTimeArg := func(t hexutil.Uint64) *hexutil.Uint64 { return &t }
	for _, test := range []struct {
		name                 string
		genesisFilePath      string
		head                 *types.Header
		blockTimeOverride    *hexutil.Uint64
		wantResponseFilePath string
		wantIsError          error
	}{
		{
			name:                 "hoodi prague scheduled but not activated",
			genesisFilePath:      path.Join(".", "testdata", "eth_config", "hoodi_prague_scheduled_no_osaka_no_bpos_genesis.json"),
			blockTimeOverride:    toTimeArg(1742999830),
			wantResponseFilePath: path.Join(".", "testdata", "eth_config", "hoodi_prague_scheduled_no_osaka_no_bpos_response_prague_not_activated.json"),
		},
		{
			name:                 "hoodi osaka scheduled but not activated with 5 bpos none activated",
			genesisFilePath:      path.Join(".", "testdata", "eth_config", "hoodi_osaka_scheduled_with_5_bpos_genesis.json"),
			blockTimeOverride:    toTimeArg(1753110000),
			wantResponseFilePath: path.Join(".", "testdata", "eth_config", "hoodi_osaka_scheduled_with_5_bpos_response_osaka_not_activated_bpo_none_activated.json"),
		},
		{
			name:                 "hoodi osaka scheduled and activated with 5 bpos none activated",
			genesisFilePath:      path.Join(".", "testdata", "eth_config", "hoodi_osaka_scheduled_with_5_bpos_genesis.json"),
			blockTimeOverride:    toTimeArg(1753110150),
			wantResponseFilePath: path.Join(".", "testdata", "eth_config", "hoodi_osaka_scheduled_with_5_bpos_response_osaka_activated_bpo_none_activated.json"),
		},
		{
			name:                 "hoodi osaka scheduled and activated with 5 bpos bpo1 activated",
			genesisFilePath:      path.Join(".", "testdata", "eth_config", "hoodi_osaka_scheduled_with_5_bpos_genesis.json"),
			blockTimeOverride:    toTimeArg(1753111150),
			wantResponseFilePath: path.Join(".", "testdata", "eth_config", "hoodi_osaka_scheduled_with_5_bpos_response_osaka_activated_bpo_1_activated.json"),
		},
		{
			name:                 "hoodi osaka scheduled and activated with 5 bpos bpo2 activated",
			genesisFilePath:      path.Join(".", "testdata", "eth_config", "hoodi_osaka_scheduled_with_5_bpos_genesis.json"),
			blockTimeOverride:    toTimeArg(1753112150),
			wantResponseFilePath: path.Join(".", "testdata", "eth_config", "hoodi_osaka_scheduled_with_5_bpos_response_osaka_activated_bpo_2_activated.json"),
		},
		{
			name:                 "hoodi osaka scheduled and activated with 5 bpos bpo3 activated",
			genesisFilePath:      path.Join(".", "testdata", "eth_config", "hoodi_osaka_scheduled_with_5_bpos_genesis.json"),
			blockTimeOverride:    toTimeArg(1753113150),
			wantResponseFilePath: path.Join(".", "testdata", "eth_config", "hoodi_osaka_scheduled_with_5_bpos_response_osaka_activated_bpo_3_activated.json"),
		},
		{
			name:                 "hoodi osaka scheduled and activated with 5 bpos bpo4 activated",
			genesisFilePath:      path.Join(".", "testdata", "eth_config", "hoodi_osaka_scheduled_with_5_bpos_genesis.json"),
			blockTimeOverride:    toTimeArg(1753114150),
			wantResponseFilePath: path.Join(".", "testdata", "eth_config", "hoodi_osaka_scheduled_with_5_bpos_response_osaka_activated_bpo_4_activated.json"),
		},
		{
			name:                 "hoodi osaka scheduled and activated with 5 bpos bpo5 activated",
			genesisFilePath:      path.Join(".", "testdata", "eth_config", "hoodi_osaka_scheduled_with_5_bpos_genesis.json"),
			blockTimeOverride:    toTimeArg(1753115150),
			wantResponseFilePath: path.Join(".", "testdata", "eth_config", "hoodi_osaka_scheduled_with_5_bpos_response_osaka_activated_bpo_5_activated.json"),
		},
		{
			name:                 "mainnet prague scheduled and activated no osaka no bpos",
			genesisFilePath:      path.Join(".", "testdata", "eth_config", "mainnet_prague_scheduled_no_osaka_no_bpos_genesis.json"),
			blockTimeOverride:    toTimeArg(1746612311 + 1000),
			wantResponseFilePath: path.Join(".", "testdata", "eth_config", "mainnet_prague_scheduled_no_osaka_no_bpos_response_prague_activated.json"),
		},
		{
			name:                 "mainnet prague scheduled but not activated no osaka no bpos with blockTimeOverride at shanghai",
			genesisFilePath:      path.Join(".", "testdata", "eth_config", "mainnet_prague_scheduled_no_osaka_no_bpos_genesis.json"),
			blockTimeOverride:    toTimeArg(1710338135 - 1000),
			wantResponseFilePath: path.Join(".", "testdata", "eth_config", "mainnet_prague_scheduled_no_osaka_no_bpos_response_head_at_shanghai.json"),
		},
		{
			name:                 "mainnet prague scheduled but not activated no osaka no bpos with head at shanghai",
			genesisFilePath:      path.Join(".", "testdata", "eth_config", "mainnet_prague_scheduled_no_osaka_no_bpos_genesis.json"),
			head:                 &types.Header{Number: *uint256.NewInt(123), Time: 1710338135 - 1000},
			wantResponseFilePath: path.Join(".", "testdata", "eth_config", "mainnet_prague_scheduled_no_osaka_no_bpos_response_head_at_shanghai.json"),
		},
		{
			name:                 "steel example genesis with head at genesis block and no blockTimeOverride",
			genesisFilePath:      path.Join(".", "testdata", "eth_config", "steel_example_1_genesis.json"),
			blockTimeOverride:    nil,
			head:                 nil,
			wantResponseFilePath: path.Join(".", "testdata", "eth_config", "steel_example_1_response_head_at_genesis.json"),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
			require.NoError(t, err)
			genesisBytes, err := os.ReadFile(test.genesisFilePath)
			require.NoError(t, err)
			var genesis types.Genesis
			err = json.Unmarshal(genesisBytes, &genesis)
			require.NoError(t, err)
			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&genesis), execmoduletester.WithKey(key))
			defer m.Close()
			eth := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
			if test.head != nil {
				tx, err := m.DB.BeginTemporalRw(ctx)
				require.NoError(t, err)
				defer tx.Rollback()
				rawdb.WriteForkchoiceHead(tx, test.head.Hash())
				err = rawdb.WriteHeader(tx, test.head)
				require.NoError(t, err)
				err = rawdb.WriteCanonicalHash(tx, test.head.Hash(), test.head.Number.Uint64())
				require.NoError(t, err)
				err = tx.Commit()
				require.NoError(t, err)
			}
			result, err := eth.Config(t.Context(), test.blockTimeOverride)
			require.ErrorIs(t, err, test.wantIsError)
			haveResponseBytes, err := json.MarshalIndent(result, "", "    ")
			require.NoError(t, err)
			wantResponseBytes, err := os.ReadFile(test.wantResponseFilePath)
			require.NoError(t, err)
			want, have := string(wantResponseBytes), string(haveResponseBytes)
			// replace \r\n with \n is necessary for CI on windows
			want, have = strings.ReplaceAll(want, "\r\n", "\n"), strings.ReplaceAll(have, "\r\n", "\n")
			require.Equal(t, want, have)
		})
	}
}

func createGasPriceTestKV(t *testing.T, chainSize int) *execmoduletester.ExecModuleTester {
	var (
		key, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		addr   = crypto.PubkeyToAddress(key.PublicKey)
		gspec  = &types.Genesis{
			Config: chain.TestChainBerlinConfig,
			Alloc:  types.GenesisAlloc{addr: {Balance: big.NewInt(math.MaxInt64)}},
		}
		signer = types.LatestSigner(gspec.Config)
	)
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(key))

	// Generate testing blocks
	chain, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, chainSize, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
		tx, txErr := types.SignTx(types.NewTransaction(b.TxNonce(addr), common.HexToAddress("deadbeef"), uint256.NewInt(100), 21000, uint256.NewInt(uint64(int64(i+1)*common.GWei)), nil), *signer, key)
		if txErr != nil {
			t.Fatalf("failed to create tx: %v", txErr)
		}
		b.AddTx(tx)
	})
	if err != nil {
		t.Error(err)
	}
	// Construct testing chain
	if err = m.InsertChain(chain); err != nil {
		t.Error(err)
	}

	return m
}
