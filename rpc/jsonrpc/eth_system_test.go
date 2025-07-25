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

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/execution/stages/mock"
)

func TestGasPrice(t *testing.T) {

	cases := []struct {
		description   string
		chainSize     int
		expectedPrice *big.Int
	}{
		{
			description:   "standard settings 60 blocks",
			chainSize:     60,
			expectedPrice: big.NewInt(common.GWei * int64(36)),
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
			eth := NewEthAPI(newBaseApiForTest(m), m.DB, nil, nil, nil, 5000000, ethconfig.Defaults.RPCTxFeeCap, 100_000, false, 100_000, 128, log.New())

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
	t.Parallel()
	for _, test := range []struct {
		name                 string
		genesisFilePath      string
		timeArg              hexutil.Uint64
		wantResponseFilePath string
		wantIsError          error
	}{
		{
			name:                 "hoodi prague scheduled but not activated",
			genesisFilePath:      path.Join(".", "testdata", "eth_config", "hoodi_prague_scheduled_no_osaka_no_bpos_genesis.json"),
			timeArg:              hexutil.Uint64(1742999830),
			wantResponseFilePath: path.Join(".", "testdata", "eth_config", "hoodi_prague_scheduled_no_osaka_no_bpos_response_prague_not_activated.json"),
		},
		{
			name:                 "hoodi osaka scheduled but not activated with 5 bpos none activated",
			genesisFilePath:      path.Join(".", "testdata", "eth_config", "hoodi_osaka_scheduled_with_5_bpos_genesis.json"),
			timeArg:              hexutil.Uint64(1753110000),
			wantResponseFilePath: path.Join(".", "testdata", "eth_config", "hoodi_osaka_scheduled_with_5_bpos_response_osaka_not_activated_bpo_none_activated.json"),
		},
		{
			name:                 "hoodi osaka scheduled and activated with 5 bpos none activated",
			genesisFilePath:      path.Join(".", "testdata", "eth_config", "hoodi_osaka_scheduled_with_5_bpos_genesis.json"),
			timeArg:              hexutil.Uint64(1753110150),
			wantResponseFilePath: path.Join(".", "testdata", "eth_config", "hoodi_osaka_scheduled_with_5_bpos_response_osaka_activated_bpo_none_activated.json"),
		},
		{
			name:                 "hoodi osaka scheduled and activated with 5 bpos bpo1 activated",
			genesisFilePath:      path.Join(".", "testdata", "eth_config", "hoodi_osaka_scheduled_with_5_bpos_genesis.json"),
			timeArg:              hexutil.Uint64(1753111150),
			wantResponseFilePath: path.Join(".", "testdata", "eth_config", "hoodi_osaka_scheduled_with_5_bpos_response_osaka_activated_bpo_1_activated.json"),
		},
		{
			name:                 "hoodi osaka scheduled and activated with 5 bpos bpo2 activated",
			genesisFilePath:      path.Join(".", "testdata", "eth_config", "hoodi_osaka_scheduled_with_5_bpos_genesis.json"),
			timeArg:              hexutil.Uint64(1753112150),
			wantResponseFilePath: path.Join(".", "testdata", "eth_config", "hoodi_osaka_scheduled_with_5_bpos_response_osaka_activated_bpo_2_activated.json"),
		},
		{
			name:                 "hoodi osaka scheduled and activated with 5 bpos bpo3 activated",
			genesisFilePath:      path.Join(".", "testdata", "eth_config", "hoodi_osaka_scheduled_with_5_bpos_genesis.json"),
			timeArg:              hexutil.Uint64(1753113150),
			wantResponseFilePath: path.Join(".", "testdata", "eth_config", "hoodi_osaka_scheduled_with_5_bpos_response_osaka_activated_bpo_3_activated.json"),
		},
		{
			name:                 "hoodi osaka scheduled and activated with 5 bpos bpo4 activated",
			genesisFilePath:      path.Join(".", "testdata", "eth_config", "hoodi_osaka_scheduled_with_5_bpos_genesis.json"),
			timeArg:              hexutil.Uint64(1753114150),
			wantResponseFilePath: path.Join(".", "testdata", "eth_config", "hoodi_osaka_scheduled_with_5_bpos_response_osaka_activated_bpo_4_activated.json"),
		},
		{
			name:                 "hoodi osaka scheduled and activated with 5 bpos bpo5 activated",
			genesisFilePath:      path.Join(".", "testdata", "eth_config", "hoodi_osaka_scheduled_with_5_bpos_genesis.json"),
			timeArg:              hexutil.Uint64(1753115150),
			wantResponseFilePath: path.Join(".", "testdata", "eth_config", "hoodi_osaka_scheduled_with_5_bpos_response_osaka_activated_bpo_5_activated.json"),
		},
		{
			name:                 "mainnet prague scheduled and activated no osaka no bpos",
			genesisFilePath:      path.Join(".", "testdata", "eth_config", "mainnet_prague_scheduled_no_osaka_no_bpos_genesis.json"),
			timeArg:              hexutil.Uint64(1746612311 + 1000),
			wantResponseFilePath: path.Join(".", "testdata", "eth_config", "mainnet_prague_scheduled_no_osaka_no_bpos_response_prague_activated.json"),
		},
		{
			name:                 "no support for timestamps before cancun",
			genesisFilePath:      path.Join(".", "testdata", "eth_config", "mainnet_prague_scheduled_no_osaka_no_bpos_genesis.json"),
			timeArg:              hexutil.Uint64(1710338135 - 1000),
			wantResponseFilePath: path.Join(".", "testdata", "eth_config", "response_empty.json"),
			wantIsError:          ErrForkTimeBeforeCancun,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
			require.NoError(t, err)
			genesisBytes, err := os.ReadFile(test.genesisFilePath)
			require.NoError(t, err)
			var genesis types.Genesis
			err = json.Unmarshal(genesisBytes, &genesis)
			require.NoError(t, err)
			m := mock.MockWithGenesis(t, &genesis, key, false)
			defer m.Close()
			eth := NewEthAPI(newBaseApiForTest(m), m.DB, nil, nil, nil, 5000, ethconfig.Defaults.RPCTxFeeCap, 10_000, false, 10_000, 128, log.New())

			timeArg := test.timeArg
			result, err := eth.Config(t.Context(), &timeArg)
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

func createGasPriceTestKV(t *testing.T, chainSize int) *mock.MockSentry {
	var (
		key, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		addr   = crypto.PubkeyToAddress(key.PublicKey)
		gspec  = &types.Genesis{
			Config: chain.TestChainConfig,
			Alloc:  types.GenesisAlloc{addr: {Balance: big.NewInt(math.MaxInt64)}},
		}
		signer = types.LatestSigner(gspec.Config)
	)
	m := mock.MockWithGenesis(t, gspec, key, false)

	// Generate testing blocks
	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, chainSize, func(i int, b *core.BlockGen) {
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
