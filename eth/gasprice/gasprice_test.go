// Copyright 2020 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package gasprice

import (
	"context"
	"math"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rpc"
)

type testBackend struct {
	db  ethdb.Database
	cfg *params.ChainConfig
}

func (b *testBackend) HeaderByNumber(ctx context.Context, number rpc.BlockNumber) (*types.Header, error) {
	if number == rpc.LatestBlockNumber {
		return rawdb.ReadCurrentHeader(b.db), nil
	}
	return rawdb.ReadHeaderByNumber(b.db, uint64(number)), nil
}

func (b *testBackend) BlockByNumber(ctx context.Context, number rpc.BlockNumber) (*types.Block, error) {
	if number == rpc.LatestBlockNumber {
		return rawdb.ReadCurrentBlockDeprecated(b.db), nil
	}
	return rawdb.ReadBlockByNumberDeprecated(b.db, uint64(number))
}

func (b *testBackend) ChainConfig() *params.ChainConfig {
	return b.cfg
}

func newTestBackend(t *testing.T) *testBackend {
	var (
		key, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		addr   = crypto.PubkeyToAddress(key.PublicKey)
		gspec  = &core.Genesis{
			Config: params.TestChainConfig,
			Alloc:  core.GenesisAlloc{addr: {Balance: big.NewInt(math.MaxInt64)}},
		}
		signer = types.LatestSigner(gspec.Config)
	)
	engine := ethash.NewFaker()
	db := ethdb.NewTestDB(t)
	genesis, _, err := gspec.Commit(db, false)
	if err != nil {
		t.Fatal(err)
	}

	// Generate testing blocks
	chain, err := core.GenerateChain(params.TestChainConfig, genesis, engine, db.RwKV(), 32, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
		tx, txErr := types.SignTx(types.NewTransaction(b.TxNonce(addr), common.HexToAddress("deadbeef"), uint256.NewInt(100), 21000, uint256.NewInt(uint64(int64(i+1)*params.GWei)), nil), *signer, key)
		if txErr != nil {
			t.Fatalf("failed to create tx: %v", txErr)
		}
		b.AddTx(tx)
	}, false)
	if err != nil {
		t.Error(err)
	}
	// Construct testing chain
	if _, err = stagedsync.InsertBlocksInStages(db, ethdb.DefaultStorageMode, params.TestChainConfig, &vm.Config{}, engine, chain.Blocks, true /* checkRoot */); err != nil {
		t.Error(err)
	}
	return &testBackend{db: db, cfg: params.TestChainConfig}
}

func (b *testBackend) CurrentHeader() *types.Header {
	return rawdb.ReadCurrentHeader(b.db)
}

func (b *testBackend) GetBlockByNumber(number uint64) *types.Block {
	r, _ := rawdb.ReadBlockByNumberDeprecated(b.db, number)
	return r
}

func TestSuggestPrice(t *testing.T) {
	config := Config{
		Blocks:     2,
		Percentile: 60,
		Default:    big.NewInt(params.GWei),
	}
	backend := newTestBackend(t)
	oracle := NewOracle(backend, config)

	// The gas price sampled is: 32G, 31G, 30G, 29G, 28G, 27G
	got, err := oracle.SuggestPrice(context.Background())
	if err != nil {
		t.Fatalf("Failed to retrieve recommended gas price: %v", err)
	}
	expect := big.NewInt(params.GWei * int64(30))
	if got.Cmp(expect) != 0 {
		t.Fatalf("Gas price mismatch, want %d, got %d", expect, got)
	}
}
