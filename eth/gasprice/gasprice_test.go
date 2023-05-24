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

package gasprice_test

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/eth/gasprice/gaspricecfg"
	"github.com/ledgerwatch/erigon/turbo/services"

	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/commands"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/gasprice"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/stages"
)

type testBackend struct {
	db          kv.RwDB
	cfg         *chain.Config
	blockReader services.FullBlockReader
}

func (b *testBackend) GetReceipts(ctx context.Context, hash libcommon.Hash) (types.Receipts, error) {
	tx, err := b.db.BeginRo(context.Background())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	number := rawdb.ReadHeaderNumber(tx, hash)
	if number == nil {
		return nil, nil
	}
	canonicalHash, err := rawdb.ReadCanonicalHash(tx, *number)
	if err != nil {
		return nil, fmt.Errorf("requested non-canonical hash %x. canonical=%x", hash, canonicalHash)
	}

	block, s, err := b.blockReader.BlockWithSenders(ctx, tx, hash, *number)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}
	receipts := rawdb.ReadReceipts(tx, block, s)
	return receipts, nil
}

func (b *testBackend) PendingBlockAndReceipts() (*types.Block, types.Receipts) {
	return nil, nil
	//if b.pending {
	//	block := b.chain.GetBlockByNumber(testHead + 1)
	//	return block, b.chain.GetReceiptsByHash(block.Hash())
	//}
}
func (b *testBackend) HeaderByNumber(ctx context.Context, number rpc.BlockNumber) (*types.Header, error) {
	tx, err := b.db.BeginRo(context.Background())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if number == rpc.LatestBlockNumber {
		return rawdb.ReadCurrentHeader(tx), nil
	}
	hash, err := rawdb.ReadCanonicalHash(tx, uint64(number))
	if err != nil {
		return nil, fmt.Errorf("failed ReadCanonicalHash: %w", err)
	}
	if hash == (libcommon.Hash{}) {
		return nil, nil
	}
	return b.blockReader.Header(ctx, tx, hash, uint64(number))
}

func (b *testBackend) BlockByNumber(ctx context.Context, number rpc.BlockNumber) (*types.Block, error) {
	tx, err := b.db.BeginRo(context.Background())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	if number == rpc.LatestBlockNumber {
		return b.blockReader.CurrentBlock(tx)
	}
	hash, err := rawdb.ReadCanonicalHash(tx, uint64(number))
	if err != nil {
		return nil, fmt.Errorf("failed ReadCanonicalHash: %w", err)
	}
	if hash == (libcommon.Hash{}) {
		return nil, nil
	}
	block, _, err := b.blockReader.BlockWithSenders(ctx, tx, hash, uint64(number))
	return block, err
}

func (b *testBackend) ChainConfig() *chain.Config {
	return b.cfg
}

func newTestBackend(t *testing.T) *testBackend {
	var (
		key, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		addr   = crypto.PubkeyToAddress(key.PublicKey)
		gspec  = &types.Genesis{
			Config: params.TestChainConfig,
			Alloc:  types.GenesisAlloc{addr: {Balance: big.NewInt(math.MaxInt64)}},
		}
		signer = types.LatestSigner(gspec.Config)
	)
	m := stages.MockWithGenesis(t, gspec, key, false)

	// Generate testing blocks
	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 32, func(i int, b *core.BlockGen) {
		b.SetCoinbase(libcommon.Address{1})
		tx, txErr := types.SignTx(types.NewTransaction(b.TxNonce(addr), libcommon.HexToAddress("deadbeef"), uint256.NewInt(100), 21000, uint256.NewInt(uint64(int64(i+1)*params.GWei)), nil), *signer, key)
		if txErr != nil {
			t.Fatalf("failed to create tx: %v", txErr)
		}
		b.AddTx(tx)
	}, false)
	if err != nil {
		t.Error(err)
	}
	// Construct testing chain
	if err = m.InsertChain(chain); err != nil {
		t.Error(err)
	}
	br, _ := m.NewBlocksIO()
	return &testBackend{db: m.DB, cfg: params.TestChainConfig, blockReader: br}
}

func (b *testBackend) CurrentHeader() *types.Header {
	tx, err := b.db.BeginRo(context.Background())
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()
	return rawdb.ReadCurrentHeader(tx)
}

func (b *testBackend) GetBlockByNumber(number uint64) *types.Block {
	tx, err := b.db.BeginRo(context.Background())
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()

	hash, err := rawdb.ReadCanonicalHash(tx, number)
	if err != nil {
		return nil
	}
	block, _, _ := b.blockReader.BlockWithSenders(context.Background(), tx, hash, number)
	return block
}

func TestSuggestPrice(t *testing.T) {
	config := gaspricecfg.Config{
		Blocks:     2,
		Percentile: 60,
		Default:    big.NewInt(params.GWei),
	}
	backend := newTestBackend(t)
	cache := commands.NewGasPriceCache()
	oracle := gasprice.NewOracle(backend, config, cache)

	// The gas price sampled is: 32G, 31G, 30G, 29G, 28G, 27G
	got, err := oracle.SuggestTipCap(context.Background())
	if err != nil {
		t.Fatalf("Failed to retrieve recommended gas price: %v", err)
	}
	expect := big.NewInt(params.GWei * int64(30))
	if got.Cmp(expect) != 0 {
		t.Fatalf("Gas price mismatch, want %d, got %d", expect, got)
	}
}
