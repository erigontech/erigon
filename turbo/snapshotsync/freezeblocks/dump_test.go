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

package freezeblocks_test

import (
	"context"
	"math/big"
	"runtime"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/chain/networkname"
	"github.com/erigontech/erigon-lib/chain/snapcfg"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon-lib/log/v3"
	types2 "github.com/erigontech/erigon-lib/types"

	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/crypto"
	"github.com/erigontech/erigon/ethdb/prune"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	"github.com/erigontech/erigon/rlp"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/turbo/stages/mock"
)

func nonceRange(from, to int) []uint64 {
	a := make([]uint64, to-from+1)
	for i := range a {
		a[i] = uint64(from + i)
	}
	return a
}

func baseIdRange(base, indexer, len int) []uint64 {
	a := make([]uint64, len)
	index := 0

	for i := range a {
		a[i] = uint64(base + index)
		index += indexer
	}
	return a
}

func TestDump(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fix me on win")
	}

	type test struct {
		chainConfig *chain.Config
		chainSize   int
	}

	withConfig := func(config chain.Config, sprints map[string]uint64) *chain.Config {
		bor := *config.Bor.(*borcfg.BorConfig)
		bor.Sprint = sprints
		config.Bor = &bor
		return &config
	}

	tests := []test{
		{
			chainSize:   5,
			chainConfig: params.TestChainConfig,
		},
		{
			chainSize:   50,
			chainConfig: params.TestChainConfig,
		},
		{
			chainSize:   1000,
			chainConfig: params.BorDevnetChainConfig,
		},
		{
			chainSize:   2000,
			chainConfig: params.BorDevnetChainConfig,
		},
		{
			chainSize: 1000,
			chainConfig: withConfig(*params.BorDevnetChainConfig,
				map[string]uint64{
					"0":    64,
					"800":  16,
					"1600": 8,
				}),
		},
		{
			chainSize: 2000,
			chainConfig: withConfig(*params.BorDevnetChainConfig,
				map[string]uint64{
					"0":    64,
					"800":  16,
					"1600": 8,
				}),
		},
	}

	for _, test := range tests {
		m := createDumpTestKV(t, test.chainConfig, test.chainSize)
		chainID, _ := uint256.FromBig(m.ChainConfig.ChainID)
		t.Run("txs", func(t *testing.T) {
			require := require.New(t)
			slot := types2.TxSlot{}
			parseCtx := types2.NewTxParseContext(*chainID)
			parseCtx.WithSender(false)
			var sender [20]byte

			var systemTxs int
			var nonceList []uint64
			_, err := freezeblocks.DumpTxs(m.Ctx, m.DB, m.ChainConfig, 0, uint64(2*test.chainSize), nil, func(v []byte) error {
				if v == nil {
					systemTxs++
				} else {
					if _, err := parseCtx.ParseTransaction(v[1+20:], 0, &slot, sender[:], false /* hasEnvelope */, false /* wrappedWithBlobs */, nil); err != nil {
						return err
					}
					nonceList = append(nonceList, slot.Nonce)
				}
				return nil
			}, 1, log.LvlInfo, log.New())
			require.NoError(err)
			require.Equal(2*(test.chainSize+1), systemTxs)
			require.Equal(nonceRange(0, test.chainSize-1), nonceList)
			//require.Equal(2*(test.chainSize+1)+test.chainSize, cnt)
		})
		t.Run("txs_not_from_zero", func(t *testing.T) {
			require := require.New(t)
			slot := types2.TxSlot{}
			parseCtx := types2.NewTxParseContext(*chainID)
			parseCtx.WithSender(false)
			var sender [20]byte

			var systemTxs int
			var nonceList []uint64
			_, err := freezeblocks.DumpTxs(m.Ctx, m.DB, m.ChainConfig, 2, uint64(test.chainSize), nil, func(v []byte) error {
				if v == nil {
					systemTxs++
				} else {
					if _, err := parseCtx.ParseTransaction(v[1+20:], 0, &slot, sender[:], false /* hasEnvelope */, false /* wrappedWithBlobs */, nil); err != nil {
						return err
					}
					nonceList = append(nonceList, slot.Nonce)
				}
				return nil
			}, 1, log.LvlInfo, log.New())
			require.NoError(err)
			require.Equal(2*(test.chainSize-2), systemTxs)
			require.Equal(nonceRange(1, test.chainSize-2), nonceList)
			//require.Equal(3*test.chainSize-6, cnt)
		})
		t.Run("headers", func(t *testing.T) {
			require := require.New(t)
			var nonceList []uint64
			_, err := freezeblocks.DumpHeaders(m.Ctx, m.DB, m.ChainConfig, 0, uint64(2*test.chainSize), nil, func(v []byte) error {
				h := types.Header{}
				if err := rlp.DecodeBytes(v[1:], &h); err != nil {
					return err
				}
				nonceList = append(nonceList, h.Number.Uint64())
				return nil
			}, 1, log.LvlInfo, log.New())
			require.NoError(err)
			require.Equal(nonceRange(0, test.chainSize), nonceList)
		})
		t.Run("headers_not_from_zero", func(t *testing.T) {
			require := require.New(t)
			var nonceList []uint64
			_, err := freezeblocks.DumpHeaders(m.Ctx, m.DB, m.ChainConfig, 2, uint64(test.chainSize), nil, func(v []byte) error {
				h := types.Header{}
				if err := rlp.DecodeBytes(v[1:], &h); err != nil {
					return err
				}
				nonceList = append(nonceList, h.Number.Uint64())
				return nil
			}, 1, log.LvlInfo, log.New())
			require.NoError(err)
			require.Equal(nonceRange(2, test.chainSize-1), nonceList)
		})
		t.Run("body", func(t *testing.T) {
			require := require.New(t)
			i := 0
			txsAmount := uint64(0)
			var baseIdList []uint64
			firstTxNum := uint64(0)
			_, err := freezeblocks.DumpBodies(m.Ctx, m.DB, m.ChainConfig, 0, uint64(test.chainSize-3),
				func(context.Context) uint64 { return firstTxNum },
				func(v []byte) error {
					i++
					body := &types.BodyForStorage{}
					require.NoError(rlp.DecodeBytes(v, body))
					txsAmount += uint64(body.TxCount)
					baseIdList = append(baseIdList, body.BaseTxnID.U64())
					return nil
				}, 1, log.LvlInfo, log.New())
			require.NoError(err)
			require.Equal(test.chainSize-3, i)
			require.Equal(3*(test.chainSize-3)-1, int(txsAmount))
			require.EqualValues(append([]uint64{0}, baseIdRange(2, 3, test.chainSize-4)...), baseIdList)

			firstTxNum += txsAmount
			i = 0
			baseIdList = baseIdList[:0]
			_, err = freezeblocks.DumpBodies(m.Ctx, m.DB, m.ChainConfig, 2, uint64(2*test.chainSize), func(context.Context) uint64 { return firstTxNum }, func(v []byte) error {
				i++
				body := &types.BodyForStorage{}
				require.NoError(rlp.DecodeBytes(v, body))
				txsAmount += uint64(body.TxCount)
				baseIdList = append(baseIdList, body.BaseTxnID.U64())
				return nil
			}, 1, log.LvlInfo, log.New())
			require.NoError(err)
			require.Equal(test.chainSize-1, i)
			require.Equal(firstTxNum+uint64(3*(test.chainSize-1)), txsAmount)
			require.EqualValues(baseIdRange(int(firstTxNum), 3, test.chainSize-1), baseIdList)
		})
		t.Run("body_not_from_zero", func(t *testing.T) {
			require := require.New(t)
			i := 0
			var baseIdList []uint64
			firstTxNum := uint64(1000)
			lastTxNum, err := freezeblocks.DumpBodies(m.Ctx, m.DB, m.ChainConfig, 2, uint64(test.chainSize), func(context.Context) uint64 { return firstTxNum }, func(v []byte) error {
				i++
				body := &types.BodyForStorage{}
				require.NoError(rlp.DecodeBytes(v, body))
				baseIdList = append(baseIdList, body.BaseTxnID.U64())
				return nil
			}, 1, log.LvlInfo, log.New())
			require.NoError(err)
			require.Equal(test.chainSize-2, i)
			require.EqualValues(baseIdRange(int(firstTxNum), 3, test.chainSize-2), baseIdList)
			require.Equal(lastTxNum, baseIdList[len(baseIdList)-1]+3)
			require.Equal(lastTxNum, firstTxNum+uint64(i*3))
		})
		t.Run("blocks", func(t *testing.T) {
			if test.chainSize < 1000 || test.chainSize%1000 != 0 {
				t.Skip("Block dump requires chain size to be a multiple of 1000")
			}

			require := require.New(t)

			logger := log.New()

			tmpDir, snapDir := t.TempDir(), t.TempDir()
			snConfig := snapcfg.KnownCfg(networkname.Mainnet)
			snConfig.ExpectBlocks = math.MaxUint64

			err := freezeblocks.DumpBlocks(m.Ctx, 0, uint64(test.chainSize), m.ChainConfig, tmpDir, snapDir, m.DB, 1, log.LvlInfo, logger, m.BlockReader)
			require.NoError(err)
		})
	}
}

func createDumpTestKV(t *testing.T, chainConfig *chain.Config, chainSize int) *mock.MockSentry {
	var (
		key, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		addr   = crypto.PubkeyToAddress(key.PublicKey)
		gspec  = &types.Genesis{
			Config: chainConfig,
			Alloc:  types.GenesisAlloc{addr: {Balance: (&big.Int{}).Mul(big.NewInt(math.MaxInt64), big.NewInt(int64(chainSize)))}},
		}
		signer = types.LatestSigner(gspec.Config)
	)

	m := mock.MockWithGenesisPruneMode(t, gspec, key, chainSize, prune.DefaultMode, false)

	// Generate testing blocks
	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, chainSize, func(i int, b *core.BlockGen) {
		b.SetCoinbase(libcommon.Address{1})
		tx, txErr := types.SignTx(types.NewTransaction(b.TxNonce(addr), libcommon.HexToAddress("deadbeef"), uint256.NewInt(100), 21000, uint256.NewInt(uint64(int64(i+1)*params.GWei)), nil), *signer, key)
		if txErr != nil {
			t.Fatalf("failed to create tx: %v", txErr)
		}
		b.AddTx(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
	// Construct testing chain
	if err = m.InsertChain(chain); err != nil {
		t.Fatal(err)
	}

	return m
}
