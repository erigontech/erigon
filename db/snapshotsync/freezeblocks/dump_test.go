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
	"testing"

	"github.com/holiman/uint256"
	"github.com/jinzhu/copier"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/stages/mock"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	polychain "github.com/erigontech/erigon/polygon/chain"
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
	if testing.Short() {
		t.Skip()
	}

	type test struct {
		chainConfig *chain.Config
		chainSize   int
	}

	withConfig := func(config *chain.Config, sprints map[string]uint64) *chain.Config {
		var copy chain.Config
		copier.Copy(&copy, config)
		bor := *config.Bor.(*borcfg.BorConfig)
		bor.Sprint = sprints
		copy.Bor = &bor
		return &copy
	}

	tests := []test{
		{
			chainSize:   5,
			chainConfig: chain.TestChainConfig,
		},
		{
			chainSize:   50,
			chainConfig: chain.TestChainConfig,
		},
		{
			chainSize:   1000,
			chainConfig: polychain.BorDevnet.Config,
		},
		{
			chainSize:   2000,
			chainConfig: polychain.BorDevnet.Config,
		},
		{
			chainSize: 1000,
			chainConfig: withConfig(polychain.BorDevnet.Config,
				map[string]uint64{
					"0":    64,
					"800":  16,
					"1600": 8,
				}),
		},
		{
			chainSize: 2000,
			chainConfig: withConfig(polychain.BorDevnet.Config,
				map[string]uint64{
					"0":    64,
					"800":  16,
					"1600": 8,
				}),
		},
	}

	for _, test := range tests {
		m := createDumpTestKV(t, test.chainConfig, test.chainSize)
		t.Run("txs", func(t *testing.T) {
			require := require.New(t)

			var systemTxs int
			var nonceList []uint64

			_, err := freezeblocks.DumpTxs(m.Ctx, m.DB, m.ChainConfig, 0, uint64(2*test.chainSize), nil, func(v []byte) error {
				if v == nil {
					systemTxs++
				} else {
					txn, err := types.DecodeTransaction(v[1+20:])
					if err != nil {
						return err
					}
					nonceList = append(nonceList, txn.GetNonce())
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

			var systemTxs int
			var nonceList []uint64
			_, err := freezeblocks.DumpTxs(m.Ctx, m.DB, m.ChainConfig, 2, uint64(test.chainSize), nil, func(v []byte) error {
				if v == nil {
					systemTxs++
				} else {
					txn, err := types.DecodeTransaction(v[1+20:])
					if err != nil {
						return err
					}
					nonceList = append(nonceList, txn.GetNonce())
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
			_, err := freezeblocks.DumpHeadersRaw(m.Ctx, m.DB, m.ChainConfig, 0, uint64(2*test.chainSize), nil, func(v []byte) error {
				h := types.Header{}
				if err := rlp.DecodeBytes(v[1:], &h); err != nil {
					return err
				}
				nonceList = append(nonceList, h.Number.Uint64())
				return nil
			}, 1, log.LvlInfo, log.New(), true)
			require.NoError(err)
			require.Equal(nonceRange(0, test.chainSize), nonceList)
		})
		t.Run("headers_not_from_zero", func(t *testing.T) {
			require := require.New(t)
			var nonceList []uint64
			_, err := freezeblocks.DumpHeadersRaw(m.Ctx, m.DB, m.ChainConfig, 2, uint64(test.chainSize), nil, func(v []byte) error {
				h := types.Header{}
				if err := rlp.DecodeBytes(v[1:], &h); err != nil {
					return err
				}
				nonceList = append(nonceList, h.Number.Uint64())
				return nil
			}, 1, log.LvlInfo, log.New(), true)
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
			require.Equal(append([]uint64{0}, baseIdRange(2, 3, test.chainSize-4)...), baseIdList)

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
			require.Equal(baseIdRange(int(firstTxNum), 3, test.chainSize-1), baseIdList)
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
			require.Equal(baseIdRange(int(firstTxNum), 3, test.chainSize-2), baseIdList)
			require.Equal(lastTxNum, baseIdList[len(baseIdList)-1]+3)
			require.Equal(lastTxNum, firstTxNum+uint64(i*3))
		})
		t.Run("blocks", func(t *testing.T) {
			if test.chainSize < 1000 || test.chainSize%1000 != 0 {
				t.Skip("Block dump requires chain size to be a multiple of 1000")
			}

			require := require.New(t)

			logger := log.New()

			tmpDir, snapDir := m.Dirs.Tmp, m.Dirs.Snap
			snConfig, _ := snapcfg.KnownCfg(networkname.Mainnet)
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
		b.SetCoinbase(common.Address{1})
		tx, txErr := types.SignTx(types.NewTransaction(b.TxNonce(addr), common.HexToAddress("deadbeef"), uint256.NewInt(100), 21000, uint256.NewInt(uint64(int64(i+1)*common.GWei)), nil), *signer, key)
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
