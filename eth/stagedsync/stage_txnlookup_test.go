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

package stagedsync_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/eth/stagedsync"
	"github.com/erigontech/erigon/turbo/stages/mock"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/crypto"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/ethdb/prune"
	"github.com/erigontech/erigon/params"
)

func TestTxLookup(t *testing.T) {
	require := require.New(t)

	m := mock.Mock(t)
	db := m.DB
	tx, err := db.BeginRw(m.Ctx)
	require.NoError(err)
	defer tx.Rollback()
	br := m.BlockReader

	var testKey, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	testAddr := crypto.PubkeyToAddress(testKey.PublicKey)

	mustSign := func(tx types.Transaction, s types.Signer) types.Transaction {
		r, err := types.SignTx(tx, s, testKey)
		require.NoError(err)
		return r
	}

	// prepare txn so it works with our test
	signer1 := types.MakeSigner(params.TestChainConfig, params.TestChainConfig.BerlinBlock.Uint64(), 0)
	header := &types.Header{Number: libcommon.Big1}
	hash := header.Hash()
	require.NoError(rawdb.WriteHeader(tx, header))
	bodies := [4]*types.Body{}
	bodies[1] = &types.Body{
		Transactions: []types.Transaction{
			mustSign(&types.AccessListTx{
				LegacyTx: types.LegacyTx{
					CommonTx: types.CommonTx{
						Nonce: 1,
						To:    &testAddr,
						Value: u256.Num1,
						Gas:   1,
					},
					GasPrice: u256.Num1,
				},
			}, *signer1),
			mustSign(&types.AccessListTx{
				LegacyTx: types.LegacyTx{
					CommonTx: types.CommonTx{
						Nonce: 2,
						To:    &testAddr,
						Value: u256.Num1,
						Gas:   2,
					},
					GasPrice: u256.Num1,
				},
			}, *signer1),
		},
	}
	require.NoError(rawdb.WriteBody(tx, hash, 1, bodies[1]))
	require.NoError(rawdb.WriteCanonicalHash(tx, hash, 1))

	signer2 := types.MakeSigner(params.TestChainConfig, params.TestChainConfig.BerlinBlock.Uint64(), 0)
	header.Number = libcommon.Big2
	hash = header.Hash()
	require.NoError(rawdb.WriteHeader(tx, header))
	bodies[2] = &types.Body{
		Transactions: []types.Transaction{
			mustSign(&types.AccessListTx{
				LegacyTx: types.LegacyTx{
					CommonTx: types.CommonTx{
						Nonce: 3,
						To:    &testAddr,
						Value: u256.Num1,
						Gas:   3,
					},
					GasPrice: u256.Num1,
				},
			}, *signer2),
			mustSign(&types.AccessListTx{
				LegacyTx: types.LegacyTx{
					CommonTx: types.CommonTx{
						Nonce: 4,
						To:    &testAddr,
						Value: u256.Num1,
						Gas:   4,
					},
					GasPrice: u256.Num1,
				},
			}, *signer2),
			mustSign(&types.AccessListTx{
				LegacyTx: types.LegacyTx{
					CommonTx: types.CommonTx{
						Nonce: 5,
						To:    &testAddr,
						Value: u256.Num1,
						Gas:   5,
					},
					GasPrice: u256.Num1,
				},
			}, *signer2),
		},
	}
	require.NoError(rawdb.WriteBody(tx, hash, 2, bodies[2]))
	require.NoError(rawdb.WriteCanonicalHash(tx, hash, 2))

	header.Number = libcommon.Big3
	hash = header.Hash()
	require.NoError(rawdb.WriteHeader(tx, header))
	err = rawdb.WriteBody(tx, hash, 3, &types.Body{
		Transactions: []types.Transaction{}, Uncles: []*types.Header{{GasLimit: 3}},
	})
	require.NoError(err)

	require.NoError(rawdb.WriteCanonicalHash(tx, hash, 3))
	require.NoError(stages.SaveStageProgress(tx, stages.Bodies, 3))

	require.NoError(stages.SaveStageProgress(tx, stages.Execution, 3))

	pm := prune.Mode{ // prune nothing
		Initialised: true,
		History:     prune.Distance(10_000),
	}
	cfg := stagedsync.StageTxLookupCfg(db, pm, "", nil, br)
	err = stagedsync.SpawnTxLookup(&stagedsync.StageState{ID: stages.TxLookup}, tx, 3, cfg, m.Ctx, log.New())
	require.NoError(err)

	{
		bn, _ := rawdb.ReadTxLookupEntry(tx, bodies[1].Transactions[0].Hash())
		require.Equal(1, int(*bn))

		bn, _ = rawdb.ReadTxLookupEntry(tx, bodies[2].Transactions[0].Hash())
		require.Equal(2, int(*bn))
	}

	pm = prune.Mode{ // prune more
		Initialised: true,
		History:     prune.Distance(1),
	}
	cfg = stagedsync.StageTxLookupCfg(db, pm, "", nil, br)
	err = stagedsync.PruneTxLookup(&stagedsync.PruneState{ID: stages.TxLookup, ForwardProgress: 3}, tx, cfg, m.Ctx, log.New())
	require.NoError(err)

	{
		bn, _ := rawdb.ReadTxLookupEntry(tx, bodies[1].Transactions[0].Hash())
		require.Nil(bn)

		bn, _ = rawdb.ReadTxLookupEntry(tx, bodies[2].Transactions[0].Hash())
		require.Equal(2, int(*bn))
	}
}
