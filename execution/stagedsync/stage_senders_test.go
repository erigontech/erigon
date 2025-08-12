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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/u256"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/stages/mock"
	"github.com/erigontech/erigon/execution/types"
)

func TestSenders(t *testing.T) {
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
	signer1 := types.MakeSigner(chain.TestChainConfig, chain.TestChainConfig.BerlinBlock.Uint64(), 0)
	header := &types.Header{Number: common.Big1}
	hash := header.Hash()
	require.NoError(rawdb.WriteHeader(tx, header))
	require.NoError(rawdb.WriteBody(tx, hash, 1, &types.Body{
		Transactions: []types.Transaction{
			mustSign(&types.AccessListTx{
				LegacyTx: types.LegacyTx{
					CommonTx: types.CommonTx{
						Nonce:    1,
						To:       &testAddr,
						Value:    u256.Num1,
						GasLimit: 1,
					},
					GasPrice: u256.Num1,
				},
			}, *signer1),
			mustSign(&types.AccessListTx{
				LegacyTx: types.LegacyTx{
					CommonTx: types.CommonTx{
						Nonce:    2,
						To:       &testAddr,
						Value:    u256.Num1,
						GasLimit: 2,
					},
					GasPrice: u256.Num1,
				},
			}, *signer1),
		},
	}))
	require.NoError(rawdb.WriteCanonicalHash(tx, hash, 1))

	signer2 := types.MakeSigner(chain.TestChainConfig, chain.TestChainConfig.BerlinBlock.Uint64(), 0)
	header.Number = common.Big2
	hash = header.Hash()
	require.NoError(rawdb.WriteHeader(tx, header))
	require.NoError(rawdb.WriteBody(tx, hash, 2, &types.Body{
		Transactions: []types.Transaction{
			mustSign(&types.AccessListTx{
				LegacyTx: types.LegacyTx{
					CommonTx: types.CommonTx{
						Nonce:    3,
						To:       &testAddr,
						Value:    u256.Num1,
						GasLimit: 3,
					},
					GasPrice: u256.Num1,
				},
			}, *signer2),
			mustSign(&types.AccessListTx{
				LegacyTx: types.LegacyTx{
					CommonTx: types.CommonTx{
						Nonce:    4,
						To:       &testAddr,
						Value:    u256.Num1,
						GasLimit: 4,
					},
					GasPrice: u256.Num1,
				},
			}, *signer2),
			mustSign(&types.AccessListTx{
				LegacyTx: types.LegacyTx{
					CommonTx: types.CommonTx{
						Nonce:    5,
						To:       &testAddr,
						Value:    u256.Num1,
						GasLimit: 5,
					},
					GasPrice: u256.Num1,
				},
			}, *signer2),
		},
	}))

	require.NoError(rawdb.WriteCanonicalHash(tx, hash, 2))

	header.Number = common.Big3
	hash = header.Hash()
	require.NoError(rawdb.WriteHeader(tx, header))
	err = rawdb.WriteBody(tx, hash, 3, &types.Body{
		Transactions: []types.Transaction{}, Uncles: []*types.Header{{GasLimit: 3}},
	})
	require.NoError(err)

	require.NoError(rawdb.WriteCanonicalHash(tx, hash, 3))

	require.NoError(stages.SaveStageProgress(tx, stages.Bodies, 3))

	cfg := stagedsync.StageSendersCfg(db, chain.TestChainConfig, ethconfig.Defaults.Sync, false, "", prune.Mode{}, br, nil)
	err = stagedsync.SpawnRecoverSendersStage(cfg, &stagedsync.StageState{ID: stages.Senders}, nil, tx, 3, m.Ctx, log.New())
	require.NoError(err)

	{
		header.Number = common.Big1
		hash = header.Hash()
		found, senders, _ := br.BlockWithSenders(m.Ctx, tx, hash, 1)
		assert.NotNil(t, found)
		assert.Len(t, found.Body().Transactions, 2)
		assert.Len(t, senders, 2)
		header.Number = common.Big2
		hash = header.Hash()
		found, senders, _ = br.BlockWithSenders(m.Ctx, tx, hash, 2)
		assert.NotNil(t, found)
		assert.NotNil(t, 3, len(found.Body().Transactions))
		assert.Len(t, senders, 3)
		header.Number = common.Big3
		hash = header.Hash()
		found, senders, _ = br.BlockWithSenders(m.Ctx, tx, hash, 3)
		assert.NotNil(t, found)
		assert.NotNil(t, 0, len(found.Body().Transactions))
		assert.NotNil(t, 2, len(found.Body().Uncles))
		assert.Empty(t, senders)
	}

	{
		cnt, _ := tx.Count(kv.EthTx)
		assert.Equal(t, 5, int(cnt))

		txs, err := rawdb.CanonicalTransactions(tx, 1, 2)
		require.NoError(err)
		assert.Len(t, txs, 2)
		txs, err = rawdb.CanonicalTransactions(tx, 5, 3)
		require.NoError(err)
		assert.Len(t, txs, 3)
		txs, err = rawdb.CanonicalTransactions(tx, 5, 1024)
		require.NoError(err)
		assert.Len(t, txs, 3)
	}
}
