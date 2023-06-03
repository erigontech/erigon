package stagedsync_test

import (
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	stages2 "github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/log/v3"
)

func TestSenders(t *testing.T) {
	logger := log.New()
	require := require.New(t)

	m := stages2.Mock(t)
	db := m.DB
	tx, err := db.BeginRw(m.Ctx)
	require.NoError(err)
	defer tx.Rollback()
	br, bw := m.NewBlocksIO()

	var testKey, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	testAddr := crypto.PubkeyToAddress(testKey.PublicKey)

	mustSign := func(tx types.Transaction, s types.Signer) types.Transaction {
		r, err := types.SignTx(tx, s, testKey)
		require.NoError(err)
		return r
	}

	// prepare tx so it works with our test
	signer1 := types.MakeSigner(params.TestChainConfig, params.TestChainConfig.BerlinBlock.Uint64())
	header := &types.Header{Number: libcommon.Big1}
	hash := header.Hash()
	require.NoError(bw.WriteHeader(tx, header))
	require.NoError(bw.WriteBody(tx, hash, 1, &types.Body{
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
	}))
	require.NoError(rawdb.WriteCanonicalHash(tx, hash, 1))

	signer2 := types.MakeSigner(params.TestChainConfig, params.TestChainConfig.BerlinBlock.Uint64())
	header.Number = libcommon.Big2
	hash = header.Hash()
	require.NoError(bw.WriteHeader(tx, header))
	require.NoError(bw.WriteBody(tx, hash, 2, &types.Body{
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
	}))

	require.NoError(rawdb.WriteCanonicalHash(tx, hash, 2))

	header.Number = libcommon.Big3
	hash = header.Hash()
	require.NoError(bw.WriteHeader(tx, header))
	err = bw.WriteBody(tx, hash, 3, &types.Body{
		Transactions: []types.Transaction{}, Uncles: []*types.Header{{GasLimit: 3}},
	})
	require.NoError(err)

	require.NoError(rawdb.WriteCanonicalHash(tx, hash, 3))

	require.NoError(stages.SaveStageProgress(tx, stages.Bodies, 3))

	blockRetire := snapshotsync.NewBlockRetire(1, "", br, bw, db, nil, nil, logger)
	cfg := stagedsync.StageSendersCfg(db, params.TestChainConfig, false, "", prune.Mode{}, blockRetire, bw, br, nil)
	err = stagedsync.SpawnRecoverSendersStage(cfg, &stagedsync.StageState{ID: stages.Senders}, nil, tx, 3, m.Ctx, log.New())
	require.NoError(err)

	{
		header.Number = libcommon.Big1
		hash = header.Hash()
		found, senders, _ := br.BlockWithSenders(m.Ctx, tx, hash, 1)
		assert.NotNil(t, found)
		assert.Equal(t, 2, len(found.Body().Transactions))
		assert.Equal(t, 2, len(senders))
		header.Number = libcommon.Big2
		hash = header.Hash()
		found, senders, _ = br.BlockWithSenders(m.Ctx, tx, hash, 2)
		assert.NotNil(t, found)
		assert.NotNil(t, 3, len(found.Body().Transactions))
		assert.Equal(t, 3, len(senders))
		header.Number = libcommon.Big3
		hash = header.Hash()
		found, senders, _ = br.BlockWithSenders(m.Ctx, tx, hash, 3)
		assert.NotNil(t, found)
		assert.NotNil(t, 0, len(found.Body().Transactions))
		assert.NotNil(t, 2, len(found.Body().Uncles))
		assert.Equal(t, 0, len(senders))
	}

	{
		if br.TxsV3Enabled() {
			c, _ := tx.Cursor(kv.EthTx)
			cnt, _ := c.Count()
			assert.Equal(t, 5, int(cnt))
		} else {
			txs, err := rawdb.CanonicalTransactions(tx, 1, 2)
			assert.NoError(t, err)
			assert.Equal(t, 2, len(txs))
			txs, err = rawdb.CanonicalTransactions(tx, 5, 3)
			assert.NoError(t, err)
			assert.Equal(t, 3, len(txs))
			txs, err = rawdb.CanonicalTransactions(tx, 5, 1024)
			assert.NoError(t, err)
			assert.Equal(t, 3, len(txs))
		}

	}
}
