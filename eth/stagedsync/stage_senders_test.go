package stagedsync

import (
	"math/big"
	"testing"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/u256"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSenders(t *testing.T) {
	db, require := ethdb.NewMemDatabase(), require.New(t)
	var testKey, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	testAddr := crypto.PubkeyToAddress(testKey.PublicKey)

	mustSign := func(tx *types.Transaction, s types.Signer) *types.Transaction {
		r, err := types.SignTx(tx, s, testKey)
		require.NoError(err)
		return r
	}

	// prepare db so it works with our test
	signer1 := types.MakeSigner(params.MainnetChainConfig, big.NewInt(int64(1)))
	require.NoError(rawdb.WriteBody(db, common.HexToHash("01"), 1, &types.Body{
		Transactions: []*types.Transaction{
			mustSign(types.NewTransaction(1, testAddr, u256.Num1, 1, u256.Num1, nil), signer1),
			mustSign(types.NewTransaction(2, testAddr, u256.Num1, 2, u256.Num1, nil), signer1),
		},
	}))
	require.NoError(rawdb.WriteCanonicalHash(db, common.HexToHash("01"), 1))

	signer2 := types.MakeSigner(params.MainnetChainConfig, big.NewInt(int64(1)))
	require.NoError(rawdb.WriteBody(db, common.HexToHash("02"), 2, &types.Body{
		Transactions: []*types.Transaction{
			mustSign(types.NewTransaction(3, testAddr, u256.Num1, 3, u256.Num1, nil), signer2),
			mustSign(types.NewTransaction(4, testAddr, u256.Num1, 4, u256.Num1, nil), signer2),
			mustSign(types.NewTransaction(5, testAddr, u256.Num1, 5, u256.Num1, nil), signer2),
		},
	}))
	require.NoError(rawdb.WriteCanonicalHash(db, common.HexToHash("02"), 2))

	require.NoError(rawdb.WriteBody(db, common.HexToHash("03"), 3, &types.Body{
		Transactions: []*types.Transaction{}, Uncles: []*types.Header{{GasLimit: 3}},
	}))
	require.NoError(rawdb.WriteCanonicalHash(db, common.HexToHash("03"), 3))

	require.NoError(stages.SaveStageProgress(db, stages.Bodies, 3))

	cfg := Stage3Config{
		BatchSize:       1024,
		BlockSize:       1024,
		BufferSize:      (1024 * 10 / 20) * 10000, // 20*4096
		NumOfGoroutines: 2,
		ReadChLen:       4,
		Now:             time.Now(),
	}
	err := SpawnRecoverSendersStage(cfg, &StageState{Stage: stages.Senders}, db, params.MainnetChainConfig, 3, "", nil)
	assert.NoError(t, err)

	{
		found := rawdb.ReadBody(db, common.HexToHash("01"), 1)
		assert.NotNil(t, found)
		assert.Equal(t, 2, len(found.Transactions))
		found = rawdb.ReadBody(db, common.HexToHash("02"), 2)
		assert.NotNil(t, found)
		assert.NotNil(t, 3, len(found.Transactions))
		found = rawdb.ReadBody(db, common.HexToHash("03"), 3)
		assert.NotNil(t, found)
		assert.NotNil(t, 0, len(found.Transactions))
		assert.NotNil(t, 2, len(found.Uncles))
	}

	{
		senders, err := rawdb.ReadSenders(db, common.HexToHash("01"), 1)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(senders))
		senders, err = rawdb.ReadSenders(db, common.HexToHash("02"), 2)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(senders))
		senders, err = rawdb.ReadSenders(db, common.HexToHash("03"), 3)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(senders))
	}
	{
		txs, err := rawdb.ReadTransactions(db, 0, 2)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(txs))
		txs, err = rawdb.ReadTransactions(db, 2, 3)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(txs))
		txs, err = rawdb.ReadTransactions(db, 0, 1024)
		assert.NoError(t, err)
		assert.Equal(t, 5, len(txs))
	}

}
