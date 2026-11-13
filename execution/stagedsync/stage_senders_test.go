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
	"crypto/ecdsa"
	"slices"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
)

func TestSenders(t *testing.T) {
	require := require.New(t)

	m := execmoduletester.New(t)
	db := m.DB
	tx, err := db.BeginRw(m.Ctx)
	require.NoError(err)
	defer tx.Rollback()
	br := m.BlockReader

	testKey, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	testAddr := crypto.PubkeyToAddress(testKey.PublicKey)

	mustSign := func(tx types.Transaction, s types.Signer) types.Transaction {
		r, err := types.SignTx(tx, s, testKey)
		require.NoError(err)
		return r
	}

	// prepare txn so it works with our test
	signer1 := types.MakeSigner(chain.TestChainBerlinConfig, *chain.TestChainBerlinConfig.BerlinBlock, 0)
	header := &types.Header{Number: *common.Num1}
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

	signer2 := types.MakeSigner(chain.TestChainBerlinConfig, *chain.TestChainBerlinConfig.BerlinBlock, 0)
	header.Number = *common.Num2
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

	header.Number = *common.Num3
	hash = header.Hash()
	require.NoError(rawdb.WriteHeader(tx, header))
	err = rawdb.WriteBody(tx, hash, 3, &types.Body{
		Transactions: []types.Transaction{}, Uncles: []*types.Header{{GasLimit: 3}},
	})
	require.NoError(err)

	require.NoError(rawdb.WriteCanonicalHash(tx, hash, 3))

	require.NoError(stages.SaveStageProgress(tx, stages.Bodies, 3))

	cfg := stagedsync.StageSendersCfg(chain.TestChainBerlinConfig, false, "", br, exec.NewBlockReadAheader())
	err = stagedsync.SpawnRecoverSendersStage(cfg, &stagedsync.StageState{ID: stages.Senders}, nil, tx, 3, m.Ctx, log.New())
	require.NoError(err)

	{
		header.Number = *common.Num1
		hash = header.Hash()
		found, senders, _ := br.BlockWithSenders(m.Ctx, tx, hash, 1)
		assert.NotNil(t, found)
		assert.Len(t, found.Body().Transactions, 2)
		assert.Len(t, senders, 2)
		header.Number = *common.Num2
		hash = header.Hash()
		found, senders, _ = br.BlockWithSenders(m.Ctx, tx, hash, 2)
		assert.NotNil(t, found)
		assert.NotNil(t, 3, len(found.Body().Transactions))
		assert.Len(t, senders, 3)
		header.Number = *common.Num3
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

func TestSendersRegisteredRecovery(t *testing.T) {
	require := require.New(t)

	m := execmoduletester.New(t)
	tx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(err)
	defer tx.Rollback()

	bodyKey, err := crypto.GenerateKey()
	require.NoError(err)
	recoveredKey, err := crypto.GenerateKey()
	require.NoError(err)
	signer := types.MakeSigner(chain.TestChainBerlinConfig, 1, 0)
	signed := func(key *ecdsa.PrivateKey) []types.Transaction {
		txn, err := types.SignTx(&types.LegacyTx{
			CommonTx: types.CommonTx{GasLimit: 21_000, Value: u256.Num1},
			GasPrice: u256.Num1,
		}, *signer, key)
		require.NoError(err)
		return []types.Transaction{txn}
	}

	header := &types.Header{Number: *common.Num1}
	hash := header.Hash()
	require.NoError(rawdb.WriteHeader(tx, header))
	require.NoError(rawdb.WriteBody(tx, hash, 1, &types.Body{Transactions: signed(bodyKey)}))
	require.NoError(rawdb.WriteCanonicalHash(tx, hash, 1))
	require.NoError(stages.SaveStageProgress(tx, stages.Bodies, 1))

	readAheader := exec.NewBlockReadAheader()
	readAheader.AddSendersRecovery(hash, exec.StartSendersRecovery(m.Ctx, signer, signed(recoveredKey)))

	cfg := stagedsync.StageSendersCfg(chain.TestChainBerlinConfig, false, "", m.BlockReader, readAheader)
	require.NoError(stagedsync.SpawnRecoverSendersStage(cfg, &stagedsync.StageState{ID: stages.Senders}, nil, tx, 1, m.Ctx, log.New()))

	_, senders, err := m.BlockReader.BlockWithSenders(m.Ctx, tx, hash, 1)
	require.NoError(err)
	require.Equal([]common.Address{crypto.PubkeyToAddress(recoveredKey.PublicKey)}, senders)
}

func writeSignedBlock(t *testing.T, tx kv.RwTx, header *types.Header, key *ecdsa.PrivateKey) (common.Hash, *types.Body) {
	t.Helper()
	signer := types.MakeSigner(chain.TestChainBerlinConfig, header.Number.Uint64(), header.Time)
	txn, err := types.SignTx(&types.LegacyTx{
		CommonTx: types.CommonTx{GasLimit: 21_000, Value: u256.Num1},
		GasPrice: u256.Num1,
	}, *signer, key)
	require.NoError(t, err)
	hash := header.Hash()
	body := &types.Body{Transactions: []types.Transaction{txn}}
	require.NoError(t, rawdb.WriteHeader(tx, header))
	require.NoError(t, rawdb.WriteBody(tx, hash, header.Number.Uint64(), body))
	require.NoError(t, rawdb.WriteCanonicalHash(tx, hash, header.Number.Uint64()))
	require.NoError(t, stages.SaveStageProgress(tx, stages.Bodies, header.Number.Uint64()))
	return hash, body
}

func TestSendersAttachesSendersToCachedBody(t *testing.T) {
	m := execmoduletester.New(t)
	tx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	header := &types.Header{Number: *common.Num1}
	hash, body := writeSignedBlock(t, tx, header, key)
	readAheader := exec.NewBlockReadAheader()
	readAheader.AddHeaderAndBody(m.Ctx, nil, nil, header, body)
	readAheader.AddSendersRecovery(hash, exec.StartSendersRecovery(m.Ctx, types.MakeSigner(chain.TestChainBerlinConfig, 1, 0), body.Transactions))

	cfg := stagedsync.StageSendersCfg(chain.TestChainBerlinConfig, false, t.TempDir(), m.BlockReader, readAheader)
	require.NoError(t, stagedsync.SpawnRecoverSendersStage(cfg, &stagedsync.StageState{ID: stages.Senders}, nil, tx, 1, m.Ctx, log.New()))

	cached, ok := readAheader.ReadBodyWithTransactions(hash)
	require.True(t, ok)
	sender, ok := cached.Transactions[0].GetSender()
	require.True(t, ok, "the cached body is what execution reads, so the stage must leave the senders on its txs")
	require.Equal(t, crypto.PubkeyToAddress(key.PublicKey), sender.Value())
}

func TestSendersWritesBALBlockSenders(t *testing.T) {
	m := execmoduletester.New(t)
	tx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	balHash := common.Hash{0x01}
	var zero uint64
	header := &types.Header{
		Number:                *common.Num1,
		BaseFee:               uint256.NewInt(1),
		WithdrawalsHash:       &common.Hash{},
		BlobGasUsed:           &zero,
		ExcessBlobGas:         &zero,
		ParentBeaconBlockRoot: &common.Hash{},
		RequestsHash:          &common.Hash{},
		BlockAccessListHash:   &balHash,
	}
	hash, body := writeSignedBlock(t, tx, header, key)
	stored, err := m.BlockReader.Header(m.Ctx, tx, hash, 1)
	require.NoError(t, err)
	require.True(t, stored.HasNonEmptyBAL(), "the stage reads the header back from the DB")
	readAheader := exec.NewBlockReadAheader()
	readAheader.AddHeaderAndBody(m.Ctx, nil, nil, header, body)
	readAheader.AddBlockAccessList(hash, types.NewBlockAccessListSidecar(types.BlockAccessList{{Address: common.Address{0x02}}}))
	readAheader.AddSendersRecovery(hash, exec.StartSendersRecovery(m.Ctx, types.MakeSigner(chain.TestChainBerlinConfig, 1, 0), body.Transactions))

	cfg := stagedsync.StageSendersCfg(chain.TestChainBerlinConfig, false, t.TempDir(), m.BlockReader, readAheader)
	require.NoError(t, stagedsync.SpawnRecoverSendersStage(cfg, &stagedsync.StageState{ID: stages.Senders}, nil, tx, 1, m.Ctx, log.New()))

	_, senders, err := m.BlockReader.BlockWithSenders(m.Ctx, tx, hash, 1)
	require.NoError(t, err)
	require.Equal(t, []common.Address{crypto.PubkeyToAddress(key.PublicKey)}, senders)
}

func TestSendersRecoversWhenRegisteredRecoveryFails(t *testing.T) {
	m := execmoduletester.New(t)
	tx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	header := &types.Header{Number: *common.Num1}
	hash, _ := writeSignedBlock(t, tx, header, key)
	unsigned := types.Transactions{&types.LegacyTx{CommonTx: types.CommonTx{GasLimit: 21_000}, GasPrice: u256.Num1}}
	readAheader := exec.NewBlockReadAheader()
	readAheader.AddSendersRecovery(hash, exec.StartSendersRecovery(m.Ctx, types.MakeSigner(chain.TestChainBerlinConfig, 1, 0), unsigned))

	cfg := stagedsync.StageSendersCfg(chain.TestChainBerlinConfig, false, t.TempDir(), m.BlockReader, readAheader)
	require.NoError(t, stagedsync.SpawnRecoverSendersStage(cfg, &stagedsync.StageState{ID: stages.Senders}, nil, tx, 1, m.Ctx, log.New()))

	_, senders, err := m.BlockReader.BlockWithSenders(m.Ctx, tx, hash, 1)
	require.NoError(t, err)
	require.Equal(t, []common.Address{crypto.PubkeyToAddress(key.PublicKey)}, senders)
}

type recordingUnwinder struct {
	point  uint64
	reason stagedsync.UnwindReason
	calls  int
}

func (u *recordingUnwinder) UnwindTo(point uint64, reason stagedsync.UnwindReason, _ kv.Tx) error {
	u.point, u.reason = point, reason
	u.calls++
	return nil
}
func (u *recordingUnwinder) HasUnwindPoint() bool { return u.calls > 0 }
func (u *recordingUnwinder) LogPrefix() string    { return "" }

func TestSendersUnwindsToTheBlockBeforeABadSignature(t *testing.T) {
	m := execmoduletester.New(t)
	tx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	signer := types.MakeSigner(chain.TestChainBerlinConfig, 1, 0)
	var hashes []common.Hash
	for n := uint64(1); n <= 5; n++ {
		txns := make([]types.Transaction, 3)
		for i := range txns {
			txns[i], err = types.SignTx(&types.LegacyTx{
				CommonTx: types.CommonTx{Nonce: n*10 + uint64(i), GasLimit: 21_000, Value: u256.Num1},
				GasPrice: u256.Num1,
			}, *signer, key)
			require.NoError(t, err)
		}
		if n == 4 {
			txns[0] = &types.LegacyTx{CommonTx: types.CommonTx{Nonce: 40, GasLimit: 21_000}, GasPrice: u256.Num1}
		}
		header := &types.Header{Number: *uint256.NewInt(n)}
		hash := header.Hash()
		require.NoError(t, rawdb.WriteHeader(tx, header))
		require.NoError(t, rawdb.WriteBody(tx, hash, n, &types.Body{Transactions: txns}))
		require.NoError(t, rawdb.WriteCanonicalHash(tx, hash, n))
		hashes = append(hashes, hash)
	}
	require.NoError(t, stages.SaveStageProgress(tx, stages.Bodies, 5))

	u := &recordingUnwinder{}
	cfg := stagedsync.StageSendersCfg(chain.TestChainBerlinConfig, false, t.TempDir(), m.BlockReader, exec.NewBlockReadAheader())
	require.NoError(t, stagedsync.SpawnRecoverSendersStage(cfg, &stagedsync.StageState{ID: stages.Senders}, u, tx, 5, m.Ctx, log.New()))

	require.Equal(t, 1, u.calls)
	require.Equal(t, uint64(3), u.point)
	require.NotNil(t, u.reason.Block)
	require.Equal(t, hashes[3], *u.reason.Block)
}

func TestSendersBatchUsesEachBlocksSigner(t *testing.T) {
	m := execmoduletester.New(t)
	tx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	config := &chain.Config{ChainID: uint256.NewInt(1337), HomesteadBlock: common.NewUint64(2), SpuriousDragonBlock: common.NewUint64(2)}
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	to := common.Address{0x1}

	frontier, err := types.SignTx(&types.LegacyTx{CommonTx: types.CommonTx{To: &to, GasLimit: 21_000}, GasPrice: u256.Num1}, *types.MakeSigner(config, 1, 0), key)
	require.NoError(t, err)
	malleable := frontier.(*types.LegacyTx)
	curveOrder := uint256.MustFromHex("0xfffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141")
	malleable.S.Sub(curveOrder, &malleable.S)
	malleable.V.SetUint64(55 - malleable.V.Uint64())
	protected, err := types.SignTx(&types.LegacyTx{CommonTx: types.CommonTx{Nonce: 1, To: &to, GasLimit: 21_000}, GasPrice: u256.Num1}, *types.MakeSigner(config, 2, 0), key)
	require.NoError(t, err)

	var hashes []common.Hash
	for n, txn := range []types.Transaction{malleable, protected} {
		header := &types.Header{Number: *uint256.NewInt(uint64(n + 1))}
		hash := header.Hash()
		require.NoError(t, rawdb.WriteHeader(tx, header))
		require.NoError(t, rawdb.WriteBody(tx, hash, uint64(n+1), &types.Body{Transactions: []types.Transaction{txn}}))
		require.NoError(t, rawdb.WriteCanonicalHash(tx, hash, uint64(n+1)))
		hashes = append(hashes, hash)
	}
	require.NoError(t, stages.SaveStageProgress(tx, stages.Bodies, 2))

	u := &recordingUnwinder{}
	cfg := stagedsync.StageSendersCfg(config, false, t.TempDir(), m.BlockReader, exec.NewBlockReadAheader())
	require.NoError(t, stagedsync.SpawnRecoverSendersStage(cfg, &stagedsync.StageState{ID: stages.Senders}, u, tx, 2, m.Ctx, log.New()))

	require.Zero(t, u.calls, "a Frontier-only and a Spurious-Dragon-only signature are both valid in their own blocks")
	for i, hash := range hashes {
		_, senders, err := m.BlockReader.BlockWithSenders(m.Ctx, tx, hash, uint64(i+1))
		require.NoError(t, err)
		require.Equal(t, []common.Address{sender}, senders)
	}
}

func TestSendersMixesRegisteredAndRecoveredBlocks(t *testing.T) {
	require := require.New(t)

	m := execmoduletester.New(t)
	tx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(err)
	defer tx.Rollback()

	key, err := crypto.GenerateKey()
	require.NoError(err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	signer := types.MakeSigner(chain.TestChainBerlinConfig, 1, 0)
	signedTxns := func(firstNonce uint64, n int) []types.Transaction {
		txns := make([]types.Transaction, n)
		for i := range txns {
			txns[i], err = types.SignTx(&types.LegacyTx{
				CommonTx: types.CommonTx{Nonce: firstNonce + uint64(i), GasLimit: 21_000, Value: u256.Num1},
				GasPrice: u256.Num1,
			}, *signer, key)
			require.NoError(err)
		}
		return txns
	}
	writeBlock := func(number *uint256.Int, txns []types.Transaction) common.Hash {
		header := &types.Header{Number: *number}
		hash := header.Hash()
		require.NoError(rawdb.WriteHeader(tx, header))
		require.NoError(rawdb.WriteBody(tx, hash, number.Uint64(), &types.Body{Transactions: txns}))
		require.NoError(rawdb.WriteCanonicalHash(tx, hash, number.Uint64()))
		return hash
	}

	recovered := signedTxns(0, 1)
	registered := signedTxns(1, 2_000)
	hashes := []common.Hash{writeBlock(common.Num1, recovered), writeBlock(common.Num2, registered)}
	require.NoError(stages.SaveStageProgress(tx, stages.Bodies, 2))

	readAheader := exec.NewBlockReadAheader()
	readAheader.AddSendersRecovery(hashes[1], exec.StartSendersRecovery(m.Ctx, signer, registered))

	cfg := stagedsync.StageSendersCfg(chain.TestChainBerlinConfig, false, "", m.BlockReader, readAheader)
	require.NoError(stagedsync.SpawnRecoverSendersStage(cfg, &stagedsync.StageState{ID: stages.Senders}, nil, tx, 2, m.Ctx, log.New()))

	for i, want := range []int{len(recovered), len(registered)} {
		_, senders, err := m.BlockReader.BlockWithSenders(m.Ctx, tx, hashes[i], uint64(i+1))
		require.NoError(err)
		require.Len(senders, want)
		for _, got := range senders {
			require.Equal(sender, got)
		}
	}
}

func TestSendersRecoveryErrorDoesNotDeadlock(t *testing.T) {
	m := execmoduletester.New(t)
	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	to := crypto.PubkeyToAddress(key.PublicKey)
	signer := types.MakeSigner(chain.TestChainBerlinConfig, 1, 0)
	const total = 2500
	txs := make([]types.Transaction, total)
	for i := range txs {
		tx, err := types.SignTx(&types.LegacyTx{CommonTx: types.CommonTx{Nonce: uint64(i), To: &to, Value: u256.Num1, GasLimit: 21000}, GasPrice: u256.Num1}, *signer, key)
		require.NoError(t, err)
		txs[i] = tx
	}
	bad := &types.LegacyTx{CommonTx: types.CommonTx{Nonce: 1 << 40, To: &to, Value: u256.Num1, GasLimit: 21000}, GasPrice: u256.Num1}
	bad.V.SetUint64(27)
	bad.R.Set(uint256.NewInt(0))
	bad.S.Set(uint256.NewInt(1))
	for it := range 60 {
		body := slices.Clone(txs)
		body[total-1000-30+it] = bad
		recoverSendersOfBlock(t, m, body, byte(it))
	}
}

func recoverSendersOfBlock(t *testing.T, m *execmoduletester.ExecModuleTester, body []types.Transaction, salt byte) {
	tx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	header := &types.Header{Number: *common.Num1, Extra: []byte{salt}}
	hash := header.Hash()
	require.NoError(t, rawdb.WriteHeader(tx, header))
	require.NoError(t, rawdb.WriteBody(tx, hash, 1, &types.Body{Transactions: body}))
	require.NoError(t, rawdb.WriteCanonicalHash(tx, hash, 1))
	require.NoError(t, stages.SaveStageProgress(tx, stages.Bodies, 1))
	cfg := stagedsync.StageSendersCfg(chain.TestChainBerlinConfig, true, t.TempDir(), m.BlockReader, exec.NewBlockReadAheader())
	done := make(chan error, 1)
	go func() {
		done <- stagedsync.SpawnRecoverSendersStage(cfg, &stagedsync.StageState{ID: stages.Senders}, nil, tx, 1, m.Ctx, log.New())
	}()
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("senders stage hung after a sender recovery error")
	}
}
