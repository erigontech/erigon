// Copyright 2025 The Erigon Authors
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

package txpool

import (
	"bytes"
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/kv/remotedbserver"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	accounts3 "github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

type addMockTxPool struct {
	knownByCall      []bool
	knownTxnCalls    int
	addReasons       []txpoolcfg.DiscardReason
	addLocalSlotsLen int
	addLocalCalls    int
}

func (m *addMockTxPool) PeekBest(ctx context.Context, n int, txns *TxnsRlp, onTopOf uint64) (bool, error) {
	return false, nil
}
func (m *addMockTxPool) GetRlp(tx kv.Tx, hash []byte) ([]byte, error) { return nil, nil }
func (m *addMockTxPool) AddLocalTxns(ctx context.Context, newTxns TxnSlots) ([]txpoolcfg.DiscardReason, error) {
	m.addLocalCalls++
	m.addLocalSlotsLen = len(newTxns.Txns)
	return m.addReasons, nil
}

func (m *addMockTxPool) deprecatedForEach(f func(rlp []byte, sender common.Address, t SubPoolType), tx kv.Tx) {
}
func (m *addMockTxPool) CountContent() (int, int, int) { return 0, 0, 0 }
func (m *addMockTxPool) RejectFramePrefix(common.Hash) {}
func (m *addMockTxPool) KnownTxnReason(hash []byte) txpoolcfg.DiscardReason {
	i := m.knownTxnCalls
	m.knownTxnCalls++
	if i < len(m.knownByCall) && m.knownByCall[i] {
		return txpoolcfg.AlreadyKnown
	}
	return txpoolcfg.NotSet
}

func (m *addMockTxPool) NonceFromAddress(addr [20]byte) (nonce uint64, inPool bool) { return 0, false }

func (m *addMockTxPool) GetBlobs(blobhashes []common.Hash) (blobBundles []PoolBlobBundle) { return nil }

func TestMapDiscardReasonToProto(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		reason txpoolcfg.DiscardReason
		want   txpoolproto.ImportResult
	}{
		{txpoolcfg.IntrinsicGas, txpoolproto.ImportResult_INVALID},
		{txpoolcfg.GasLimitTooHigh, txpoolproto.ImportResult_INVALID},
		{txpoolcfg.NonceTooDistant, txpoolproto.ImportResult_INVALID},
		{txpoolcfg.DelegatedNonceGap, txpoolproto.ImportResult_INVALID},
		{txpoolcfg.DelegatedTxnLimit, txpoolproto.ImportResult_INVALID},
		{txpoolcfg.NotReplaced, txpoolproto.ImportResult_FEE_TOO_LOW},
		{txpoolcfg.InvalidFrame, txpoolproto.ImportResult_INVALID},
		{txpoolcfg.FrameValidationUnavailable, txpoolproto.ImportResult_INTERNAL_ERROR},
		{txpoolcfg.FrameStateChanged, txpoolproto.ImportResult_INTERNAL_ERROR},
	} {
		t.Run(tc.reason.String(), func(t *testing.T) {
			require.Equal(t, tc.want, mapDiscardReasonToProto(tc.reason))
		})
	}
}

func TestGrpcServerAddDoesNotAdmitEmptySlots(t *testing.T) {
	for _, tc := range []struct {
		name  string
		rlps  [][]byte
		known bool
		want  []txpoolproto.ImportResult
	}{
		{"empty", nil, false, []txpoolproto.ImportResult{}},
		{"invalid", [][]byte{{0xff}}, false, []txpoolproto.ImportResult{txpoolproto.ImportResult_INVALID}},
		{"known", [][]byte{hexutil.MustDecodeHex(TxnParseMainnetTests[0].PayloadStr)}, true, []txpoolproto.ImportResult{txpoolproto.ImportResult_ALREADY_EXISTS}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := &addMockTxPool{knownByCall: []bool{tc.known}}
			server := NewGrpcServer(t.Context(), pool, nil, nil, uint256.Int{1}, log.New())
			reply, err := server.Add(t.Context(), &txpoolproto.AddRequest{RlpTxs: tc.rlps})
			require.NoError(t, err)
			require.Equal(t, tc.want, reply.Imported)
			require.Zero(t, pool.addLocalCalls, "rejected submissions must not wait for pool recovery")
		})
	}
}

func TestGrpcServerAddDiscardReasonIndexAlignment(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	chainID := *uint256.NewInt(1)

	mockPool := &addMockTxPool{
		knownByCall: []bool{true, false}, // first tx treated as already-known, second goes to AddLocalTxns
		addReasons:  []txpoolcfg.DiscardReason{txpoolcfg.TipAboveFeeCap},
	}

	s := NewGrpcServer(ctx, mockPool, mdbxtest.NewTestPoolDB(t), nil, chainID, log.New())
	validRlp := hexutil.MustDecodeHex(TxnParseMainnetTests[0].PayloadStr)

	reply, err := s.Add(ctx, &txpoolproto.AddRequest{RlpTxs: [][]byte{validRlp, validRlp}})
	if err != nil {
		t.Fatalf("Add returned error: %v", err)
	}

	if got := mockPool.addLocalSlotsLen; got != 1 {
		t.Fatalf("expected 1 slot sent to AddLocalTxns, got %d", got)
	}

	if len(reply.Imported) != 2 || len(reply.Errors) != 2 {
		t.Fatalf("unexpected reply lengths: imported=%d errors=%d", len(reply.Imported), len(reply.Errors))
	}

	if reply.Imported[0] != txpoolproto.ImportResult_ALREADY_EXISTS || reply.Errors[0] != txpoolcfg.AlreadyKnown.String() {
		t.Fatalf("unexpected first tx result: imported=%v error=%q", reply.Imported[0], reply.Errors[0])
	}
	if reply.Imported[1] != txpoolproto.ImportResult_INVALID || reply.Errors[1] != "max priority fee per gas higher than max fee per gas" {
		t.Fatalf("unexpected second tx result: imported=%v error=%q", reply.Imported[1], reply.Errors[1])
	}
}

func TestGrpcServerAddDuplicateDuringAdmission(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	mockPool := &addMockTxPool{
		knownByCall: []bool{false},
		addReasons:  []txpoolcfg.DiscardReason{txpoolcfg.DuplicateHash},
	}
	server := NewGrpcServer(ctx, mockPool, mdbxtest.NewTestPoolDB(t), nil, uint256.Int{1}, log.New())
	validRlp := hexutil.MustDecodeHex(TxnParseMainnetTests[0].PayloadStr)

	reply, err := server.Add(ctx, &txpoolproto.AddRequest{RlpTxs: [][]byte{validRlp}})
	require.NoError(t, err)
	require.Equal(t, 1, mockPool.addLocalSlotsLen)
	require.Equal(t, &txpoolproto.AddReply{
		Imported: []txpoolproto.ImportResult{txpoolproto.ImportResult_ALREADY_EXISTS},
		Errors:   []string{"already known"},
	}, reply)
}

func TestGrpcServerAddUnavailablePool(t *testing.T) {
	for _, starting := range []bool{false, true} {
		name := "recovering"
		if starting {
			name = "starting"
		}
		t.Run(name, func(t *testing.T) {
			_, pool, poolDB, _ := newTestPool(t)
			if starting {
				pool.started.Store(false)
			} else {
				pool.MarkStateStale()
			}
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				server := NewGrpcServer(ctx, pool, poolDB, nil, uint256.Int{1}, pool.logger)
				start := time.Now()
				reply, err := server.Add(ctx, &txpoolproto.AddRequest{
					RlpTxs: [][]byte{hexutil.MustDecodeHex(TxnParseMainnetTests[0].PayloadStr)},
				})
				require.Equal(t, codes.Unavailable, status.Code(err))
				require.ErrorContains(t, err, "retry later")
				require.Zero(t, time.Since(start), "unavailable admission must not wait for recovery")
				require.Nil(t, reply)
				require.Empty(t, pool.byHash)
			})
		})
	}
}

func TestGrpcServerAddNonceTooLow(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	pool := &addMockTxPool{addReasons: []txpoolcfg.DiscardReason{txpoolcfg.NonceTooLow}}
	server := NewGrpcServer(ctx, pool, mdbxtest.NewTestPoolDB(t), nil, uint256.Int{1}, log.New())
	reply, err := server.Add(ctx, &txpoolproto.AddRequest{
		RlpTxs: [][]byte{hexutil.MustDecodeHex(TxnParseMainnetTests[0].PayloadStr)},
	})
	require.NoError(t, err)
	require.Equal(t, &txpoolproto.AddReply{
		Imported: []txpoolproto.ImportResult{txpoolproto.ImportResult_STALE},
		Errors:   []string{txpoolcfg.NonceTooLow.String()},
	}, reply)
}

func TestGrpcServerAddBlobNonceGap(t *testing.T) {
	ctx, pool, poolDB, _ := newTestPool(t)
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	require.NoError(t, pool.OnNewBlock(ctx, newBlockChangeForAccounts(common.Hash{}, 0, sender), TxnSlots{}, TxnSlots{}, TxnSlots{}))
	blob := types.MakeV1WrappedBlobTxn(&pool.chainID)
	blob.Tx.Nonce = 1
	signer := *types.LatestSignerForChainID(&pool.chainID)
	signed, err := types.SignTx(blob, signer, key)
	require.NoError(t, err)
	var encoded bytes.Buffer
	require.NoError(t, signed.(*types.BlobTxWrapper).MarshalBinaryWrapped(&encoded))
	server := NewGrpcServer(ctx, pool, poolDB, nil, pool.chainID, pool.logger)
	reply, err := server.Add(ctx, &txpoolproto.AddRequest{RlpTxs: [][]byte{encoded.Bytes()}})
	require.NoError(t, err)
	require.Equal(t, &txpoolproto.AddReply{
		Imported: []txpoolproto.ImportResult{txpoolproto.ImportResult_INVALID},
		Errors:   []string{"nonce-gapped blob transaction"},
	}, reply)
	hash := signed.Hash()
	require.NotEqual(t, txpoolcfg.AlreadyKnown, pool.KnownTxnReason(hash[:]))
	require.NoError(t, poolDB.View(ctx, func(tx kv.Tx) error {
		known, knownErr := pool.IdHashKnown(tx, hash[:])
		require.NoError(t, knownErr)
		require.False(t, known, "a blob transaction must be refetchable after its nonce gap closes")
		return nil
	}))

	predecessor := newTestTxnSlot(0, 0, 1, 2, 21_000).Txn.(*types.DynamicFeeTransaction)
	predecessor.ChainID = pool.chainID
	signedPredecessor, err := types.SignTx(predecessor, signer, key)
	require.NoError(t, err)
	var predecessorRLP bytes.Buffer
	require.NoError(t, signedPredecessor.MarshalBinary(&predecessorRLP))
	reply, err = server.Add(ctx, &txpoolproto.AddRequest{RlpTxs: [][]byte{predecessorRLP.Bytes()}})
	require.NoError(t, err)
	require.Equal(t, []txpoolproto.ImportResult{txpoolproto.ImportResult_SUCCESS}, reply.Imported)

	reply, err = server.Add(ctx, &txpoolproto.AddRequest{RlpTxs: [][]byte{encoded.Bytes()}})
	require.NoError(t, err)
	require.Equal(t, []txpoolproto.ImportResult{txpoolproto.ImportResult_SUCCESS}, reply.Imported)
	require.Equal(t, txpoolcfg.AlreadyKnown, pool.KnownTxnReason(hash[:]))
}

func TestGrpcServerAddInvalidSelfAuthorizationNonce(t *testing.T) {
	for _, tc := range []struct {
		name  string
		nonce uint64
	}{
		{"below required nonce", 0},
		{"above required nonce", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, pool, poolDB, _ := newTestPool(t)
			key, err := crypto.GenerateKey()
			require.NoError(t, err)
			sender := crypto.PubkeyToAddress(key.PublicKey)
			require.NoError(t, pool.OnNewBlock(ctx, newBlockChangeForAccounts(common.Hash{}, 0, sender), TxnSlots{}, TxnSlots{}, TxnSlots{}))
			auth, err := types.SignAuthorization(key, pool.chainID, common.Address{2}, tc.nonce)
			require.NoError(t, err)
			txn := newTestSetCodeTxnSlot(0, 0, 1, 2, 100_000).Txn.(*types.SetCodeTransaction)
			txn.ChainID = pool.chainID
			txn.Authorizations = []types.Authorization{auth}
			signed, err := types.SignTx(txn, *types.LatestSignerForChainID(&pool.chainID), key)
			require.NoError(t, err)
			var encoded bytes.Buffer
			require.NoError(t, signed.MarshalBinary(&encoded))
			server := NewGrpcServer(ctx, pool, poolDB, nil, pool.chainID, pool.logger)
			hash := signed.Hash()
			for range 2 {
				reply, err := server.Add(ctx, &txpoolproto.AddRequest{RlpTxs: [][]byte{encoded.Bytes()}})
				require.NoError(t, err)
				require.Equal(t, &txpoolproto.AddReply{
					Imported: []txpoolproto.ImportResult{txpoolproto.ImportResult_INVALID},
					Errors:   []string{"self-authorization nonce must equal transaction nonce plus one"},
				}, reply)
				require.Equal(t, txpoolcfg.InvalidAuthorizationNonce, pool.KnownTxnReason(hash[:]), "repeated RPC submissions must reuse the rejection before signature recovery")
			}
			require.NoError(t, poolDB.View(ctx, func(tx kv.Tx) error {
				known, knownErr := pool.IdHashKnown(tx, hash[:])
				require.NoError(t, knownErr)
				require.True(t, known, "this self-authorization nonce mismatch is independent of state")
				return nil
			}))
		})
	}
}

type concurrentAddTxPool struct {
	*TxPool
	admissions atomic.Int32
	bothParsed chan struct{}
}

func (p *concurrentAddTxPool) AddLocalTxns(ctx context.Context, txns TxnSlots) ([]txpoolcfg.DiscardReason, error) {
	if p.admissions.Add(1) == 2 {
		close(p.bothParsed)
	}
	return p.TxPool.AddLocalTxns(ctx, txns)
}

func TestGrpcServerAddFrameFailureAfterConcurrentAdmission(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"invalid", nil},
		{"unavailable", status.Error(codes.Unavailable, "backend unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, pool, sender := keyedFramePoolEnv(t)
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			pool.chainID.SetUint64(1)
			backend := pool.ethBackend.(*frameValidationBackend)
			wrapped := &concurrentAddTxPool{TxPool: pool, bothParsed: make(chan struct{})}
			entered, admitted := make(chan struct{}), make(chan struct{})
			var calls atomic.Int32
			pool.ethBackend = &frameValidationFuncBackend{validate: func(ctx context.Context, req *remoteproto.FrameValidationRequest) (*remoteproto.FrameValidationReply, error) {
				if calls.Add(1) == 1 {
					close(entered)
					// Both requests must pass the initial membership check before either can admit the transaction.
					select {
					case <-wrapped.bothParsed:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
					return backend.FrameValidation(ctx, req)
				}
				select {
				case <-admitted:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				return &remoteproto.FrameValidationReply{Valid: false}, tc.err
			}}
			frame := framePoolSlot(t, sender, 1, 2, uint256.Int{1})
			server := NewGrpcServer(ctx, wrapped, pool.poolDB, nil, pool.chainID, pool.logger)
			request := &txpoolproto.AddRequest{RlpTxs: [][]byte{frame.Rlp}}
			type outcome struct {
				reply *txpoolproto.AddReply
				err   error
			}
			first := make(chan outcome, 1)
			go func() {
				reply, err := server.Add(ctx, request)
				first <- outcome{reply, err}
				close(admitted)
			}()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			reply, err := server.Add(ctx, request)
			accepted := <-first
			require.NoError(t, accepted.err)
			require.Equal(t, []txpoolproto.ImportResult{txpoolproto.ImportResult_SUCCESS}, accepted.reply.Imported)
			require.NoError(t, err)
			require.Equal(t, txpoolcfg.AlreadyKnown, pool.KnownTxnReason(frame.IDHash[:]))
			require.Equal(t, &txpoolproto.AddReply{
				Imported: []txpoolproto.ImportResult{txpoolproto.ImportResult_ALREADY_EXISTS},
				Errors:   []string{txpoolcfg.AlreadyKnown.String()},
			}, reply)
		})
	}
}

func TestGrpcServerAddRetriesNonceGapAfterStateUpdate(t *testing.T) {
	for _, persisted := range []bool{false, true} {
		name := "memory only"
		if persisted {
			name = "pool DB row awaiting cleanup"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			logger := log.New()
			coreDB := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
			poolDB := mdbxtest.NewTestPoolDB(t)
			cfg := txpoolcfg.DefaultConfig
			cfg.MaxNonceGap = 64
			pool, err := New(ctx, make(chan Announcements, 1), poolDB, coreDB, cfg, kvcache.NewLatestBatchCache(),
				chain.AllProtocolChanges, nil, nil, func() {}, nil, nil, logger, WithFeeCalculator(nil))
			require.NoError(t, err)
			pool.started.Store(true)
			key, err := crypto.GenerateKey()
			require.NoError(t, err)
			sender := crypto.PubkeyToAddress(key.PublicKey)
			txn := newTestTxnSlot(141, 0, 300_000, 300_000, 100_000).Txn.(*types.DynamicFeeTransaction)
			txn.ChainID = pool.chainID
			signed, err := types.SignTx(txn, *types.LatestSignerForChainID(&pool.chainID), key)
			require.NoError(t, err)
			hash := signed.Hash()
			var encoded bytes.Buffer
			require.NoError(t, signed.MarshalBinary(&encoded))
			server := NewGrpcServer(ctx, pool, poolDB, nil, pool.chainID, logger)
			request := &txpoolproto.AddRequest{RlpTxs: [][]byte{encoded.Bytes()}}
			account := accounts3.Account{Nonce: 61, Balance: *uint256.NewInt(common.Ether), CodeHash: accounts3.EmptyCodeHash}
			change := &remoteproto.StateChangeBatch{
				PendingBlockBaseFee: 200_000, BlockGasLimit: 1_000_000,
				ChangeBatch: []*remoteproto.StateChange{{
					BlockHeight: 62, BlockHash: gointerfaces.ConvertHashToH256(common.Hash{}),
					Changes: []*remoteproto.AccountChange{{
						Action: remoteproto.Action_UPSERT, Address: gointerfaces.ConvertAddressToH160(sender), Data: accounts3.SerialiseV3(&account),
					}},
				}},
			}
			require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))

			first, err := server.Add(ctx, request)
			require.NoError(t, err)
			require.Equal(t, []txpoolproto.ImportResult{txpoolproto.ImportResult_INVALID}, first.Imported)
			require.Equal(t, []string{txpoolcfg.NonceTooDistant.String()}, first.Errors)
			require.Empty(t, pool.byHash)
			if persisted {
				staleSender := sender
				staleSender[0] ^= 0xff
				require.NoError(t, poolDB.Update(ctx, func(tx kv.RwTx) error {
					return tx.Put(kv.PoolTransaction, hash[:], append(staleSender[:], encoded.Bytes()...))
				}))
			}
			account.Nonce = 141
			change.ChangeBatch[0].BlockHeight = 142
			change.ChangeBatch[0].Changes[0].Data = accounts3.SerialiseV3(&account)
			require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))

			retry, err := server.Add(ctx, request)
			require.NoError(t, err)
			require.Equal(t, []txpoolproto.ImportResult{txpoolproto.ImportResult_SUCCESS}, retry.Imported)
			pending, baseFee, queued := pool.CountContent()
			require.Equal(t, 1, pending)
			require.Zero(t, baseFee+queued)
			duplicate, err := server.Add(ctx, request)
			require.NoError(t, err)
			require.Equal(t, []txpoolproto.ImportResult{txpoolproto.ImportResult_ALREADY_EXISTS}, duplicate.Imported)
			if persisted {
				_, err = pool.flush(ctx)
				require.NoError(t, err)
				require.NoError(t, poolDB.View(ctx, func(tx kv.Tx) error {
					row, err := tx.GetOne(kv.PoolTransaction, hash[:])
					require.NoError(t, err)
					require.Equal(t, append(sender[:], encoded.Bytes()...), row)
					return nil
				}))
			}
		})
	}
}

func TestGrpcServerParkedFrame(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	frame := framePoolSlot(t, sender, 1, 2, uint256.Int{1})
	var txns TxnSlots
	txns.Append(frame, sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, txns)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)
	server := NewGrpcServer(ctx, pool, pool.poolDB, nil, pool.chainID, pool.logger)

	checkContent := func(wantType txpoolproto.AllReply_TxnType, pending, queued uint32) {
		t.Helper()
		reply, err := server.All(ctx, &txpoolproto.AllRequest{})
		require.NoError(t, err)
		require.Len(t, reply.Txs, 1)
		require.Equal(t, wantType, reply.Txs[0].TxnType)
		require.Equal(t, sender, common.Address(gointerfaces.ConvertH160toAddress(reply.Txs[0].Sender)))
		require.Equal(t, frame.Rlp, reply.Txs[0].RlpTx)
		status, err := server.Status(ctx, &txpoolproto.StatusRequest{})
		require.NoError(t, err)
		require.Equal(t, pending, status.PendingCount)
		require.Equal(t, queued, status.QueuedCount)
		require.Zero(t, status.BaseFeeCount)
	}
	checkMetrics := func(pending, queued uint32) {
		t.Helper()
		require.EqualValues(t, pending, pendingSubCounter.GetValue())
		require.EqualValues(t, queued, queuedSubCounter.GetValue())
		require.Zero(t, basefeeSubCounter.GetValue())
	}
	checkContent(txpoolproto.AllReply_PENDING, 1, 0)

	checkParked := func() {
		t.Helper()
		checkContent(txpoolproto.AllReply_QUEUED, 0, 1)
		checkMetrics(0, 1)
		require.Zero(t, pool.byHash[string(frame.IDHash[:])].currentSubPool)
		require.Zero(t, pool.queued.Len(), "RPC classification must not put parked frames in a scheduling sub-pool")
		var selected TxnsRlp
		_, err := pool.PeekBest(ctx, 10, &selected, 0)
		require.NoError(t, err)
		require.Empty(t, selected.Txns)
	}
	backend := pool.ethBackend.(*frameValidationBackend)
	backend.onValidate = checkParked
	backend.err = context.DeadlineExceeded
	change := newBlockChangeForAccounts(common.Hash{1}, 0, sender)
	change.ChangeBatch[0].BlockHeight = 1
	require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
	checkParked()

	backend.err = nil
	change = newBlockChangeForAccounts(common.Hash{2}, 0)
	change.ChangeBatch[0].BlockHeight = 2
	require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
	checkContent(txpoolproto.AllReply_PENDING, 1, 0)
	checkMetrics(1, 0)
}

func TestGrpcServerCachedFrameRejectionBeforeSignatures(t *testing.T) {
	for _, tc := range []struct {
		reason txpoolcfg.DiscardReason
		want   txpoolproto.ImportResult
	}{
		{txpoolcfg.InvalidFrame, txpoolproto.ImportResult_INVALID},
		{txpoolcfg.NonceTooLow, txpoolproto.ImportResult_STALE},
	} {
		t.Run(tc.reason.String(), func(t *testing.T) {
			ctx, pool, sender := keyedFramePoolEnv(t)
			pool.chainID.SetUint64(1)
			frame := framePoolSlot(t, sender, 1, 2).Txn.(*types.FrameTransaction)
			// Invalid signatures make a repeated signature check observable without instrumenting the verifier.
			frame.Signatures = []types.FrameSignature{{Scheme: types.FrameSignatureSecp256k1, Signer: sender[:], Signature: make([]byte, 65)}}
			var encoded bytes.Buffer
			require.NoError(t, frame.MarshalBinary(&encoded))
			hash := crypto.Keccak256Hash(encoded.Bytes())
			pool.frameRejections.Add(string(hash[:]), frameRejection{generation: pool.stateGeneration, reason: tc.reason})
			server := NewGrpcServer(ctx, pool, pool.poolDB, nil, pool.chainID, pool.logger)
			reply, err := server.Add(ctx, &txpoolproto.AddRequest{RlpTxs: [][]byte{encoded.Bytes()}})
			require.NoError(t, err)
			require.Equal(t, []string{tc.reason.String()}, reply.Errors)
			require.Equal(t, []txpoolproto.ImportResult{tc.want}, reply.Imported)
			require.Empty(t, pool.ethBackend.(*frameValidationBackend).calls)
			require.NotEqual(t, txpoolcfg.AlreadyKnown, pool.KnownTxnReason(hash[:]))
		})
	}
}

func TestGrpcServerRejectedFrameResubmission(t *testing.T) {
	for _, persisted := range []bool{false, true} {
		ctx, pool, sender := keyedFramePoolEnv(t)
		pool.chainID.SetUint64(1)
		backend := pool.ethBackend.(*frameValidationBackend)
		backend.valid = false
		frame := framePoolSlot(t, sender, 1, 2, uint256.Int{1})
		server := NewGrpcServer(ctx, pool, pool.poolDB, nil, pool.chainID, pool.logger)
		request := &txpoolproto.AddRequest{RlpTxs: [][]byte{frame.Rlp}}
		first, err := server.Add(ctx, request)
		require.NoError(t, err)
		require.Equal(t, []txpoolproto.ImportResult{txpoolproto.ImportResult_INVALID}, first.Imported)
		if persisted {
			require.NoError(t, pool.poolDB.Update(ctx, func(tx kv.RwTx) error {
				return tx.Put(kv.PoolTransaction, frame.IDHash[:], append(sender[:], frame.Rlp...))
			}))
		}
		second, err := server.Add(ctx, request)
		require.NoError(t, err)
		require.Equal(t, first, second)
		require.Len(t, backend.calls, 1)
		require.NotContains(t, pool.byHash, string(frame.IDHash[:]))
		require.NoError(t, pool.poolDB.View(ctx, func(tx kv.Tx) error {
			known, readErr := pool.IdHashKnown(tx, frame.IDHash[:])
			require.NoError(t, readErr)
			require.True(t, known, "gossip must still deduplicate rejected transactions")
			return nil
		}))
		backend.valid = true
		require.NoError(t, pool.OnNewBlock(ctx, newBlockChangeForAccounts(common.Hash{1}, 0, sender), TxnSlots{}, TxnSlots{}, TxnSlots{}))
		third, err := server.Add(ctx, request)
		require.NoError(t, err)
		require.Equal(t, []txpoolproto.ImportResult{txpoolproto.ImportResult_SUCCESS}, third.Imported)
		require.Len(t, backend.calls, 2)
		duplicate, err := server.Add(ctx, request)
		require.NoError(t, err)
		require.Equal(t, []txpoolproto.ImportResult{txpoolproto.ImportResult_ALREADY_EXISTS}, duplicate.Imported)
	}
}

func TestGrpcServerFramePrefixRejectionKnownToGossip(t *testing.T) {
	ctx, pool, sender := keyedFramePoolEnv(t)
	pool.chainID.SetUint64(1)
	frame := framePoolSlot(t, sender, 1, 2).Txn.(*types.FrameTransaction)
	frame.Frames[0].Limits.Execution = 150_000
	var encoded bytes.Buffer
	require.NoError(t, frame.MarshalBinary(&encoded))
	hash := crypto.Keccak256Hash(encoded.Bytes())
	server := NewGrpcServer(ctx, pool, pool.poolDB, nil, pool.chainID, pool.logger)
	request := &txpoolproto.AddRequest{RlpTxs: [][]byte{encoded.Bytes()}}
	reply, err := server.Add(ctx, request)
	require.NoError(t, err)
	require.Equal(t, []txpoolproto.ImportResult{txpoolproto.ImportResult_INVALID}, reply.Imported)
	require.NoError(t, pool.poolDB.View(ctx, func(tx kv.Tx) error {
		known, readErr := pool.IdHashKnown(tx, hash[:])
		require.True(t, known, "gossip must not fetch a prefix already rejected by RPC")
		return readErr
	}))
	retry, err := server.Add(ctx, request)
	require.NoError(t, err)
	require.Equal(t, reply, retry)
	require.NotEqual(t, txpoolcfg.AlreadyKnown, pool.KnownTxnReason(hash[:]))
	require.Empty(t, pool.ethBackend.(*frameValidationBackend).calls)
}

// TestQueryAllWithoutPanicUnknown tries to reproduce https://github.com/erigontech/erigon/issues/18076 relying on
// the TOCTOU between the deprecatedForEach locking window and the conversion of currentSubPool in GrpcServer.All().
// It runs 3 concurrent loops: one repeatedly calling GrpcServer.All(), the others repeatedly triggering public
// operations that reset currentSubPool to zero (mined removal and replacement), aiming to hit the race window.
// If the panic("unknown") is triggered in the observation window, the test fails.
func TestQueryAllWithoutPanicUnknown(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	t.Parallel()

	const ObservationWindow = 10 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), ObservationWindow)
	defer cancel()

	// Prepare tx pool and core+pool DBs
	newTxns := make(chan Announcements, 1)
	chainDB := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	poolDB := mdbxtest.NewTestPoolDB(t)
	cfg := txpoolcfg.DefaultConfig
	cache := kvcache.New(kvcache.DefaultCoherentConfig)
	pool, err := New(ctx, newTxns, poolDB, chainDB, cfg, cache, chain.AllProtocolChanges, nil, nil, func() {}, nil, nil, log.New())
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}

	pool.started.Store(true)

	// Seed minimal chain state so the pool accepts local txns
	var stateVersionID uint64 = 0
	pendingBaseFee := uint64(200000)
	h256 := gointerfaces.ConvertHashToH256(common.Hash{})
	change := &remoteproto.StateChangeBatch{
		StateVersionId:      stateVersionID,
		PendingBlockBaseFee: pendingBaseFee,
		BlockGasLimit:       1_000_000,
		ChangeBatch:         []*remoteproto.StateChange{{BlockHeight: 0, BlockHash: h256}},
	}
	var addr common.Address
	addr[0] = 0xAB
	acc := accounts3.Account{Nonce: 0, Balance: *uint256.NewInt(10 * common.Ether)}
	accBlob := accounts3.SerialiseV3(&acc)
	change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remoteproto.AccountChange{
		Action:  remoteproto.Action_UPSERT,
		Address: gointerfaces.ConvertAddressToH160(addr),
		Data:    accBlob,
	})

	// Apply state change
	if err = pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}); err != nil {
		t.Fatalf("OnNewBlock: %v", err)
	}

	// Prepare two alternating local transactions with the same nonce to exercise replacement
	mkSlot := func(id byte, tip uint64) *TxnSlot {
		to := common.Address{1}
		s := &TxnSlot{
			Txn: &types.DynamicFeeTransaction{
				CommonTx: types.CommonTx{GasLimit: 21000, To: &to},
				TipCap:   *uint256.NewInt(tip),
				FeeCap:   *uint256.NewInt(tip),
			},
			Nonce: 0,
			Rlp:   []byte{id}, // ensure All() doesn't need DB to fetch
		}
		s.IDHash[0] = id
		return s
	}
	slotA := mkSlot(0xA1, 300000)
	slotB := mkSlot(0xB2, 400000) // higher to ensure replacement

	// Add initial txn (A)
	var slots TxnSlots
	slots.Append(slotA, addr[:], true)
	discards, err := pool.AddLocalTxns(ctx, slots)
	if err != nil {
		t.Fatalf("AddLocalTxns(A): %v", err)
	}
	if len(discards) != 1 || discards[0] != txpoolcfg.Success {
		t.Fatalf("unexpected add result A: %+v", discards)
	}

	// Build gRPC server for TxPool
	chainID := *uint256.NewInt(1)
	s := NewGrpcServer(ctx, pool, poolDB, nil, chainID, log.New())

	var panicObserved atomic.Bool
	panicCh := make(chan struct{}, 1)

	var allTasks sync.WaitGroup

	// Reader task: repeatedly call GrpcServer.All() and catch the panic("unknown")
	allTasks.Go(func() {
		for !panicObserved.Load() {
			func() {
				defer func() {
					if r := recover(); r != nil {
						if r == "unknown" {
							panicObserved.Store(true)
							select {
							case panicCh <- struct{}{}:
							default:
							}
						}
					}
				}()
				_, _ = s.All(ctx, &txpoolproto.AllRequest{})
			}()

			// Either exit if the observation window is done or sleep a bit
			select {
			case <-ctx.Done():
				return
			default:
				time.Sleep(50 * time.Microsecond)
			}
		}
	})

	// Mutator task: alternate between replacement and mined-removal cycles
	allTasks.Go(func() {
		for !panicObserved.Load() {
			// Replacement path: add B to replace A (or vice versa)
			var r TxnSlots
			r.Append(slotB, addr[:], true)
			_, _ = pool.AddLocalTxns(ctx, r)

			// Now mined-removal path for whichever is present (use B here)
			var mined TxnSlots
			mined.Append(slotB, addr[:], true)
			_ = pool.OnNewBlock(ctx, &remoteproto.StateChangeBatch{ // keep the same base fee
				StateVersionId:      stateVersionID,
				PendingBlockBaseFee: pendingBaseFee,
				BlockGasLimit:       1_000_000,
				ChangeBatch:         []*remoteproto.StateChange{{BlockHeight: 0, BlockHash: h256}},
			}, TxnSlots{}, TxnSlots{}, mined)

			// Re-add A again to keep cycling
			var r2 TxnSlots
			r2.Append(slotA, addr[:], true)
			_, _ = pool.AddLocalTxns(ctx, r2)

			// Either exit if the observation window is done or sleep a bit
			select {
			case <-ctx.Done():
				return
			default:
				time.Sleep(50 * time.Microsecond)
			}
		}
	})

	// BaseFee churn task: alternates base fee above/below thresholds to force demotions/promotions across sub-pools
	// while sender mapping remains.
	allTasks.Go(func() {
		flip := false
		for !panicObserved.Load() {
			var bf uint64
			if flip {
				bf = pendingBaseFee * 20 // very high to push below fee cap
			} else {
				bf = pendingBaseFee / 20 // very low to allow promotions
			}
			flip = !flip
			_ = pool.OnNewBlock(ctx, &remoteproto.StateChangeBatch{
				StateVersionId:      stateVersionID,
				PendingBlockBaseFee: bf,
				BlockGasLimit:       1_000_000,
				ChangeBatch:         []*remoteproto.StateChange{{BlockHeight: 0, BlockHash: h256}},
			}, TxnSlots{}, TxnSlots{}, TxnSlots{})

			// Either exit if the observation window is done or sleep a bit
			select {
			case <-ctx.Done():
				return
			default:
				time.Sleep(75 * time.Microsecond)
			}
		}
	})

	// Wait for all tasks to finish
	allTasks.Wait()

	select {
	case <-panicCh:
		t.Fatalf("panic(\"unknown\") triggered")
	case <-ctx.Done():
		// Success
	}
}

// stalledOnAddStream is an OnAdd subscriber that never finishes a Send, the way
// a connected client that stops reading exhausts its HTTP/2 flow control.
type stalledOnAddStream struct {
	grpc.ServerStream
	ctx      context.Context
	entered  chan struct{}
	enterOne sync.Once
}

func newStalledOnAddStream(ctx context.Context) *stalledOnAddStream {
	return &stalledOnAddStream{ctx: ctx, entered: make(chan struct{})}
}

func (s *stalledOnAddStream) Context() context.Context { return s.ctx }

func (s *stalledOnAddStream) Send(*txpoolproto.OnAddReply) error {
	s.enterOne.Do(func() { close(s.entered) })
	<-s.ctx.Done()
	return s.ctx.Err()
}

// awaitSubscribed broadcasts until wait returns, since a subscriber only
// registers once its handler runs on the server.
func awaitSubscribed(t *testing.T, streams *NewSlotsStreams, wait func()) {
	t.Helper()
	probing := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-probing:
				return
			case <-ticker.C:
				streams.Broadcast(&txpoolproto.OnAddReply{}, log.New())
			}
		}
	}()
	wait()
	close(probing)
	<-done
}

// TestP2PPropagationContinuesWhileOnAddSubscriberStalls drives the real
// newPendingTxns path in TxPool.Run and asserts that a gRPC OnAdd subscriber
// stuck in Send does not keep the batch from reaching the p2p sender.
func TestP2PPropagationContinuesWhileOnAddSubscriberStalls(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := log.New()
	ctrl := gomock.NewController(t)

	sentToPeers := make(chan *sentryproto.SendMessageToRandomPeersRequest, 16)
	sentryClient := sentryproto.NewMockSentryClient(ctrl)
	sentryClient.EXPECT().
		SendMessageToRandomPeers(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *sentryproto.SendMessageToRandomPeersRequest, _ ...grpc.CallOption) (*sentryproto.SentPeers, error) {
			select {
			case sentToPeers <- req:
			default:
			}
			return &sentryproto.SentPeers{}, nil
		}).AnyTimes()
	sentryClient.EXPECT().
		Peers(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&sentryproto.PeersReply{}, nil).AnyTimes()

	kvServer := remotedbserver.NewKvServer(ctx, nil, nil, nil, logger)
	stateChanges := direct.NewStateDiffClientDirect(kvServer)

	coreDB := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	poolDB := mdbxtest.NewTestPoolDB(t)
	newSlotsStreams := &NewSlotsStreams{}
	pool, err := New(
		ctx,
		make(chan Announcements, 16),
		poolDB,
		coreDB,
		txpoolcfg.DefaultConfig,
		kvcache.New(kvcache.DefaultCoherentConfig),
		chain.AllProtocolChanges,
		nil, // sentry clients for the fetcher: this test only drives the sender
		stateChanges,
		func() {},
		newSlotsStreams,
		nil,
		logger,
		WithFeeCalculator(nil),
	)
	require.NoError(t, err)
	pool.p2pSender = NewSend(ctx, []sentryproto.SentryClient{sentryClient}, logger)

	sender := common.Address{1}
	account := accounts3.Account{Balance: *uint256.NewInt(common.Ether)}
	change := &remoteproto.StateChangeBatch{
		PendingBlockBaseFee: 1,
		BlockGasLimit:       1_000_000,
		ChangeBatch: []*remoteproto.StateChange{{
			BlockHash: gointerfaces.ConvertHashToH256(common.Hash{}),
			Changes: []*remoteproto.AccountChange{{
				Action:  remoteproto.Action_UPSERT,
				Address: gointerfaces.ConvertAddressToH160(sender),
				Data:    accounts3.SerialiseV3(&account),
			}},
		}},
	}
	kvServer.SetStateChangesSnapshot(func(context.Context) (*remoteproto.StateChangeBatch, error) {
		return change, nil
	})

	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		pool.Run(ctx) //nolint:errcheck // returns the cancellation error on teardown
	}()

	grpcServer := NewGrpcServer(ctx, pool, poolDB, newSlotsStreams, *chain.AllProtocolChanges.ChainID, logger)
	stalled := newStalledOnAddStream(ctx)
	go grpcServer.OnAdd(&txpoolproto.OnAddRequest{}, stalled) //nolint:errcheck // ends with ctx
	awaitSubscribed(t, newSlotsStreams, func() { <-stalled.entered })
	require.Eventually(t, pool.Started, 5*time.Second, time.Millisecond)

	slot := newTestTxnSlot(0, 0, 300_000, 300_000, 100_000)
	slot.IDHash[0] = 1
	slot.Rlp = []byte{0xc0}
	slot.Size = uint32(len(slot.Rlp))
	var slots TxnSlots
	slots.Append(slot, sender[:], true)
	reasons, err := pool.AddLocalTxns(ctx, slots)
	require.NoError(t, err)
	require.Equal(t, []txpoolcfg.DiscardReason{txpoolcfg.Success}, reasons)

	select {
	case req := <-sentToPeers:
		require.Equal(t, sentryproto.MessageId_TRANSACTIONS_66, req.Data.Id)
	case <-time.After(20 * time.Second):
		t.Fatal("p2p propagation never reached the sentry while an OnAdd subscriber was stalled")
	}

	cancel()
	<-runDone
}

// TestOnAddSubscriberThatStopsReadingOverTCP exercises OnAdd over a real
// gRPC connection, where a client that stops calling Recv drains its HTTP/2
// flow-control window and wedges the server-side Send.
func TestOnAddSubscriberThatStopsReadingOverTCP(t *testing.T) {
	ctx := t.Context()
	logger := log.New()

	newSlotsStreams := &NewSlotsStreams{}
	grpcServer := NewGrpcServer(ctx, nil, nil, newSlotsStreams, *uint256.NewInt(1), logger)
	srv := grpc.NewServer()
	txpoolproto.RegisterTxpoolServer(srv, grpcServer)
	var lc net.ListenConfig
	listener, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go srv.Serve(listener) //nolint:errcheck // stopped below
	t.Cleanup(srv.Stop)

	// Separate connections, so that connection-level flow control on the
	// stalled client cannot account for anything the healthy one sees.
	subscribe := func() txpoolproto.Txpool_OnAddClient {
		conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		t.Cleanup(func() { conn.Close() })
		stream, err := txpoolproto.NewTxpoolClient(conn).OnAdd(ctx, &txpoolproto.OnAddRequest{})
		require.NoError(t, err)
		return stream
	}

	awaitRegistered := func(stream txpoolproto.Txpool_OnAddClient) {
		t.Helper()
		awaitSubscribed(t, newSlotsStreams, func() {
			_, err := stream.Recv()
			require.NoError(t, err)
		})
	}

	stalled := subscribe()
	awaitRegistered(stalled)
	healthy := subscribe()
	awaitRegistered(healthy)

	// stalled is never read again from here on. healthy keeps reading, so its
	// own queue stays empty and only the stalled one can hold the broadcaster up.
	received := make(chan *txpoolproto.OnAddReply, 1024)
	go func() {
		defer close(received)
		for {
			reply, err := healthy.Recv()
			if err != nil {
				return
			}
			received <- reply
		}
	}()

	const (
		messages    = 40
		payloadSize = 256 * 1024
	)
	marker := []byte("last")
	for i := range messages {
		payload := make([]byte, payloadSize)
		if i == messages-1 {
			copy(payload, marker)
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			newSlotsStreams.Broadcast(&txpoolproto.OnAddReply{RplTxs: [][]byte{payload}}, logger)
		}()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatalf("Broadcast %d blocked on the subscriber that stopped reading", i)
		}
	}

	timeout := time.After(30 * time.Second)
	for {
		select {
		case reply, ok := <-received:
			require.True(t, ok, "healthy subscriber's stream ended before the marker message")
			if len(reply.RplTxs) == 1 && bytes.Equal(reply.RplTxs[0][:len(marker)], marker) {
				return
			}
		case <-timeout:
			t.Fatal("healthy subscriber never received the marker message")
		}
	}
}
