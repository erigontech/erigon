// Copyright 2026 The Erigon Authors
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

package envelope_history

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/types"
)

type testEngine struct {
	header      *types.Header
	body        *engine_types.ExecutionPayloadBodyV2
	headerCalls int
	bodyCalls   int
	headerCtx   context.Context
	bodyCtx     context.Context
	headerErr   error
	headerStart chan struct{}
	headerWait  <-chan struct{}
}

func (e *testEngine) GetHeaderByHash(ctx context.Context, _ common.Hash) (*types.Header, error) {
	e.headerCalls++
	e.headerCtx = ctx
	if e.headerStart != nil {
		close(e.headerStart)
	}
	if e.headerWait != nil {
		select {
		case <-e.headerWait:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return e.header, e.headerErr
}

func (e *testEngine) GetPayloadBodiesByHash(ctx context.Context, _ []common.Hash) ([]*engine_types.ExecutionPayloadBodyV2, error) {
	e.bodyCalls++
	e.bodyCtx = ctx
	if e.body == nil {
		return nil, nil
	}
	return []*engine_types.ExecutionPayloadBodyV2{e.body}, nil
}

type rebuildFixture struct {
	reader    *tests.MockBlockReader
	db        kv.RwDB
	engine    *testEngine
	rebuilder *Rebuilder
	block     *cltypes.SignedBeaconBlock
	child     *cltypes.SignedBeaconBlock
	root      common.Hash
	want      *cltypes.SignedExecutionPayloadEnvelope
	bal       []byte
}

func TestRebuildRoundTrip(t *testing.T) {
	fixture := newRebuildFixture(t)
	rebuilt, err := fixture.rebuild(t, t.Context())
	require.NoError(t, err)

	wantSSZ, err := fixture.want.EncodeSSZ(nil)
	require.NoError(t, err)
	gotSSZ, err := rebuilt.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Equal(t, wantSSZ, gotSSZ)

	fixture.bal[0] = 99
	require.Equal(t, byte(12), rebuilt.Message.Payload.BlockAccessList.Bytes()[0])
}

func TestRebuildRejectsPayloadThatDoesNotMatchBid(t *testing.T) {
	fixture := newRebuildFixture(t)
	fixture.engine.body.Transactions = []hexutil.Bytes{{0x01}}

	rebuilt, err := fixture.rebuild(t, t.Context())
	require.NoError(t, err)
	require.Nil(t, rebuilt)
}

func TestRebuildSkipsUnavailableDatabaseStateBeforeCallingEL(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*testing.T, *rebuildFixture)
	}{
		{
			name: "missing signature",
			change: func(t *testing.T, fixture *rebuildFixture) {
				require.NoError(t, fixture.db.Update(t.Context(), func(tx kv.RwTx) error {
					return tx.Delete(kv.ExecutionPayloadEnvelopeSignatures, dbutils.BlockBodyKey(fixture.block.Block.Slot, fixture.root))
				}))
			},
		},
		{
			name: "malformed signature",
			change: func(t *testing.T, fixture *rebuildFixture) {
				require.NoError(t, fixture.db.Update(t.Context(), func(tx kv.RwTx) error {
					return tx.Put(kv.ExecutionPayloadEnvelopeSignatures, dbutils.BlockBodyKey(fixture.block.Block.Slot, fixture.root), []byte{1, 2, 3})
				}))
			},
		},
		{
			name: "no child",
			change: func(t *testing.T, fixture *rebuildFixture) {
				require.NoError(t, fixture.db.Update(t.Context(), func(tx kv.RwTx) error {
					return beacon_indicies.TruncateCanonicalChain(tx, fixture.child.Block.Slot)
				}))
			},
		},
		{
			name: "empty child",
			change: func(t *testing.T, fixture *rebuildFixture) {
				fixture.child.Block.Body.SignedExecutionPayloadBid.Message.ParentBlockHash = common.Hash{99}
				childRoot, err := fixture.child.Block.HashSSZ()
				require.NoError(t, err)
				require.NoError(t, fixture.db.Update(t.Context(), func(tx kv.RwTx) error {
					return beacon_indicies.MarkRootCanonical(tx, fixture.child.Block.Slot, childRoot)
				}))
			},
		},
		{
			name: "non-canonical root",
			change: func(t *testing.T, fixture *rebuildFixture) {
				require.NoError(t, fixture.db.Update(t.Context(), func(tx kv.RwTx) error {
					return beacon_indicies.MarkRootCanonical(tx, fixture.block.Block.Slot, common.Hash{99})
				}))
			},
		},
		{
			name: "pre-Gloas block",
			change: func(t *testing.T, fixture *rebuildFixture) {
				block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
				block.Block.Slot = fixture.block.Block.Slot
				fixture.block = block
				fixture.reader.U[block.Block.Slot] = block
				fixture.root, _ = block.Block.HashSSZ()
				require.NoError(t, fixture.db.Update(t.Context(), func(tx kv.RwTx) error {
					return beacon_indicies.MarkRootCanonical(tx, block.Block.Slot, fixture.root)
				}))
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRebuildFixture(t)
			test.change(t, fixture)
			rebuilt, err := fixture.rebuild(t, t.Context())
			require.NoError(t, err)
			require.Nil(t, rebuilt)
			require.Zero(t, fixture.engine.headerCalls)
			require.Zero(t, fixture.engine.bodyCalls)
		})
	}
}

func TestRebuildReturnsUnavailableForMissingELData(t *testing.T) {
	for _, test := range []struct {
		name   string
		bal    []byte
		change func(*rebuildFixture)
	}{
		{name: "missing header", change: func(fixture *rebuildFixture) { fixture.engine.header = nil }},
		{name: "missing body", change: func(fixture *rebuildFixture) { fixture.engine.body = nil }},
		{name: "nil block access list", change: func(fixture *rebuildFixture) { fixture.engine.body.BlockAccessList = nil }},
		{name: "empty block access list", bal: []byte{0xc0}, change: func(fixture *rebuildFixture) {
			empty := hexutil.Bytes{}
			fixture.engine.body.BlockAccessList = &empty
		}},
		{name: "nil base fee", change: func(fixture *rebuildFixture) { fixture.engine.header.BaseFee = nil }},
		{name: "nil withdrawal", change: func(fixture *rebuildFixture) { fixture.engine.body.Withdrawals = types.Withdrawals{nil} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			bal := []byte{12, 13, 14}
			if test.bal != nil {
				bal = test.bal
			}
			fixture := newRebuildFixtureWithBAL(t, bal)
			test.change(fixture)
			rebuilt, err := fixture.rebuild(t, t.Context())
			require.NoError(t, err)
			require.Nil(t, rebuilt)
		})
	}
}

func TestRebuildDoesNotCacheMissingBlockAccessList(t *testing.T) {
	fixture := newRebuildFixture(t)
	blockAccessList := fixture.engine.body.BlockAccessList
	fixture.engine.body.BlockAccessList = nil

	rebuilt, err := fixture.rebuild(t, t.Context())
	require.NoError(t, err)
	require.Nil(t, rebuilt)
	fixture.engine.body.BlockAccessList = blockAccessList
	rebuilt, err = fixture.rebuild(t, t.Context())
	require.NoError(t, err)
	require.NotNil(t, rebuilt)
	require.Equal(t, 2, fixture.engine.headerCalls)
}

func TestRebuildReturnsContextErrorWhenSlotIsUnavailable(t *testing.T) {
	fixture := newRebuildFixture(t)
	require.NoError(t, fixture.rebuilder.rebuildSlot.Acquire(t.Context(), 1))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	rebuilt, err := fixture.rebuild(t, ctx)
	require.Nil(t, rebuilt)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, fixture.engine.headerCalls)
}

func TestRebuildDoesNotAcquireSlotAfterContextEnds(t *testing.T) {
	fixture := newRebuildFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	for range 64 {
		rebuilt, err := fixture.rebuild(t, ctx)
		require.Nil(t, rebuilt)
		require.ErrorIs(t, err, context.Canceled)
	}
	require.Zero(t, fixture.engine.headerCalls)
}

func TestTryRebuildReturnsBusyWithoutCallingEL(t *testing.T) {
	fixture := newRebuildFixture(t)
	prepared := fixture.prepare(t)

	synctest.Test(t, func(t *testing.T) {
		rebuilder := NewRebuilder(fixture.rebuilder.beaconCfg, fixture.reader, fixture.engine)
		require.NoError(t, rebuilder.rebuildSlot.Acquire(t.Context(), 1))
		defer rebuilder.rebuildSlot.Release(1)
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		started := time.Now()

		rebuilt, err := rebuilder.TryRebuild(ctx, prepared)
		require.Nil(t, rebuilt)
		require.ErrorIs(t, err, ErrRebuildBusy)
		require.Equal(t, started, time.Now())
		require.Zero(t, fixture.engine.headerCalls)
		require.Zero(t, fixture.engine.bodyCalls)
	})
}

func TestTryRebuildReturnsCanceledBeforeBusy(t *testing.T) {
	fixture := newRebuildFixture(t)
	prepared := fixture.prepare(t)
	require.NoError(t, fixture.rebuilder.rebuildSlot.Acquire(t.Context(), 1))
	defer fixture.rebuilder.rebuildSlot.Release(1)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	rebuilt, err := fixture.rebuilder.TryRebuild(ctx, prepared)
	require.Nil(t, rebuilt)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, fixture.engine.headerCalls)
	require.Zero(t, fixture.engine.bodyCalls)
}

func TestRebuildWaitsForHeldSlot(t *testing.T) {
	fixture := newRebuildFixture(t)
	prepared := fixture.prepare(t)

	synctest.Test(t, func(t *testing.T) {
		rebuilder := NewRebuilder(fixture.rebuilder.beaconCfg, fixture.reader, fixture.engine)
		require.NoError(t, rebuilder.rebuildSlot.Acquire(t.Context(), 1))
		var rebuilt *cltypes.SignedExecutionPayloadEnvelope
		var rebuildErr error
		go func() {
			rebuilt, rebuildErr = rebuilder.Rebuild(t.Context(), prepared)
		}()

		synctest.Wait()
		require.Zero(t, fixture.engine.headerCalls)
		require.Zero(t, fixture.engine.bodyCalls)
		rebuilder.rebuildSlot.Release(1)
		synctest.Wait()
		require.NoError(t, rebuildErr)
		require.NotNil(t, rebuilt)
		require.Equal(t, 1, fixture.engine.headerCalls)
		require.Equal(t, 1, fixture.engine.bodyCalls)
	})
}

func TestRebuildWaiterHasPriorityOverTryRebuild(t *testing.T) {
	fixture := newRebuildFixture(t)
	prepared := fixture.prepare(t)

	synctest.Test(t, func(t *testing.T) {
		headerWait := make(chan struct{})
		engine := &testEngine{header: fixture.engine.header, body: fixture.engine.body, headerWait: headerWait}
		rebuilder := NewRebuilder(fixture.rebuilder.beaconCfg, fixture.reader, engine)
		require.NoError(t, rebuilder.rebuildSlot.Acquire(t.Context(), 1))
		var rebuilt *cltypes.SignedExecutionPayloadEnvelope
		var rebuildErr error
		go func() {
			rebuilt, rebuildErr = rebuilder.Rebuild(t.Context(), prepared)
		}()

		synctest.Wait()
		rebuilder.rebuildSlot.Release(1)
		tried, err := rebuilder.TryRebuild(t.Context(), prepared)
		require.Nil(t, tried)
		require.ErrorIs(t, err, ErrRebuildBusy)
		synctest.Wait()
		require.Equal(t, 1, engine.headerCalls)
		require.Zero(t, engine.bodyCalls)
		close(headerWait)
		synctest.Wait()
		require.NoError(t, rebuildErr)
		require.NotNil(t, rebuilt)
	})
}

func TestTryRebuildReleasesSlot(t *testing.T) {
	errELUnavailable := errors.New("EL unavailable")
	for _, test := range []struct {
		name    string
		run     func(*testing.T, *rebuildFixture, *PreparedRebuild) error
		wantErr error
	}{
		{
			name: "success",
			run: func(t *testing.T, fixture *rebuildFixture, prepared *PreparedRebuild) error {
				_, err := fixture.rebuilder.TryRebuild(t.Context(), prepared)
				return err
			},
		},
		{
			name:    "EL error",
			wantErr: errELUnavailable,
			run: func(t *testing.T, fixture *rebuildFixture, prepared *PreparedRebuild) error {
				fixture.engine.headerErr = errELUnavailable
				_, err := fixture.rebuilder.TryRebuild(t.Context(), prepared)
				return err
			},
		},
		{
			name:    "context canceled during EL call",
			wantErr: context.Canceled,
			run: func(t *testing.T, fixture *rebuildFixture, prepared *PreparedRebuild) error {
				headerStart := make(chan struct{})
				headerWait := make(chan struct{})
				engine := &testEngine{header: fixture.engine.header, body: fixture.engine.body, headerStart: headerStart, headerWait: headerWait}
				fixture.rebuilder = NewRebuilder(fixture.rebuilder.beaconCfg, fixture.reader, engine)
				ctx, cancel := context.WithCancel(t.Context())
				result := make(chan error, 1)
				go func() {
					_, err := fixture.rebuilder.TryRebuild(ctx, prepared)
					result <- err
				}()
				<-headerStart
				cancel()
				return <-result
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRebuildFixture(t)
			prepared := fixture.prepare(t)
			err := test.run(t, fixture, prepared)
			if test.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, test.wantErr)
			}
			require.True(t, fixture.rebuilder.rebuildSlot.TryAcquire(1))
			fixture.rebuilder.rebuildSlot.Release(1)
		})
	}
}

func TestRebuildPassesCallerContextToEL(t *testing.T) {
	fixture := newRebuildFixture(t)
	type contextKey struct{}
	ctx := context.WithValue(t.Context(), contextKey{}, "caller")

	_, err := fixture.rebuild(t, ctx)
	require.NoError(t, err)
	require.Equal(t, "caller", fixture.engine.headerCtx.Value(contextKey{}))
	require.Equal(t, "caller", fixture.engine.bodyCtx.Value(contextKey{}))
}

func newRebuildFixture(t *testing.T) *rebuildFixture {
	return newRebuildFixtureWithBAL(t, []byte{12, 13, 14})
}

func newRebuildFixtureWithBAL(t *testing.T, bal []byte) *rebuildFixture {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	reader := tests.NewMockBlockReader()
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	requests := cltypes.NewExecutionRequestsWithVersion(&cfg, clparams.GloasVersion)
	payload := cltypes.NewEth1Block(clparams.GloasVersion, &cfg)
	payload.ParentHash = common.Hash{1}
	payload.FeeRecipient = common.Address{2}
	payload.StateRoot = common.Hash{3}
	payload.ReceiptsRoot = common.Hash{4}
	payload.PrevRandao = common.Hash{5}
	payload.BlockNumber = 6
	payload.GasLimit = 7
	payload.GasUsed = 8
	payload.Time = 9
	payload.SlotNumber = 10
	payload.Extra = solid.NewExtraData()
	payload.Extra.SetBytes([]byte{11})
	payload.Transactions = solid.NewProgressiveTransactionsSSZFromTransactions([][]byte{{0xc0}})
	payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload), 44)
	require.NoError(t, payload.BlockAccessList.SetBytes(bal))
	parentRoot := common.Hash{15}
	requestsHash := cltypes.ComputeExecutionRequestHash(cltypes.GetExecutionRequestsList(&cfg, requests))
	blockHash, err := payload.ComputeBlockHash(&parentRoot, requestsHash, nil)
	require.NoError(t, err)
	payload.BlockHash = blockHash
	header, err := payload.RlpHeader(&parentRoot, requestsHash, nil)
	require.NoError(t, err)

	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	block.Block.Slot = payload.SlotNumber
	block.Block.ParentRoot = parentRoot
	bid := block.Block.Body.SignedExecutionPayloadBid.Message
	bid.ParentBlockRoot = parentRoot
	bid.ParentBlockHash = payload.ParentHash
	bid.BlockHash = payload.BlockHash
	bid.PrevRandao = payload.PrevRandao
	bid.GasLimit = payload.GasLimit
	bid.BuilderIndex = 16
	bid.Slot = payload.SlotNumber
	bid.ExecutionRequestsRoot, err = requests.HashSSZ()
	require.NoError(t, err)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)

	child := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	child.Block.Slot = block.Block.Slot + 2
	child.Block.ParentRoot = root
	child.Block.Body.SignedExecutionPayloadBid.Message.ParentBlockHash = payload.BlockHash
	child.Block.Body.ParentExecutionRequests = requests
	childRoot, err := child.Block.HashSSZ()
	require.NoError(t, err)
	reader.U[block.Block.Slot] = block
	reader.U[child.Block.Slot] = child
	signature := common.Bytes96{17}
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		if err := beacon_indicies.MarkRootCanonical(tx, block.Block.Slot, root); err != nil {
			return err
		}
		if err := beacon_indicies.MarkRootCanonical(tx, child.Block.Slot, childRoot); err != nil {
			return err
		}
		return beacon_indicies.WriteExecutionPayloadEnvelopeSignature(tx, block.Block.Slot, root, signature)
	}))
	body := &engine_types.ExecutionPayloadBodyV2{
		Transactions:    []hexutil.Bytes{{0xc0}},
		Withdrawals:     types.Withdrawals{},
		BlockAccessList: (*hexutil.Bytes)(&bal),
	}
	engine := &testEngine{header: header, body: body}
	want := &cltypes.SignedExecutionPayloadEnvelope{
		Message: &cltypes.ExecutionPayloadEnvelope{
			Payload:               payload,
			ExecutionRequests:     requests,
			BuilderIndex:          bid.BuilderIndex,
			BeaconBlockRoot:       root,
			ParentBeaconBlockRoot: parentRoot,
		},
		Signature: signature,
	}
	return &rebuildFixture{
		reader:    reader,
		db:        db,
		engine:    engine,
		rebuilder: NewRebuilder(&cfg, reader, engine),
		block:     block,
		child:     child,
		root:      root,
		want:      want,
		bal:       bal,
	}
}

func (f *rebuildFixture) rebuild(t *testing.T, ctx context.Context) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	t.Helper()
	var prepared *PreparedRebuild
	err := f.db.View(t.Context(), func(tx kv.Tx) error {
		var err error
		prepared, err = f.rebuilder.Prepare(ctx, tx, f.block.Block.Slot, f.root)
		return err
	})
	if err != nil || prepared == nil {
		return nil, err
	}
	return f.rebuilder.Rebuild(ctx, prepared)
}

func (f *rebuildFixture) prepare(t *testing.T) *PreparedRebuild {
	t.Helper()
	var prepared *PreparedRebuild
	require.NoError(t, f.db.View(t.Context(), func(tx kv.Tx) error {
		var err error
		prepared, err = f.rebuilder.Prepare(t.Context(), tx, f.block.Block.Slot, f.root)
		return err
	}))
	require.NotNil(t, prepared)
	return prepared
}
