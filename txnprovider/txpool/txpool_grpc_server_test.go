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
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	accounts3 "github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

type addMockTxPool struct {
	knownByCall      []bool
	idHashKnownCalls int
	addReasons       []txpoolcfg.DiscardReason
	addLocalSlotsLen int
}

func (m *addMockTxPool) ValidateSerializedTxn(serializedTxn []byte) error { return nil }
func (m *addMockTxPool) PeekBest(ctx context.Context, n int, txns *TxnsRlp, onTopOf uint64) (bool, error) {
	return false, nil
}
func (m *addMockTxPool) GetRlp(tx kv.Tx, hash []byte) ([]byte, error) { return nil, nil }
func (m *addMockTxPool) AddLocalTxns(ctx context.Context, newTxns TxnSlots) ([]txpoolcfg.DiscardReason, error) {
	m.addLocalSlotsLen = len(newTxns.Txns)
	return m.addReasons, nil
}
func (m *addMockTxPool) deprecatedForEach(f func(rlp []byte, sender common.Address, t SubPoolType), tx kv.Tx) {
}
func (m *addMockTxPool) CountContent() (int, int, int) { return 0, 0, 0 }
func (m *addMockTxPool) IdHashKnown(tx kv.Tx, hash []byte) (bool, error) {
	i := m.idHashKnownCalls
	m.idHashKnownCalls++
	if i < len(m.knownByCall) {
		return m.knownByCall[i], nil
	}
	return false, nil
}
func (m *addMockTxPool) NonceFromAddress(addr [20]byte) (nonce uint64, inPool bool)       { return 0, false }
func (m *addMockTxPool) GetBlobs(blobhashes []common.Hash) (blobBundles []PoolBlobBundle) { return nil }

func TestGrpcServerAddDiscardReasonIndexAlignment(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	chainID := *uint256.NewInt(1)

	mockPool := &addMockTxPool{
		knownByCall: []bool{true, false}, // first tx treated as already-known, second goes to AddLocalTxns
		addReasons:  []txpoolcfg.DiscardReason{txpoolcfg.Success},
	}

	s := NewGrpcServer(ctx, mockPool, memdb.NewTestPoolDB(t), nil, chainID, log.New())
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
	if reply.Imported[1] != txpoolproto.ImportResult_SUCCESS || reply.Errors[1] != txpoolcfg.Success.String() {
		t.Fatalf("unexpected second tx result: imported=%v error=%q", reply.Imported[1], reply.Errors[1])
	}
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
	poolDB := memdb.NewTestPoolDB(t)
	cfg := txpoolcfg.DefaultConfig
	cache := kvcache.New(kvcache.DefaultCoherentConfig)
	pool, err := New(ctx, newTxns, poolDB, chainDB, cfg, cache, chain.AllProtocolChanges, nil, nil, func() {}, nil, nil, log.New())
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}

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
	if err := pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}); err != nil {
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
	allTasks.Add(1)
	go func() {
		defer allTasks.Done()
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
	}()

	// Mutator task: alternate between replacement and mined-removal cycles
	allTasks.Add(1)
	go func() {
		defer allTasks.Done()
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
	}()

	// BaseFee churn task: alternates base fee above/below thresholds to force demotions/promotions across sub-pools
	// while sender mapping remains.
	allTasks.Add(1)
	go func() {
		defer allTasks.Done()
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
	}()

	// Wait for all tasks to finish
	allTasks.Wait()

	select {
	case <-panicCh:
		t.Fatalf("panic(\"unknown\") triggered")
	case <-ctx.Done():
		// Success
	}
}
