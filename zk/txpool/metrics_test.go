package txpool

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/u256"
	"github.com/erigontech/erigon-lib/gointerfaces"
	"github.com/erigontech/erigon-lib/gointerfaces/remote"
	"github.com/erigontech/erigon-lib/kv/kvcache"
	"github.com/erigontech/erigon-lib/kv/memdb"
	"github.com/erigontech/erigon-lib/kv/temporal/temporaltest"
	"github.com/erigontech/erigon-lib/txpool/txpoolcfg"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/google/btree"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCalculateMetrics(t *testing.T) {
	assert, require := assert.New(t), require.New(t)
	ctx := context.Background()
	var addr [20]byte
	addr[0] = 1
	v := make([]byte, types.EncodeSenderLengthForStorage(2, *uint256.NewInt(18 * common.Ether)))
	types.EncodeSender(2, *uint256.NewInt(18 * common.Ether), v)

	scenarios := map[string]struct {
		firstRunTxIn         uint64
		firstRunTxOut        uint64
		expectedFirstIn      uint64
		expectedFirstOut     uint64
		secondRunTxIn        uint64
		secondRunTxOut       uint64
		expectedSecondRunIn  uint64
		expectedSecondRunOut uint64
	}{
		"first run 3 in 3 out second run 3 in 1 out": {
			firstRunTxIn:         3,
			firstRunTxOut:        3,
			expectedFirstIn:      3,
			expectedFirstOut:     3,
			secondRunTxIn:        3,
			secondRunTxOut:       1,
			expectedSecondRunIn:  3,
			expectedSecondRunOut: 1,
		},
		"first run 3 in 3 out second run 10 in 0 out": {
			firstRunTxIn:         3,
			firstRunTxOut:        3,
			expectedFirstIn:      3,
			expectedFirstOut:     3,
			secondRunTxIn:        10,
			secondRunTxOut:       0,
			expectedSecondRunIn:  10,
			expectedSecondRunOut: 0,
		},
		"first run 10 in 0 out second run 10 in 0 out": {
			firstRunTxIn:         10,
			firstRunTxOut:        0,
			expectedFirstIn:      10,
			expectedFirstOut:     0,
			secondRunTxIn:        10,
			secondRunTxOut:       0,
			expectedSecondRunIn:  10,
			expectedSecondRunOut: 0,
		},
		"first run 10 in 3 out second run 0 in 7 out": {
			firstRunTxIn:         10,
			firstRunTxOut:        3,
			expectedFirstIn:      10,
			expectedFirstOut:     3,
			secondRunTxIn:        0,
			secondRunTxOut:       7,
			expectedSecondRunIn:  0,
			expectedSecondRunOut: 7,
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			ch := make(chan types.Announcements, 100)
			_, coreDB, _ := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
			defer coreDB.Close()
			db := memdb.NewTestPoolDB(t)
			path := fmt.Sprintf("/tmp/db-test-%v", time.Now().UTC().Format(time.RFC3339Nano))
			txPoolDB := newTestTxPoolDB(t, path)
			defer txPoolDB.Close()
			aclsDB := newTestACLDB(t, path)
			defer aclsDB.Close()

			require.NotNil(t, db)
			require.NotNil(t, txPoolDB)
			require.NotNil(t, aclsDB)

			pool, err := New(ch, coreDB, txpoolcfg.DefaultConfig, kvcache.New(kvcache.DefaultCoherentConfig), uint256.Int(*u256.N1), nil, nil, nil, nil, nil, nil, &ethconfig.Defaults, aclsDB, nil)
			assert.NoError(err)
			require.True(pool != nil)

			var stateVersionID uint64 = 0
			pendingBaseFee := uint64(200000)
			h1 := gointerfaces.ConvertHashToH256([32]byte{})

			change := &remote.StateChangeBatch{
				StateVersionId:      stateVersionID,
				PendingBlockBaseFee: pendingBaseFee,
				BlockGasLimit:       1000000,
				ChangeBatch: []*remote.StateChange{
					{BlockHeight: 0, BlockHash: h1},
				},
			}
			change.ChangeBatch[0].Changes = append(change.ChangeBatch[0].Changes, &remote.AccountChange{
				Action:  remote.Action_UPSERT,
				Address: gointerfaces.ConvertAddressToH160(addr),
				Data:    v,
			})
			tx, err := db.BeginRw(ctx)
			require.NoError(err)
			defer tx.Rollback()
			err = pool.OnNewBlock(ctx, change, types.TxSlots{}, types.TxSlots{}, tx)
			assert.NoError(err)

			var nonceCounter uint64 = 3

			// first run in
			var txSlotsFirst types.TxSlots
			for i := uint64(0); i < scenario.firstRunTxIn; i++ {
				txSlot := &types.TxSlot{
					Tip:    *uint256.NewInt(300000),
					FeeCap: *uint256.NewInt(300000),
					Gas:    100000,
					Nonce:  nonceCounter,
				}
				txSlot.IDHash[0] = byte(i)
				txSlotsFirst.Append(txSlot, addr[:], true)
				nonceCounter++

				pool.metrics.IncrementCounter()
			}
			reasonsFirst, err := pool.AddLocalTxs(ctx, txSlotsFirst, tx)
			assert.NoError(err)
			for _, reason := range reasonsFirst {
				assert.Equal(Success, reason, reason.String())
			}

			poolFirst := make([]*metaTx, 0)
			pool.all.ascendAll(func(mt *metaTx) bool {
				poolFirst = append(poolFirst, mt)
				return true
			})
			for i, mt := range poolFirst {
				if i >= int(scenario.firstRunTxOut) {
					return
				}
				switch mt.currentSubPool {
				case PendingSubPool:
					pool.pending.Remove(mt)
				case BaseFeeSubPool:
					pool.baseFee.Remove(mt)
				case QueuedSubPool:
					pool.queued.Remove(mt)
				default:
				}
			}

			pool.metrics.Update(pool)
			assert.Equal(scenario.expectedFirstIn, pool.metrics.TxIn)
			assert.Equal(scenario.expectedFirstOut, pool.metrics.TxIn)

			// second run in
			var txSlotsSecond types.TxSlots
			for i := uint64(0); i < scenario.secondRunTxIn; i++ {
				txSlot := &types.TxSlot{
					Tip:    *uint256.NewInt(300000),
					FeeCap: *uint256.NewInt(300000),
					Gas:    100000,
					Nonce:  nonceCounter,
				}
				txSlot.IDHash[0] = byte(i)
				txSlotsFirst.Append(txSlot, addr[:], true)
				nonceCounter++

				pool.metrics.IncrementCounter()
			}
			reasonsSecond, err := pool.AddLocalTxs(ctx, txSlotsSecond, tx)
			assert.NoError(err)
			for _, reason := range reasonsSecond {
				assert.Equal(Success, reason, reason.String())
			}

			poolSecond := make([]*metaTx, 0)
			pool.all.ascendAll(func(mt *metaTx) bool {
				poolSecond = append(poolSecond, mt)
				return true
			})
			for i, mt := range poolSecond {
				if i >= int(scenario.secondRunTxOut) {
					return
				}
				switch mt.currentSubPool {
				case PendingSubPool:
					pool.pending.Remove(mt)
				case BaseFeeSubPool:
					pool.baseFee.Remove(mt)
				case QueuedSubPool:
					pool.queued.Remove(mt)
				default:
				}
			}

			pool.metrics.Update(pool)
			assert.Equal(scenario.expectedSecondRunIn, pool.metrics.TxIn)
			assert.Equal(scenario.expectedSecondRunOut, pool.metrics.TxOut)
		})
	}
}

func TestMedianTimeMetrics(t *testing.T) {
	pool := &TxPool{
		all: &BySenderAndNonce{
			tree:             btree.NewG[*metaTx](32, SortByNonceLess),
			search:           &metaTx{Tx: &types.TxSlot{}},
			senderIDTxnCount: map[uint64]int{},
		},
		pending: NewPendingSubPool(PendingSubPool, 100),
		baseFee: NewSubPool(BaseFeeSubPool, 100),
		queued:  NewSubPool(QueuedSubPool, 100),
		metrics: &Metrics{},
	}

	newMtx := &metaTx{
		Tx: &types.TxSlot{
			SenderID: 1,
		},
		created: uint64(time.Now().Add(-time.Hour).Unix()),
	}

	pool.all.replaceOrInsert(newMtx)

	pool.metrics.Update(pool)

	// we added a tx to the pool an hour ago so the median time should not be 0
	assert.NotEqual(t, uint64(0), pool.metrics.MedianWaitTimeSeconds)
}
