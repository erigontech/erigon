package bridge

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/turbo/testlog"
)

var defaultBorConfig = borcfg.BorConfig{
	Sprint:                     map[string]uint64{"0": 2},
	StateReceiverContract:      "0x0000000000000000000000000000000000001001",
	IndoreBlock:                big.NewInt(10),
	StateSyncConfirmationDelay: map[string]uint64{"0": 1},
}

func setup(t *testing.T, borConfig borcfg.BorConfig) (*heimdall.MockHeimdallClient, *Bridge) {
	ctrl := gomock.NewController(t)
	logger := testlog.Logger(t, log.LvlDebug)
	heimdallClient := heimdall.NewMockHeimdallClient(ctrl)
	b := Assemble(t.TempDir(), logger, &borConfig, heimdallClient)
	t.Cleanup(b.Close)
	return heimdallClient, b
}

func getBlocks(t *testing.T, numBlocks int) []*types.Block {
	// Feed in new blocks
	rawBlocks := make([]*types.RawBlock, 0, numBlocks)

	for i := 1; i <= numBlocks; i++ {
		rawBlocks = append(rawBlocks, &types.RawBlock{
			Header: &types.Header{
				Number: big.NewInt(int64(i)),
				Time:   uint64(50 * i),
			},
			Body: &types.RawBody{},
		})
	}

	blocks := make([]*types.Block, len(rawBlocks))

	for i, rawBlock := range rawBlocks {
		b, err := rawBlock.AsBlock()
		require.NoError(t, err)

		blocks[i] = b
	}

	return blocks
}

func TestBridge(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	heimdallClient, b := setup(t, defaultBorConfig)
	event1 := &heimdall.EventRecordWithTime{
		EventRecord: heimdall.EventRecord{
			ID:      1,
			ChainID: "80002",
			Data:    hexutil.MustDecode("0x01"),
		},
		// pre-indore: block0Time=1,block2Time=100,block4Time=200 => event1 falls in block4 (toTime=preSprintBlockTime=100)
		Time: time.Unix(50, 0),
	}
	event1Data, err := event1.MarshallBytes()
	require.NoError(t, err)
	event2 := &heimdall.EventRecordWithTime{
		EventRecord: heimdall.EventRecord{
			ID:      2,
			ChainID: "80002",
			Data:    hexutil.MustDecode("0x02"),
		},
		// pre-indore: block0Time=1,block2Time=100,block4Time=200 => event2 falls in block4 (toTime=preSprintBlockTime=100)
		Time: time.Unix(99, 0), // block 2
	}
	event2Data, err := event2.MarshallBytes()
	require.NoError(t, err)
	event3 := &heimdall.EventRecordWithTime{
		EventRecord: heimdall.EventRecord{
			ID:      3,
			ChainID: "80002",
			Data:    hexutil.MustDecode("0x03"),
		},
		// pre-indore: block4Time=200,block6Time=300 => event3 falls in block6 (toTime=preSprintBlockTime=200)
		Time: time.Unix(199, 0),
	}
	event3Data, err := event3.MarshallBytes()
	require.NoError(t, err)
	event4 := &heimdall.EventRecordWithTime{
		EventRecord: heimdall.EventRecord{
			ID:      4,
			ChainID: "80002",
			Data:    hexutil.MustDecode("0x04"),
		},
		// post-indore: block8Time=400,block10Time=500 => event4 falls in block10 (toTime=currentSprintBlockTime-delay=500-1=499)
		Time: time.Unix(498, 0),
	}
	event4Data, err := event4.MarshallBytes()
	require.NoError(t, err)

	events := []*heimdall.EventRecordWithTime{event1, event2, event3, event4}

	heimdallClient.EXPECT().FetchStateSyncEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(events, nil).Times(1)
	heimdallClient.EXPECT().FetchStateSyncEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]*heimdall.EventRecordWithTime{}, nil).AnyTimes()

	var wg sync.WaitGroup
	wg.Add(1)

	go func(bridge Service) {
		defer wg.Done()

		err := bridge.Run(ctx)
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				t.Error(err)
			}

			return
		}
	}(b)

	err = b.store.Prepare(ctx)
	require.NoError(t, err)

	replayBlockNum, replayNeeded, err := b.InitialBlockReplayNeeded(ctx)
	require.NoError(t, err)
	require.True(t, replayNeeded)
	require.Equal(t, uint64(0), replayBlockNum)

	genesis := types.NewBlockWithHeader(&types.Header{Time: 1, Number: big.NewInt(0)})
	err = b.ReplayInitialBlock(ctx, genesis)
	require.NoError(t, err)

	blocks := getBlocks(t, 10)
	err = b.ProcessNewBlocks(ctx, blocks)
	require.NoError(t, err)

	err = b.Synchronize(ctx, 6)
	require.NoError(t, err)

	res, err := b.Events(ctx, 2)
	require.NoError(t, err)
	require.Len(t, res, 0)

	res, err = b.Events(ctx, 4)
	require.NoError(t, err)
	require.Len(t, res, 2)                      // have first two events
	require.Equal(t, event1Data, res[0].Data()) // check data fields
	require.Equal(t, event2Data, res[1].Data())

	res, err = b.Events(ctx, 6)
	require.NoError(t, err)
	require.Len(t, res, 1)                      // have third event
	require.Equal(t, event3Data, res[0].Data()) // check data fields

	res, err = b.Events(ctx, 10)
	require.NoError(t, err)
	require.Len(t, res, 1)                      // have fourth event
	require.Equal(t, event4Data, res[0].Data()) // check data fields

	// get non-sprint block
	res, err = b.Events(ctx, 1)
	require.Equal(t, len(res), 0)
	require.NoError(t, err)

	res, err = b.Events(ctx, 3)
	require.Equal(t, len(res), 0)
	require.NoError(t, err)

	// check block 0
	res, err = b.Events(ctx, 0)
	require.Equal(t, len(res), 0)
	require.NoError(t, err)

	cancel()
	wg.Wait()
}

func TestBridge_Unwind(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	heimdallClient, b := setup(t, defaultBorConfig)
	event1 := &heimdall.EventRecordWithTime{
		EventRecord: heimdall.EventRecord{
			ID:      1,
			ChainID: "80002",
			Data:    hexutil.MustDecode("0x01"),
		},
		// pre-indore: block0Time=1,block2Time=100,block4Time=200 => event1 falls in block4 (toTime=preSprintBlockTime=100)
		Time: time.Unix(50, 0),
	}
	event2 := &heimdall.EventRecordWithTime{
		EventRecord: heimdall.EventRecord{
			ID:      2,
			ChainID: "80002",
			Data:    hexutil.MustDecode("0x02"),
		},
		// pre-indore: block0Time=1,block2Time=100,block4Time=200 => event2 falls in block4 (toTime=preSprintBlockTime=100)
		Time: time.Unix(99, 0), // block 2
	}
	event3 := &heimdall.EventRecordWithTime{
		EventRecord: heimdall.EventRecord{
			ID:      3,
			ChainID: "80002",
			Data:    hexutil.MustDecode("0x03"),
		},
		// pre-indore: block4Time=200,block6Time=300 => event3 falls in block6 (toTime=preSprintBlockTime=200)
		Time: time.Unix(199, 0),
	}
	event4 := &heimdall.EventRecordWithTime{
		EventRecord: heimdall.EventRecord{
			ID:      4,
			ChainID: "80002",
			Data:    hexutil.MustDecode("0x04"),
		},
		// post-indore: block8Time=400,block10Time=500 => event4 falls in block10 (toTime=currentSprintBlockTime-delay=500-1=499)
		Time: time.Unix(498, 0),
	}

	events := []*heimdall.EventRecordWithTime{event1, event2, event3, event4}

	heimdallClient.EXPECT().FetchStateSyncEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(events, nil).Times(1)
	heimdallClient.EXPECT().FetchStateSyncEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]*heimdall.EventRecordWithTime{}, nil).AnyTimes()

	var wg sync.WaitGroup
	wg.Add(1)

	go func(bridge Service) {
		defer wg.Done()

		err := bridge.Run(ctx)
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				t.Error(err)
			}

			return
		}
	}(b)

	err := b.store.Prepare(ctx)
	require.NoError(t, err)

	genesis := types.NewBlockWithHeader(&types.Header{Time: 1, Number: big.NewInt(0)})
	err = b.ReplayInitialBlock(ctx, genesis)
	require.NoError(t, err)

	blocks := getBlocks(t, 10)
	err = b.ProcessNewBlocks(ctx, blocks)
	require.NoError(t, err)

	err = b.Synchronize(ctx, 10)
	require.NoError(t, err)

	res, err := b.Events(ctx, 4)
	require.NoError(t, err)
	require.Len(t, res, 2)
	res, err = b.Events(ctx, 6)
	require.NoError(t, err)
	require.Len(t, res, 1)
	res, err = b.Events(ctx, 10)
	require.NoError(t, err)
	require.Len(t, res, 1)

	err = b.Unwind(ctx, 5)
	require.NoError(t, err)

	res, err = b.Events(ctx, 4)
	require.NoError(t, err)
	require.Len(t, res, 2)
	res, err = b.Events(ctx, 6)
	require.NoError(t, err)
	require.Len(t, res, 0)
	res, err = b.Events(ctx, 10)
	require.NoError(t, err)
	require.Len(t, res, 0)

	cancel()
	wg.Wait()
}

func setupOverrideTest(t *testing.T, ctx context.Context, borConfig borcfg.BorConfig, wg *sync.WaitGroup) *Bridge {
	heimdallClient, b := setup(t, borConfig)
	event1 := &heimdall.EventRecordWithTime{
		EventRecord: heimdall.EventRecord{
			ID:      1,
			ChainID: "80002",
			Data:    hexutil.MustDecode("0x01"),
		},
		// pre-indore: block0Time=1,block2Time=100,block4Time=200 => event1 falls in block4 (toTime=preSprintBlockTime=100)
		Time: time.Unix(50, 0),
	}
	event2 := &heimdall.EventRecordWithTime{
		EventRecord: heimdall.EventRecord{
			ID:      2,
			ChainID: "80002",
			Data:    hexutil.MustDecode("0x02"),
		},
		// pre-indore: block0Time=1,block2Time=100,block4Time=200 => event2 should fall in block4 but skipped and put in block6 (toTime=preSprintBlockTime=100)
		Time: time.Unix(99, 0), // block 2
	}
	event3 := &heimdall.EventRecordWithTime{
		EventRecord: heimdall.EventRecord{
			ID:      3,
			ChainID: "80002",
			Data:    hexutil.MustDecode("0x03"),
		},
		// pre-indore: block4Time=200,block6Time=300 => event3 falls in block6 (toTime=preSprintBlockTime=200)
		Time: time.Unix(199, 0),
	}
	event4 := &heimdall.EventRecordWithTime{
		EventRecord: heimdall.EventRecord{
			ID:      4,
			ChainID: "80002",
			Data:    hexutil.MustDecode("0x04"),
		},
		// post-indore: block8Time=400,block10Time=500 => event4 falls in block10 (toTime=currentSprintBlockTime-delay=500-1=499)
		Time: time.Unix(498, 0),
	}

	events := []*heimdall.EventRecordWithTime{event1, event2, event3, event4}

	heimdallClient.EXPECT().FetchStateSyncEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(events, nil).Times(1)
	heimdallClient.EXPECT().FetchStateSyncEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]*heimdall.EventRecordWithTime{}, nil).AnyTimes()
	wg.Add(1)

	go func(bridge Service) {
		defer wg.Done()

		err := bridge.Run(ctx)
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				t.Error(err)
			}

			return
		}
	}(b)

	err := b.store.Prepare(ctx)
	require.NoError(t, err)

	replayBlockNum, replayNeeded, err := b.InitialBlockReplayNeeded(ctx)
	require.NoError(t, err)
	require.True(t, replayNeeded)
	require.Equal(t, uint64(0), replayBlockNum)

	genesis := types.NewBlockWithHeader(&types.Header{Time: 1, Number: big.NewInt(0)})
	err = b.ReplayInitialBlock(ctx, genesis)
	require.NoError(t, err)

	blocks := getBlocks(t, 10)
	err = b.ProcessNewBlocks(ctx, blocks)
	require.NoError(t, err)

	err = b.Synchronize(ctx, 10)
	require.NoError(t, err)

	return b
}

func TestBridge_ProcessNewBlocksWithOverride(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	var wg sync.WaitGroup
	borCfg := defaultBorConfig
	borCfg.OverrideStateSyncRecords = map[string]int{"4": 1}
	b := setupOverrideTest(t, ctx, borCfg, &wg)

	res, err := b.Events(ctx, 4) // should only have event1 as event2 is skipped and is present in block 6
	require.NoError(t, err)
	require.Len(t, res, 1)

	res, err = b.Events(ctx, 6)
	require.NoError(t, err)
	require.Len(t, res, 2)

	res, err = b.Events(ctx, 10)
	require.NoError(t, err)
	require.Len(t, res, 1)

	cancel()
	wg.Wait()
}

func TestBridge_ProcessNewBlocksWithZeroOverride(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	var wg sync.WaitGroup
	borCfg := defaultBorConfig
	borCfg.OverrideStateSyncRecords = map[string]int{"4": 0}
	b := setupOverrideTest(t, ctx, borCfg, &wg)

	res, err := b.Events(ctx, 4) // both event1 and event2 are in block 6
	require.NoError(t, err)
	require.Len(t, res, 0)

	res, err = b.Events(ctx, 6)
	require.NoError(t, err)
	require.Len(t, res, 3)

	res, err = b.Events(ctx, 10)
	require.NoError(t, err)
	require.Len(t, res, 1)

	cancel()
	wg.Wait()
}
