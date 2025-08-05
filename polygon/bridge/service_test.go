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

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/testlog"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
)

var defaultBorConfig = borcfg.BorConfig{
	Sprint:                     map[string]uint64{"0": 2},
	StateReceiverContract:      "0x0000000000000000000000000000000000001001",
	IndoreBlock:                big.NewInt(10),
	StateSyncConfirmationDelay: map[string]uint64{"0": 1},
}

func setup(t *testing.T, borConfig borcfg.BorConfig) (*MockClient, *Service) {
	ctrl := gomock.NewController(t)
	logger := testlog.Logger(t, log.LvlDebug)
	bridgeClient := NewMockClient(ctrl)
	b := NewService(ServiceConfig{
		Store:        NewMdbxStore(t.TempDir(), logger, false, 1),
		Logger:       logger,
		BorConfig:    &borConfig,
		EventFetcher: bridgeClient,
	})
	t.Cleanup(b.Close)
	return bridgeClient, b
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

func TestService(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	heimdallClient, b := setup(t, defaultBorConfig)
	event1 := &EventRecordWithTime{
		EventRecord: EventRecord{
			ID:      1,
			ChainID: "80002",
			Data:    hexutil.MustDecode("0x01"),
		},
		// pre-indore: block0Time=1,block2Time=100,block4Time=200 => event1 falls in block4 (toTime=preSprintBlockTime=100)
		Time: time.Unix(50, 0),
	}
	event1Data, err := event1.MarshallBytes()
	require.NoError(t, err)
	event2 := &EventRecordWithTime{
		EventRecord: EventRecord{
			ID:      2,
			ChainID: "80002",
			Data:    hexutil.MustDecode("0x02"),
		},
		// pre-indore: block0Time=1,block2Time=100,block4Time=200 => event2 falls in block4 (toTime=preSprintBlockTime=100)
		Time: time.Unix(99, 0), // block 2
	}
	event2Data, err := event2.MarshallBytes()
	require.NoError(t, err)
	event3 := &EventRecordWithTime{
		EventRecord: EventRecord{
			ID:      3,
			ChainID: "80002",
			Data:    hexutil.MustDecode("0x03"),
		},
		// pre-indore: block4Time=200,block6Time=300 => event3 falls in block6 (toTime=preSprintBlockTime=200)
		Time: time.Unix(199, 0),
	}
	event3Data, err := event3.MarshallBytes()
	require.NoError(t, err)
	event4 := &EventRecordWithTime{
		EventRecord: EventRecord{
			ID:      4,
			ChainID: "80002",
			Data:    hexutil.MustDecode("0x04"),
		},
		// post-indore: block8Time=400,block10Time=500 => event4 falls in block10 (toTime=currentSprintBlockTime-delay=500-1=499)
		Time: time.Unix(498, 0),
	}
	event4Data, err := event4.MarshallBytes()
	require.NoError(t, err)

	events := []*EventRecordWithTime{event1, event2, event3, event4}

	heimdallClient.EXPECT().FetchStateSyncEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(events, nil).Times(1)
	heimdallClient.EXPECT().FetchStateSyncEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]*EventRecordWithTime{}, nil).AnyTimes()

	var wg sync.WaitGroup
	wg.Add(1)

	go func(bridge *Service) {
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

	res, err := b.Events(ctx, blocks[1].Hash(), 2)
	require.NoError(t, err)
	require.Empty(t, res)

	res, err = b.Events(ctx, blocks[3].Hash(), 4)
	require.NoError(t, err)
	require.Len(t, res, 2)                      // have first two events
	require.Equal(t, event1Data, res[0].Data()) // check data fields
	require.Equal(t, event2Data, res[1].Data())

	res, err = b.Events(ctx, blocks[5].Hash(), 6)
	require.NoError(t, err)
	require.Len(t, res, 1)                      // have third event
	require.Equal(t, event3Data, res[0].Data()) // check data fields

	res, err = b.Events(ctx, blocks[9].Hash(), 10)
	require.NoError(t, err)
	require.Len(t, res, 1)                      // have fourth event
	require.Equal(t, event4Data, res[0].Data()) // check data fields

	// get non-sprint block
	res, err = b.Events(ctx, blocks[0].Hash(), 1)
	require.Empty(t, res)
	require.NoError(t, err)

	res, err = b.Events(ctx, blocks[2].Hash(), 3)
	require.Empty(t, res)
	require.NoError(t, err)

	// check block 0
	res, err = b.Events(ctx, libcommon.Hash{}, 0)
	require.Empty(t, res)
	require.NoError(t, err)

	cancel()
	wg.Wait()
}

func TestService_Unwind(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	heimdallClient, b := setup(t, defaultBorConfig)
	event1 := &EventRecordWithTime{
		EventRecord: EventRecord{
			ID:      1,
			ChainID: "80002",
			Data:    hexutil.MustDecode("0x01"),
		},
		// pre-indore: block0Time=1,block2Time=100,block4Time=200 => event1 falls in block4 (toTime=preSprintBlockTime=100)
		Time: time.Unix(50, 0),
	}
	event2 := &EventRecordWithTime{
		EventRecord: EventRecord{
			ID:      2,
			ChainID: "80002",
			Data:    hexutil.MustDecode("0x02"),
		},
		// pre-indore: block0Time=1,block2Time=100,block4Time=200 => event2 falls in block4 (toTime=preSprintBlockTime=100)
		Time: time.Unix(99, 0), // block 2
	}
	event3 := &EventRecordWithTime{
		EventRecord: EventRecord{
			ID:      3,
			ChainID: "80002",
			Data:    hexutil.MustDecode("0x03"),
		},
		// pre-indore: block4Time=200,block6Time=300 => event3 falls in block6 (toTime=preSprintBlockTime=200)
		Time: time.Unix(199, 0),
	}
	event4 := &EventRecordWithTime{
		EventRecord: EventRecord{
			ID:      4,
			ChainID: "80002",
			Data:    hexutil.MustDecode("0x04"),
		},
		// post-indore: block8Time=400,block10Time=500 => event4 falls in block10 (toTime=currentSprintBlockTime-delay=500-1=499)
		Time: time.Unix(498, 0),
	}

	events := []*EventRecordWithTime{event1, event2, event3, event4}

	heimdallClient.EXPECT().FetchStateSyncEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(events, nil).Times(1)
	heimdallClient.EXPECT().FetchStateSyncEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]*EventRecordWithTime{}, nil).AnyTimes()

	var wg sync.WaitGroup
	wg.Add(1)

	go func(bridge *Service) {
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

	res, err := b.Events(ctx, blocks[3].Hash(), 4)
	require.NoError(t, err)
	require.Len(t, res, 2)
	res, err = b.Events(ctx, blocks[5].Hash(), 6)
	require.NoError(t, err)
	require.Len(t, res, 1)
	res, err = b.Events(ctx, blocks[9].Hash(), 10)
	require.NoError(t, err)
	require.Len(t, res, 1)

	err = b.Unwind(ctx, 5)
	require.NoError(t, err)

	res, err = b.Events(ctx, blocks[3].Hash(), 4)
	require.NoError(t, err)
	require.Len(t, res, 2)
	res, err = b.Events(ctx, blocks[5].Hash(), 6)
	require.NoError(t, err)
	require.Empty(t, res)
	res, err = b.Events(ctx, blocks[9].Hash(), 10)
	require.NoError(t, err)
	require.Empty(t, res)

	cancel()
	wg.Wait()
}

func setupOverrideTest(t *testing.T, ctx context.Context, borConfig borcfg.BorConfig, wg *sync.WaitGroup) (*Service, []*types.Block) {
	heimdallClient, b := setup(t, borConfig)
	event1 := &EventRecordWithTime{
		EventRecord: EventRecord{
			ID:      1,
			ChainID: "80002",
			Data:    hexutil.MustDecode("0x01"),
		},
		// pre-indore: block0Time=1,block2Time=100,block4Time=200 => event1 falls in block4 (toTime=preSprintBlockTime=100)
		Time: time.Unix(50, 0),
	}
	event2 := &EventRecordWithTime{
		EventRecord: EventRecord{
			ID:      2,
			ChainID: "80002",
			Data:    hexutil.MustDecode("0x02"),
		},
		// pre-indore: block0Time=1,block2Time=100,block4Time=200 => event2 should fall in block4 but skipped and put in block6 (toTime=preSprintBlockTime=100)
		Time: time.Unix(99, 0), // block 2
	}
	event3 := &EventRecordWithTime{
		EventRecord: EventRecord{
			ID:      3,
			ChainID: "80002",
			Data:    hexutil.MustDecode("0x03"),
		},
		// pre-indore: block4Time=200,block6Time=300 => event3 falls in block6 (toTime=preSprintBlockTime=200)
		Time: time.Unix(199, 0),
	}
	event4 := &EventRecordWithTime{
		EventRecord: EventRecord{
			ID:      4,
			ChainID: "80002",
			Data:    hexutil.MustDecode("0x04"),
		},
		// post-indore: block8Time=400,block10Time=500 => event4 falls in block10 (toTime=currentSprintBlockTime-delay=500-1=499)
		Time: time.Unix(498, 0),
	}
	event5 := &EventRecordWithTime{
		EventRecord: EventRecord{
			ID:      5,
			ChainID: "80002",
			Data:    hexutil.MustDecode("0x04"),
		},
		// post-indore: block10Time=500,block12Time=600 => event4 falls in block12 (toTime=currentSprintBlockTime-delay=600-1=599)
		Time: time.Unix(598, 0),
	}
	event6 := &EventRecordWithTime{
		EventRecord: EventRecord{
			ID:      6,
			ChainID: "80002",
			Data:    hexutil.MustDecode("0x04"),
		},
		// post-indore: block12Time=600,block14Time=700 => event4 falls in block14 (toTime=currentSprintBlockTime-delay=700-1=699)
		Time: time.Unix(698, 0),
	}

	events := []*EventRecordWithTime{event1, event2, event3, event4, event5, event6}

	heimdallClient.EXPECT().FetchStateSyncEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(events, nil).Times(1)
	heimdallClient.EXPECT().FetchStateSyncEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]*EventRecordWithTime{}, nil).AnyTimes()
	wg.Add(1)

	go func(bridge *Service) {
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

	blocks := getBlocks(t, 20)
	err = b.ProcessNewBlocks(ctx, blocks)
	require.NoError(t, err)

	return b, blocks
}

func TestService_ProcessNewBlocksWithOverride(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	var wg sync.WaitGroup
	borCfg := defaultBorConfig
	borCfg.OverrideStateSyncRecords = map[string]int{
		"4":       1,
		"r.12-14": 0,
	}
	b, blocks := setupOverrideTest(t, ctx, borCfg, &wg)

	res, err := b.Events(ctx, blocks[3].Hash(), 4) // should only have event1 as event2 is skipped and is present in block 6
	require.NoError(t, err)
	require.Len(t, res, 1)

	res, err = b.Events(ctx, blocks[5].Hash(), 6)
	require.NoError(t, err)
	require.Len(t, res, 2)

	res, err = b.Events(ctx, blocks[9].Hash(), 10)
	require.NoError(t, err)
	require.Len(t, res, 1)

	res, err = b.Events(ctx, blocks[11].Hash(), 12)
	require.NoError(t, err)
	require.Len(t, res, 0) // because we skip it for r.12-14 interval

	res, err = b.Events(ctx, blocks[13].Hash(), 14)
	require.NoError(t, err)
	require.Len(t, res, 0) // because we skip it for r.12-14 interval

	res, err = b.Events(ctx, blocks[15].Hash(), 16)
	require.NoError(t, err)
	require.Len(t, res, 2)

	cancel()
	wg.Wait()
}

func TestService_ProcessNewBlocksWithZeroOverride(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	var wg sync.WaitGroup
	borCfg := defaultBorConfig
	borCfg.OverrideStateSyncRecords = map[string]int{"4": 0}
	b, blocks := setupOverrideTest(t, ctx, borCfg, &wg)

	res, err := b.Events(ctx, blocks[3].Hash(), 4) // both event1 and event2 are in block 6
	require.NoError(t, err)
	require.Empty(t, res)

	res, err = b.Events(ctx, blocks[5].Hash(), 6)
	require.NoError(t, err)
	require.Len(t, res, 3)

	res, err = b.Events(ctx, blocks[9].Hash(), 10)
	require.NoError(t, err)
	require.Len(t, res, 1)

	cancel()
	wg.Wait()
}

func TestReaderEventsWithinTime(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	heimdallClient, b := setup(t, defaultBorConfig)
	event1 := &EventRecordWithTime{
		EventRecord: EventRecord{
			ID:      1,
			ChainID: "80002",
			Data:    hexutil.MustDecode("0x01"),
		},
		Time: time.Unix(50, 0),
	}
	event1Data, err := event1.MarshallBytes()
	require.NoError(t, err)
	event2 := &EventRecordWithTime{
		EventRecord: EventRecord{
			ID:      2,
			ChainID: "80002",
			Data:    hexutil.MustDecode("0x02"),
		},
		Time: time.Unix(99, 0),
	}
	event2Data, err := event2.MarshallBytes()
	require.NoError(t, err)
	event3 := &EventRecordWithTime{
		EventRecord: EventRecord{
			ID:      3,
			ChainID: "80002",
			Data:    hexutil.MustDecode("0x03"),
		},
		Time: time.Unix(199, 0),
	}
	event3Data, err := event3.MarshallBytes()
	require.NoError(t, err)
	event4 := &EventRecordWithTime{
		EventRecord: EventRecord{
			ID:      4,
			ChainID: "80002",
			Data:    hexutil.MustDecode("0x04"),
		},
		Time: time.Unix(498, 0),
	}

	events := []*EventRecordWithTime{event1, event2, event3, event4}

	heimdallClient.EXPECT().FetchStateSyncEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(events, nil).Times(1)
	heimdallClient.EXPECT().FetchStateSyncEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]*EventRecordWithTime{}, nil).AnyTimes()

	var wg sync.WaitGroup
	wg.Add(1)

	go func(bridge *Service) {
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

	res, err := b.EventsWithinTime(ctx, time.Unix(1, 0), time.Unix(500, 0))
	require.NoError(t, err)
	require.Len(t, res, 4)

	res, err = b.EventsWithinTime(ctx, time.Unix(50, 0), time.Unix(100, 0))
	require.NoError(t, err)
	require.Len(t, res, 2)                      // have first two events
	require.Equal(t, event1Data, res[0].Data()) // check data fields
	require.Equal(t, event2Data, res[1].Data())

	res, err = b.EventsWithinTime(ctx, time.Unix(199, 0), time.Unix(498, 0))
	require.NoError(t, err)
	require.Len(t, res, 1)                      // have third event but not fourth because [A, B) does not include B
	require.Equal(t, event3Data, res[0].Data()) // check data fields

	res, err = b.EventsWithinTime(ctx, time.Unix(500, 0), time.Unix(600, 0))
	require.Empty(t, res)
	require.NoError(t, err)

	cancel()
	wg.Wait()
}
