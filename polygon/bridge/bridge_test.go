package bridge_test

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"github.com/ledgerwatch/erigon-lib/log/v3"
	"github.com/ledgerwatch/erigon/accounts/abi"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/polygon/bor"
	"github.com/ledgerwatch/erigon/polygon/bor/borcfg"
	"github.com/ledgerwatch/erigon/polygon/bridge"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
	"github.com/ledgerwatch/erigon/polygon/polygoncommon"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/testlog"
)

func setup(t *testing.T, abi abi.ABI) (*heimdall.MockHeimdallClient, *bridge.Bridge) {
	ctrl := gomock.NewController(t)
	logger := testlog.Logger(t, log.LvlDebug)
	borConfig := borcfg.BorConfig{
		Sprint:                map[string]uint64{"0": 2},
		StateReceiverContract: "0x0000000000000000000000000000000000001001",
	}

	heimdallClient := heimdall.NewMockHeimdallClient(ctrl)
	polygonBridgeDB := polygoncommon.NewDatabase(t.TempDir(), logger)
	store := bridge.NewStore(polygonBridgeDB)
	b := bridge.NewBridge(store, logger, &borConfig, heimdallClient.FetchStateSyncEvents, abi)

	return heimdallClient, b
}

func getBlocks(t *testing.T, numBlocks int) []*types.Block {
	// Feed in new blocks
	rawBlocks := make([]*types.RawBlock, 0, numBlocks)

	for i := 0; i < numBlocks; i++ {
		rawBlocks = append(rawBlocks, &types.RawBlock{
			Header: &types.Header{
				Number: big.NewInt(int64(i)),
				Time:   uint64(50 * (i + 1)),
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

	stateReceiverABI := bor.GenesisContractStateReceiverABI()
	heimdallClient, b := setup(t, stateReceiverABI)

	event1 := &heimdall.EventRecordWithTime{
		EventRecord: heimdall.EventRecord{
			ID:      1,
			ChainID: "80001",
			Data:    hexutil.MustDecode("0x01"),
		},
		Time: time.Unix(50, 0), // block 2
	}
	event2 := &heimdall.EventRecordWithTime{
		EventRecord: heimdall.EventRecord{
			ID:      2,
			ChainID: "80001",
			Data:    hexutil.MustDecode("0x02"),
		},
		Time: time.Unix(100, 0), // block 2
	}
	event3 := &heimdall.EventRecordWithTime{
		EventRecord: heimdall.EventRecord{
			ID:      3,
			ChainID: "80001",
			Data:    hexutil.MustDecode("0x03"),
		},
		Time: time.Unix(200, 0), // block 4
	}

	events := []*heimdall.EventRecordWithTime{event1, event2, event3}

	heimdallClient.EXPECT().FetchStateSyncEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(events, nil).Times(1)
	heimdallClient.EXPECT().FetchStateSyncEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]*heimdall.EventRecordWithTime{}, nil).AnyTimes()

	var wg sync.WaitGroup
	wg.Add(1)

	go func(bridge bridge.Service) {
		defer wg.Done()

		err := bridge.Run(ctx)
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				t.Error(err)
			}

			return
		}
	}(b)

	err := b.Synchronize(ctx, &types.Header{Number: big.NewInt(100)}) // hack to wait for b.ready
	require.NoError(t, err)

	blocks := getBlocks(t, 5)

	err = b.ProcessNewBlocks(ctx, blocks)
	require.NoError(t, err)

	res, err := b.GetEvents(ctx, 2)
	require.NoError(t, err)

	event1Data, err := event1.Pack(stateReceiverABI)
	require.NoError(t, err)

	event2Data, err := event2.Pack(stateReceiverABI)
	require.NoError(t, err)

	require.Equal(t, 2, len(res))                             // have first two events
	require.Equal(t, event1Data, rlp.RawValue(res[0].Data())) // check data fields
	require.Equal(t, event2Data, rlp.RawValue(res[1].Data()))

	res, err = b.GetEvents(ctx, 4)
	require.NoError(t, err)

	event3Data, err := event3.Pack(stateReceiverABI)
	require.NoError(t, err)

	require.Equal(t, 1, len(res))
	require.Equal(t, event3Data, rlp.RawValue(res[0].Data()))

	// get non-sprint block
	_, err = b.GetEvents(ctx, 1)
	require.Error(t, err)

	_, err = b.GetEvents(ctx, 3)
	require.Error(t, err)

	// check block 0
	_, err = b.GetEvents(ctx, 0)
	require.Error(t, err)

	cancel()
	wg.Wait()
}

func TestBridge_Unwind(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	stateReceiverABI := bor.GenesisContractStateReceiverABI()
	heimdallClient, b := setup(t, stateReceiverABI)

	event1 := &heimdall.EventRecordWithTime{
		EventRecord: heimdall.EventRecord{
			ID:      1,
			ChainID: "80001",
			Data:    hexutil.MustDecode("0x01"),
		},
		Time: time.Unix(50, 0), // block 2
	}
	event2 := &heimdall.EventRecordWithTime{
		EventRecord: heimdall.EventRecord{
			ID:      2,
			ChainID: "80001",
			Data:    hexutil.MustDecode("0x02"),
		},
		Time: time.Unix(100, 0), // block 2
	}
	event3 := &heimdall.EventRecordWithTime{
		EventRecord: heimdall.EventRecord{
			ID:      3,
			ChainID: "80001",
			Data:    hexutil.MustDecode("0x03"),
		},
		Time: time.Unix(200, 0), // block 4
	}
	event4 := &heimdall.EventRecordWithTime{
		EventRecord: heimdall.EventRecord{
			ID:      4,
			ChainID: "80001",
			Data:    hexutil.MustDecode("0x03"),
		},
		Time: time.Unix(300, 0), // block 6
	}

	events := []*heimdall.EventRecordWithTime{event1, event2, event3, event4}

	heimdallClient.EXPECT().FetchStateSyncEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(events, nil).Times(1)
	heimdallClient.EXPECT().FetchStateSyncEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]*heimdall.EventRecordWithTime{}, nil).AnyTimes()

	var wg sync.WaitGroup
	wg.Add(1)

	go func(bridge bridge.Service) {
		defer wg.Done()

		err := bridge.Run(ctx)
		if err != nil {
			if !errors.Is(err, ctx.Err()) {
				t.Error(err)
			}

			return
		}
	}(b)

	err := b.Synchronize(ctx, &types.Header{Number: big.NewInt(100)}) // hack to wait for b.ready
	require.NoError(t, err)

	blocks := getBlocks(t, 8)

	err = b.ProcessNewBlocks(ctx, blocks)
	require.NoError(t, err)

	event3Data, err := event3.Pack(stateReceiverABI)
	require.NoError(t, err)

	res, err := b.GetEvents(ctx, 4)
	require.Equal(t, event3Data, rlp.RawValue(res[0].Data()))
	require.NoError(t, err)

	err = b.Unwind(ctx, &types.Header{Number: big.NewInt(3)})
	require.NoError(t, err)

	_, err = b.GetEvents(ctx, 4)
	require.Error(t, err)

	cancel()
	wg.Wait()
}
