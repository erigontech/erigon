package bridge_test

import (
	"context"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"github.com/ledgerwatch/erigon-lib/log/v3"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/polygon/bor"
	"github.com/ledgerwatch/erigon/polygon/bor/borcfg"
	"github.com/ledgerwatch/erigon/polygon/bridge"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
	"github.com/ledgerwatch/erigon/polygon/polygoncommon"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/testlog"
)

func TestBridge(t *testing.T) {
	ctrl := gomock.NewController(t)
	ctx := context.Background()
	logger := testlog.Logger(t, log.LvlDebug)
	abi := bor.GenesisContractStateReceiverABI()
	borConfig := borcfg.BorConfig{
		Sprint:                map[string]uint64{"0": 2},
		StateReceiverContract: "0x0000000000000000000000000000000000001001",
	}

	heimdallClient := heimdall.NewMockHeimdallClient(ctrl)
	polygonBridgeDB := polygoncommon.NewDatabase(t.TempDir(), logger)
	store := bridge.NewStore(polygonBridgeDB)

	event1 := &heimdall.EventRecordWithTime{
		EventRecord: heimdall.EventRecord{
			ID:      1,
			ChainID: "80001",
			Data:    hexutil.MustDecode("0x01"),
		},
		Time: time.Unix(100, 0),
	}
	event2 := &heimdall.EventRecordWithTime{
		EventRecord: heimdall.EventRecord{
			ID:      2,
			ChainID: "80001",
			Data:    hexutil.MustDecode("0x02"),
		},
		Time: time.Unix(200, 0),
	}

	events := []*heimdall.EventRecordWithTime{
		event1, event2,
		{
			EventRecord: heimdall.EventRecord{
				ID:      3,
				ChainID: "80001",
				Data:    hexutil.MustDecode("0x03"),
			},
			Time: time.Unix(300, 0),
		},
	}

	heimdallClient.EXPECT().FetchStateSyncEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(events, nil).Times(1)
	heimdallClient.EXPECT().FetchStateSyncEvents(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return([]*heimdall.EventRecordWithTime{}, nil).AnyTimes()

	b := bridge.NewBridge(store, logger, &borConfig, heimdallClient.FetchStateSyncEvents, abi)

	var wg sync.WaitGroup
	wg.Add(1)

	go func(bridge bridge.Service) {
		defer wg.Done()

		err := bridge.Run(ctx)
		if err != nil {
			t.Error(err)
			return
		}
	}(b)

	err := b.Synchronize(ctx, &types.Header{Number: big.NewInt(100)}) // hack to wait for b.ready
	if err != nil {
		t.Fatal(err)
	}

	// Feed in new blocks
	rawBlocks := []*types.RawBlock{
		{
			Header: &types.Header{
				Number: big.NewInt(0),
				Time:   100,
			},
			Body: &types.RawBody{},
		},
		{
			Header: &types.Header{
				Number: big.NewInt(1),
				Time:   150,
			},
			Body: &types.RawBody{},
		},
		{
			Header: &types.Header{
				Number: big.NewInt(2),
				Time:   200,
			},
			Body: &types.RawBody{},
		},
		{
			Header: &types.Header{
				Number: big.NewInt(3),
				Time:   250,
			},
			Body: &types.RawBody{},
		},
	}

	blocks := make([]*types.Block, len(rawBlocks))

	for i, rawBlock := range rawBlocks {
		b, err := rawBlock.AsBlock()
		if err != nil {
			t.Fatal(err)
		}

		blocks[i] = b
	}

	err = b.ProcessNewBlocks(ctx, blocks)
	if err != nil {
		t.Fatal(err)
	}

	res, err := b.GetEvents(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}

	event1Data, err := event1.Pack(abi)
	if err != nil {
		t.Fatal(err)
	}

	event2Data, err := event2.Pack(abi)
	if err != nil {
		t.Fatal(err)
	}

	require.Equal(t, 2, len(res))                             // have first two events
	require.Equal(t, event1Data, rlp.RawValue(res[0].Data())) // check data fields
	require.Equal(t, event2Data, rlp.RawValue(res[1].Data()))

	// get non-sprint block
	res, err = b.GetEvents(ctx, 2)
	require.Error(t, err)

	wg.Done()
}
