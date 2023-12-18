package polygon_sync

import (
	"context"
	"github.com/golang/mock/gomock"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/checkpoint"
	heimdall_mock "github.com/ledgerwatch/erigon/consensus/bor/heimdall/mock"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)
import "github.com/stretchr/testify/require"

func TestCheckpointNumContainingBlockNum(t *testing.T) {
	// TODO: implement
	assert.Equal(t, int64(1), checkpointNumContainingBlockNum(0))
}

func makeCheckpoint() *checkpoint.Checkpoint {
	c := checkpoint.Checkpoint{
		Timestamp: uint64(time.Now().Unix()),
	}
	return &c
}

func TestFetchCheckpoints1(t *testing.T) {
	logger := log.New()
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := heimdall_mock.NewMockIHeimdallClient(ctrl)
	heimdall := NewHeimdall(client, logger)

	expectedCheckpoint := makeCheckpoint()
	client.EXPECT().FetchCheckpointCount(gomock.Any()).Return(int64(1), nil)
	client.EXPECT().FetchCheckpoint(gomock.Any(), int64(1)).Return(expectedCheckpoint, nil)

	checkpoints, err := heimdall.FetchCheckpoints(ctx, 0)
	require.Nil(t, err)

	require.Equal(t, 1, len(checkpoints))
	assert.Equal(t, expectedCheckpoint.Timestamp, checkpoints[0].Timestamp)
}
