package polygon_sync

import (
	"context"
	"github.com/golang/mock/gomock"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/checkpoint"
	heimdall_mock "github.com/ledgerwatch/erigon/consensus/bor/heimdall/mock"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/assert"
	"math/big"
	"testing"
	"time"
)
import "github.com/stretchr/testify/require"

func makeCheckpoint(start uint64, len uint) *checkpoint.Checkpoint {
	c := checkpoint.Checkpoint{
		StartBlock: new(big.Int).SetUint64(start),
		EndBlock:   new(big.Int).SetUint64(start + uint64(len) - 1),
		Timestamp:  uint64(time.Now().Unix()),
	}
	return &c
}

type fetchCheckpointsTest struct {
	ctx      context.Context
	client   *heimdall_mock.MockIHeimdallClient
	heimdall Heimdall
	logger   log.Logger
}

func newFetchCheckpointsTest(t *testing.T) fetchCheckpointsTest {
	logger := log.New()
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	client := heimdall_mock.NewMockIHeimdallClient(ctrl)
	heimdall := NewHeimdall(client, logger)

	return fetchCheckpointsTest{
		ctx,
		client,
		heimdall,
		logger,
	}
}

func (test fetchCheckpointsTest) setup1() *checkpoint.Checkpoint {
	return test.setup(1)[0]
}

func (test fetchCheckpointsTest) setup(count int) []*checkpoint.Checkpoint {
	var expectedCheckpoints []*checkpoint.Checkpoint
	for i := 0; i < count; i++ {
		c := makeCheckpoint(uint64(i*256), 256)
		expectedCheckpoints = append(expectedCheckpoints, c)
	}

	client := test.client
	client.EXPECT().FetchCheckpointCount(gomock.Any()).Return(int64(len(expectedCheckpoints)), nil)
	client.EXPECT().FetchCheckpoint(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, number int64) (*checkpoint.Checkpoint, error) {
		return expectedCheckpoints[number-1], nil
	}).AnyTimes()

	return expectedCheckpoints
}

func TestFetchCheckpoints1(t *testing.T) {
	test := newFetchCheckpointsTest(t)
	expectedCheckpoint := test.setup1()

	checkpoints, err := test.heimdall.FetchCheckpoints(test.ctx, 0)
	require.Nil(t, err)

	require.Equal(t, 1, len(checkpoints))
	assert.Equal(t, expectedCheckpoint.Timestamp, checkpoints[0].Timestamp)
}

func TestFetchCheckpointsPastLast(t *testing.T) {
	test := newFetchCheckpointsTest(t)
	_ = test.setup1()

	checkpoints, err := test.heimdall.FetchCheckpoints(test.ctx, 500)
	require.Nil(t, err)

	require.Equal(t, 0, len(checkpoints))
}

func TestFetchCheckpoints10(t *testing.T) {
	test := newFetchCheckpointsTest(t)
	expectedCheckpoints := test.setup(10)

	checkpoints, err := test.heimdall.FetchCheckpoints(test.ctx, 0)
	require.Nil(t, err)

	require.Equal(t, len(expectedCheckpoints), len(checkpoints))
	for i := 0; i < len(checkpoints); i++ {
		assert.Equal(t, expectedCheckpoints[i].StartBlock.Uint64(), checkpoints[i].StartBlock.Uint64())
	}
}

func TestFetchCheckpointsMiddleStart(t *testing.T) {
	test := newFetchCheckpointsTest(t)
	expectedCheckpoints := test.setup(10)
	const offset = 6

	checkpoints, err := test.heimdall.FetchCheckpoints(test.ctx, expectedCheckpoints[offset].StartBlock.Uint64())
	require.Nil(t, err)

	require.Equal(t, len(expectedCheckpoints)-offset, len(checkpoints))
	for i := 0; i < len(checkpoints); i++ {
		assert.Equal(t, expectedCheckpoints[offset+i].StartBlock.Uint64(), checkpoints[i].StartBlock.Uint64())
	}
}
