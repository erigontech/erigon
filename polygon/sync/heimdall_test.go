package sync

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	heimdallclient "github.com/ledgerwatch/erigon/polygon/heimdall"
)

func makeCheckpoint(start uint64, len uint) *heimdallclient.Checkpoint {
	c := heimdallclient.Checkpoint{
		StartBlock: new(big.Int).SetUint64(start),
		EndBlock:   new(big.Int).SetUint64(start + uint64(len) - 1),
		Timestamp:  uint64(time.Now().Unix()),
	}
	return &c
}

func makeMilestone(start uint64, len uint) *heimdallclient.Milestone {
	m := heimdallclient.Milestone{
		StartBlock: new(big.Int).SetUint64(start),
		EndBlock:   new(big.Int).SetUint64(start + uint64(len) - 1),
		Timestamp:  uint64(time.Now().Unix()),
	}
	return &m
}

type heimdallTest struct {
	ctx      context.Context
	client   *heimdallclient.MockHeimdallClient
	heimdall Heimdall
	logger   log.Logger
}

func newHeimdallTest(t *testing.T) heimdallTest {
	logger := log.New()
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	client := heimdallclient.NewMockHeimdallClient(ctrl)
	heimdall := NewHeimdall(client, logger)

	return heimdallTest{
		ctx,
		client,
		heimdall,
		logger,
	}
}

func (test heimdallTest) setupCheckpoints(count int) []*heimdallclient.Checkpoint {
	var expectedCheckpoints []*heimdallclient.Checkpoint
	for i := 0; i < count; i++ {
		c := makeCheckpoint(uint64(i*256), 256)
		expectedCheckpoints = append(expectedCheckpoints, c)
	}

	client := test.client
	client.EXPECT().FetchCheckpointCount(gomock.Any()).Return(int64(len(expectedCheckpoints)), nil)
	client.EXPECT().FetchCheckpoint(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, number int64) (*heimdallclient.Checkpoint, error) {
		return expectedCheckpoints[number-1], nil
	}).AnyTimes()

	return expectedCheckpoints
}

func (test heimdallTest) setupMilestones(count int) []*heimdallclient.Milestone {
	var expectedMilestones []*heimdallclient.Milestone
	for i := 0; i < count; i++ {
		m := makeMilestone(uint64(i*16), 16)
		expectedMilestones = append(expectedMilestones, m)
	}

	client := test.client
	client.EXPECT().FetchMilestoneCount(gomock.Any()).Return(int64(len(expectedMilestones)), nil)
	client.EXPECT().FetchMilestone(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, number int64) (*heimdallclient.Milestone, error) {
		return expectedMilestones[number-1], nil
	}).AnyTimes()

	return expectedMilestones
}

func TestFetchCheckpoints1(t *testing.T) {
	test := newHeimdallTest(t)
	expectedCheckpoint := test.setupCheckpoints(1)[0]

	checkpoints, err := test.heimdall.FetchCheckpoints(test.ctx, 0)
	require.Nil(t, err)

	require.Equal(t, 1, len(checkpoints))
	assert.Equal(t, expectedCheckpoint.Timestamp, checkpoints[0].Timestamp)
}

func TestFetchCheckpointsPastLast(t *testing.T) {
	test := newHeimdallTest(t)
	_ = test.setupCheckpoints(1)[0]

	checkpoints, err := test.heimdall.FetchCheckpoints(test.ctx, 500)
	require.Nil(t, err)

	require.Equal(t, 0, len(checkpoints))
}

func TestFetchCheckpoints10(t *testing.T) {
	test := newHeimdallTest(t)
	expectedCheckpoints := test.setupCheckpoints(10)

	checkpoints, err := test.heimdall.FetchCheckpoints(test.ctx, 0)
	require.Nil(t, err)

	require.Equal(t, len(expectedCheckpoints), len(checkpoints))
	for i := 0; i < len(checkpoints); i++ {
		assert.Equal(t, expectedCheckpoints[i].StartBlock.Uint64(), checkpoints[i].StartBlock.Uint64())
	}
}

func TestFetchCheckpointsMiddleStart(t *testing.T) {
	test := newHeimdallTest(t)
	expectedCheckpoints := test.setupCheckpoints(10)
	const offset = 6

	checkpoints, err := test.heimdall.FetchCheckpoints(test.ctx, expectedCheckpoints[offset].StartBlock.Uint64())
	require.Nil(t, err)

	require.Equal(t, len(expectedCheckpoints)-offset, len(checkpoints))
	for i := 0; i < len(checkpoints); i++ {
		assert.Equal(t, expectedCheckpoints[offset+i].StartBlock.Uint64(), checkpoints[i].StartBlock.Uint64())
	}
}

func TestFetchMilestones1(t *testing.T) {
	test := newHeimdallTest(t)
	expectedMilestone := test.setupMilestones(1)[0]

	milestones, err := test.heimdall.FetchMilestones(test.ctx, 0)
	require.Nil(t, err)

	require.Equal(t, 1, len(milestones))
	assert.Equal(t, expectedMilestone.Timestamp, milestones[0].Timestamp)
}

func TestFetchMilestonesPastLast(t *testing.T) {
	test := newHeimdallTest(t)
	_ = test.setupMilestones(1)[0]

	milestones, err := test.heimdall.FetchMilestones(test.ctx, 500)
	require.Nil(t, err)

	require.Equal(t, 0, len(milestones))
}

func TestFetchMilestones10(t *testing.T) {
	test := newHeimdallTest(t)
	expectedMilestones := test.setupMilestones(10)

	milestones, err := test.heimdall.FetchMilestones(test.ctx, 0)
	require.Nil(t, err)

	require.Equal(t, len(expectedMilestones), len(milestones))
	for i := 0; i < len(milestones); i++ {
		assert.Equal(t, expectedMilestones[i].StartBlock.Uint64(), milestones[i].StartBlock.Uint64())
	}
}

func TestFetchMilestonesMiddleStart(t *testing.T) {
	test := newHeimdallTest(t)
	expectedMilestones := test.setupMilestones(10)
	const offset = 6

	milestones, err := test.heimdall.FetchMilestones(test.ctx, expectedMilestones[offset].StartBlock.Uint64())
	require.Nil(t, err)

	require.Equal(t, len(expectedMilestones)-offset, len(milestones))
	for i := 0; i < len(milestones); i++ {
		assert.Equal(t, expectedMilestones[offset+i].StartBlock.Uint64(), milestones[i].StartBlock.Uint64())
	}
}

func TestFetchMilestonesStartingBeforeEvictionPoint(t *testing.T) {
	test := newHeimdallTest(t)

	var expectedMilestones []*heimdallclient.Milestone
	for i := 0; i < 20; i++ {
		m := makeMilestone(uint64(i*16), 16)
		expectedMilestones = append(expectedMilestones, m)
	}
	const keptMilestones = 5

	client := test.client
	client.EXPECT().FetchMilestoneCount(gomock.Any()).Return(int64(len(expectedMilestones)), nil)
	client.EXPECT().FetchMilestone(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, number int64) (*heimdallclient.Milestone, error) {
		if int(number) <= len(expectedMilestones)-keptMilestones {
			return nil, heimdallclient.ErrNotInMilestoneList
		}
		return expectedMilestones[number-1], nil
	}).AnyTimes()

	milestones, err := test.heimdall.FetchMilestones(test.ctx, 0)
	require.NotNil(t, err)
	require.ErrorIs(t, err, ErrIncompleteMilestoneRange)

	require.Equal(t, keptMilestones, len(milestones))
	for i := 0; i < len(milestones); i++ {
		assert.Equal(t, expectedMilestones[len(expectedMilestones)-len(milestones)+i].StartBlock.Uint64(), milestones[i].StartBlock.Uint64())
	}
}

func TestOnMilestoneEvent(t *testing.T) {
	test := newHeimdallTest(t)

	var cancel context.CancelFunc
	test.ctx, cancel = context.WithCancel(test.ctx)
	defer cancel()

	client := test.client
	count := new(int64)
	client.EXPECT().FetchMilestoneCount(gomock.Any()).DoAndReturn(func(ctx context.Context) (int64, error) {
		c := *count
		if c == 2 {
			cancel()
			return 0, ctx.Err()
		}
		*count += 1
		return c, nil
	}).AnyTimes()

	expectedMilestone := makeMilestone(0, 12)
	client.EXPECT().FetchMilestone(gomock.Any(), gomock.Any()).Return(expectedMilestone, nil)

	eventChan := make(chan *heimdallclient.Milestone)
	err := test.heimdall.OnMilestoneEvent(test.ctx, func(m *heimdallclient.Milestone) {
		eventChan <- m
	})
	require.Nil(t, err)

	m := <-eventChan
	assert.Equal(t, expectedMilestone.Timestamp, m.Timestamp)
}
