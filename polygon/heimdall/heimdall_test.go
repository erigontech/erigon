package heimdall

import (
	"context"
	"encoding/json"
	"math/big"
	"testing"
	"time"

	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon/crypto"
)

func makeCheckpoint(start uint64, len uint) *Checkpoint {
	return &Checkpoint{
		Fields: WaypointFields{
			StartBlock: new(big.Int).SetUint64(start),
			EndBlock:   new(big.Int).SetUint64(start + uint64(len) - 1),
			RootHash:   libcommon.BytesToHash(crypto.Keccak256([]byte("ROOT"))),
			Timestamp:  uint64(time.Now().Unix()),
		},
	}
}

func makeMilestone(start uint64, len uint) *Milestone {
	m := Milestone{
		Fields: WaypointFields{
			StartBlock: new(big.Int).SetUint64(start),
			EndBlock:   new(big.Int).SetUint64(start + uint64(len) - 1),
			RootHash:   libcommon.BytesToHash(crypto.Keccak256([]byte("ROOT"))),
			Timestamp:  uint64(time.Now().Unix()),
		},
	}
	return &m
}

type heimdallTest struct {
	ctx      context.Context
	client   *MockHeimdallClient
	heimdall Heimdall
	logger   log.Logger
	store    *MockStore
}

func newHeimdallTest(t *testing.T) heimdallTest {
	logger := log.New()
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	client := NewMockHeimdallClient(ctrl)
	heimdall := NewHeimdall(client, logger)
	store := NewMockStore(ctrl)

	return heimdallTest{
		ctx,
		client,
		heimdall,
		logger,
		store,
	}
}

func (test heimdallTest) setupCheckpoints(count int) []*Checkpoint {
	var expectedCheckpoints []*Checkpoint
	for i := 0; i < count; i++ {
		c := makeCheckpoint(uint64(i*256), 256)
		expectedCheckpoints = append(expectedCheckpoints, c)
	}

	client := test.client

	client.EXPECT().FetchCheckpointCount(gomock.Any()).Return(int64(len(expectedCheckpoints)), nil)

	if count < checkpointsBatchFetchThreshold {
		client.EXPECT().
			FetchCheckpoint(gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, number int64) (*Checkpoint, error) {
				return expectedCheckpoints[number-1], nil
			}).
			AnyTimes()
	} else {
		client.EXPECT().
			FetchCheckpoints(gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, page uint64, limit uint64) (Checkpoints, error) {
				if page == 0 {
					return nil, nil
				}

				limit = cmp.Min(10, limit)
				l := (page - 1) * limit
				r := page * limit
				return expectedCheckpoints[l:r], nil
			}).
			AnyTimes()
	}

	// this is a dummy store
	test.store.EXPECT().
		LastCheckpointId(gomock.Any()).Return(CheckpointId(0), false, nil).AnyTimes()

	test.store.EXPECT().
		PutCheckpoint(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, checkpointId CheckpointId, checkpoint *Checkpoint) error {
			return nil
		}).AnyTimes()

	return expectedCheckpoints
}

func (test heimdallTest) setupMilestones(count int) []*Milestone {
	var expectedMilestones []*Milestone
	for i := 0; i < count; i++ {
		m := makeMilestone(uint64(i*16), 16)
		expectedMilestones = append(expectedMilestones, m)
	}

	client := test.client
	client.EXPECT().FetchMilestoneCount(gomock.Any()).Return(int64(len(expectedMilestones)), nil)
	client.EXPECT().FetchMilestone(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, number int64) (*Milestone, error) {
		return expectedMilestones[number-1], nil
	}).AnyTimes()

	// this is a dummy store
	test.store.EXPECT().
		LastMilestoneId(gomock.Any()).Return(MilestoneId(0), false, nil).AnyTimes()
	test.store.EXPECT().
		PutMilestone(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, milestoneId MilestoneId, milestone *Milestone) error {
			return nil
		}).AnyTimes()

	return expectedMilestones
}

func TestFetchCheckpoints1(t *testing.T) {
	test := newHeimdallTest(t)
	expectedCheckpoint := test.setupCheckpoints(1)[0]

	checkpoints, err := test.heimdall.FetchCheckpointsFromBlock(test.ctx, test.store, 0)
	require.Nil(t, err)

	require.Equal(t, 1, len(checkpoints))
	assert.Equal(t, expectedCheckpoint.Timestamp(), checkpoints[0].Timestamp())
}

func TestFetchCheckpointsPastLast(t *testing.T) {
	test := newHeimdallTest(t)
	_ = test.setupCheckpoints(1)[0]

	checkpoints, err := test.heimdall.FetchCheckpointsFromBlock(test.ctx, test.store, 500)
	require.Nil(t, err)

	require.Equal(t, 0, len(checkpoints))
}

func TestFetchCheckpoints10(t *testing.T) {
	test := newHeimdallTest(t)
	expectedCheckpoints := test.setupCheckpoints(10)

	checkpoints, err := test.heimdall.FetchCheckpointsFromBlock(test.ctx, test.store, 0)
	require.Nil(t, err)

	require.Equal(t, len(expectedCheckpoints), len(checkpoints))
	for i := 0; i < len(checkpoints); i++ {
		assert.Equal(t, expectedCheckpoints[i].StartBlock().Uint64(), checkpoints[i].StartBlock().Uint64())
	}
}

func TestFetchCheckpoints100(t *testing.T) {
	test := newHeimdallTest(t)
	expectedCheckpoints := test.setupCheckpoints(100)

	checkpoints, err := test.heimdall.FetchCheckpointsFromBlock(test.ctx, test.store, 0)
	require.Nil(t, err)

	require.Equal(t, len(expectedCheckpoints), len(checkpoints))
	for i := 0; i < len(checkpoints); i++ {
		assert.Equal(t, expectedCheckpoints[i].StartBlock().Uint64(), checkpoints[i].StartBlock().Uint64())
	}
}

func TestFetchCheckpointsMiddleStart(t *testing.T) {
	test := newHeimdallTest(t)
	expectedCheckpoints := test.setupCheckpoints(10)
	const offset = 6

	checkpoints, err := test.heimdall.FetchCheckpointsFromBlock(test.ctx, test.store, expectedCheckpoints[offset].StartBlock().Uint64())
	require.Nil(t, err)

	require.Equal(t, len(expectedCheckpoints)-offset, len(checkpoints))
	for i := 0; i < len(checkpoints); i++ {
		assert.Equal(t, expectedCheckpoints[offset+i].StartBlock().Uint64(), checkpoints[i].StartBlock().Uint64())
	}
}

func TestFetchMilestones1(t *testing.T) {
	test := newHeimdallTest(t)
	expectedMilestone := test.setupMilestones(1)[0]

	milestones, err := test.heimdall.FetchMilestonesFromBlock(test.ctx, test.store, 0)
	require.Nil(t, err)

	require.Equal(t, 1, len(milestones))
	assert.Equal(t, expectedMilestone.Timestamp(), milestones[0].Timestamp())
}

func TestFetchMilestonesPastLast(t *testing.T) {
	test := newHeimdallTest(t)
	_ = test.setupMilestones(1)[0]

	milestones, err := test.heimdall.FetchMilestonesFromBlock(test.ctx, test.store, 500)
	require.Nil(t, err)

	require.Equal(t, 0, len(milestones))
}

func TestFetchMilestones10(t *testing.T) {
	test := newHeimdallTest(t)
	expectedMilestones := test.setupMilestones(10)

	milestones, err := test.heimdall.FetchMilestonesFromBlock(test.ctx, test.store, 0)
	require.Nil(t, err)

	require.Equal(t, len(expectedMilestones), len(milestones))
	for i := 0; i < len(milestones); i++ {
		assert.Equal(t, expectedMilestones[i].StartBlock().Uint64(), milestones[i].StartBlock().Uint64())
	}
}

func TestFetchMilestonesMiddleStart(t *testing.T) {
	test := newHeimdallTest(t)
	expectedMilestones := test.setupMilestones(10)
	const offset = 6

	milestones, err := test.heimdall.FetchMilestonesFromBlock(test.ctx, test.store, expectedMilestones[offset].StartBlock().Uint64())
	require.Nil(t, err)

	require.Equal(t, len(expectedMilestones)-offset, len(milestones))
	for i := 0; i < len(milestones); i++ {
		assert.Equal(t, expectedMilestones[offset+i].StartBlock().Uint64(), milestones[i].StartBlock().Uint64())
	}
}

func TestFetchMilestonesStartingBeforeEvictionPoint(t *testing.T) {
	test := newHeimdallTest(t)

	var expectedMilestones []*Milestone
	for i := 0; i < 20; i++ {
		m := makeMilestone(uint64(i*16), 16)
		expectedMilestones = append(expectedMilestones, m)
	}
	const keptMilestones = 5

	client := test.client
	client.EXPECT().FetchMilestoneCount(gomock.Any()).Return(int64(len(expectedMilestones)), nil)
	client.EXPECT().FetchMilestone(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, number int64) (*Milestone, error) {
		if int(number) <= len(expectedMilestones)-keptMilestones {
			return nil, ErrNotInMilestoneList
		}
		return expectedMilestones[number-1], nil
	}).AnyTimes()

	// this is a dummy store
	test.store.EXPECT().
		LastMilestoneId(gomock.Any()).Return(MilestoneId(0), false, nil).AnyTimes()
	test.store.EXPECT().
		PutMilestone(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, milestoneId MilestoneId, milestone *Milestone) error {
			return nil
		}).AnyTimes()

	milestones, err := test.heimdall.FetchMilestonesFromBlock(test.ctx, test.store, 0)
	require.NotNil(t, err)
	require.ErrorIs(t, err, ErrIncompleteMilestoneRange)

	require.Equal(t, keptMilestones, len(milestones))
	for i := 0; i < len(milestones); i++ {
		assert.Equal(t, expectedMilestones[len(expectedMilestones)-len(milestones)+i].StartBlock().Uint64(), milestones[i].StartBlock().Uint64())
	}
}

func TestOnMilestoneEvent(t *testing.T) {
	test := newHeimdallTest(t)

	var cancel context.CancelFunc
	test.ctx, cancel = context.WithCancel(test.ctx)
	defer cancel()

	client := test.client
	count := int64(1)
	client.EXPECT().FetchMilestoneCount(gomock.Any()).DoAndReturn(func(ctx context.Context) (int64, error) {
		c := count
		if c == 2 {
			cancel()
			return 0, ctx.Err()
		}
		count++
		return c, nil
	}).AnyTimes()

	expectedMilestone := makeMilestone(0, 12)

	client.EXPECT().
		FetchMilestone(gomock.Any(), gomock.Any()).Return(expectedMilestone, nil).AnyTimes()

	lastMilestoneId := MilestoneId(0)
	test.store.EXPECT().
		LastMilestoneId(gomock.Any()).Return(lastMilestoneId, true, nil).AnyTimes()
	test.store.EXPECT().
		PutMilestone(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, milestoneId MilestoneId, milestone *Milestone) error {
			lastMilestoneId = milestoneId
			return nil
		}).AnyTimes()

	eventChan := make(chan *Milestone)
	err := test.heimdall.OnMilestoneEvent(test.ctx, test.store, func(m *Milestone) {
		eventChan <- m
	})
	require.Nil(t, err)

	m := <-eventChan
	assert.Equal(t, expectedMilestone.Timestamp(), m.Timestamp())
}

func TestMarshall(t *testing.T) {
	m := makeMilestone(10, 100)

	b, err := json.Marshal(m)

	assert.Nil(t, err)

	var m1 Milestone

	assert.Nil(t, json.Unmarshal(b, &m1))

	assert.Equal(t, *m, m1)

	c := makeCheckpoint(10, 100)

	b, err = json.Marshal(c)

	assert.Nil(t, err)

	var c1 Checkpoint

	assert.Nil(t, json.Unmarshal(b, &c1))

	assert.Equal(t, *c, c1)
}
