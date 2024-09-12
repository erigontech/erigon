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

package heimdall

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	"github.com/erigontech/erigon/turbo/testlog"
)

const (
	testDataDir                 = "testdata/amoy"
	spanTestDataDir             = testDataDir + "/spans"
	checkpointsTestDataDir      = testDataDir + "/checkpoints"
	milestonesTestDataDir       = testDataDir + "/milestones"
	proposerSequenceTestDataDir = testDataDir + "/getSnapshotProposerSequence"
)

func TestService(t *testing.T) {
	suite.Run(t, new(ServiceTestSuite))
}

type ServiceTestSuite struct {
	suite.Suite
	ctx                context.Context
	cancel             context.CancelFunc
	eg                 errgroup.Group
	client             *MockHeimdallClient
	service            *service
	observedMilestones []*Milestone
	observedSpans      []*Span
}

func (suite *ServiceTestSuite) SetupSuite() {
	ctrl := gomock.NewController(suite.T())
	tempDir := suite.T().TempDir()
	dataDir := fmt.Sprintf("%s/datadir", tempDir)
	logger := testlog.Logger(suite.T(), log.LvlCrit)
	store := NewMdbxServiceStore(logger, dataDir, tempDir)
	borConfig := params.AmoyChainConfig.Bor.(*borcfg.BorConfig)
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.client = NewMockHeimdallClient(ctrl)
	suite.setupSpans()
	suite.setupCheckpoints()
	suite.setupMilestones()
	reader := NewReader(borConfig.CalculateSprintNumber, store, logger)
	suite.service = newService(borConfig.CalculateSprintNumber, suite.client, store, logger, reader)

	err := suite.service.store.Prepare(suite.ctx)
	require.NoError(suite.T(), err)

	suite.service.RegisterMilestoneObserver(func(milestone *Milestone) {
		suite.observedMilestones = append(suite.observedMilestones, milestone)
	})

	suite.service.RegisterSpanObserver(func(span *Span) {
		suite.observedSpans = append(suite.observedSpans, span)
	})

	suite.eg.Go(func() error {
		return suite.service.Run(suite.ctx)
	})

	err = suite.service.SynchronizeMilestones(suite.ctx)
	require.NoError(suite.T(), err)
	err = suite.service.SynchronizeCheckpoints(suite.ctx)
	require.NoError(suite.T(), err)
	err = suite.service.SynchronizeSpans(suite.ctx, math.MaxInt)
	require.NoError(suite.T(), err)
}

func (suite *ServiceTestSuite) TearDownSuite() {
	suite.cancel()
	err := suite.eg.Wait()
	require.ErrorIs(suite.T(), err, context.Canceled)
}

func (suite *ServiceTestSuite) TestMilestones() {
	ctx := suite.ctx
	t := suite.T()
	svc := suite.service

	id, ok, err := svc.store.Milestones().LastEntityId(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(285641), id)

	for id := uint64(285542); id <= 285641; id++ {
		entity, ok, err := svc.store.Milestones().Entity(ctx, id)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, id, uint64(entity.Id))
	}
}

func (suite *ServiceTestSuite) TestRegisterMilestoneObserver() {
	require.Len(suite.T(), suite.observedMilestones, 100)
}

func (suite *ServiceTestSuite) TestCheckpoints() {
	ctx := suite.ctx
	t := suite.T()
	svc := suite.service

	id, ok, err := svc.store.Checkpoints().LastEntityId(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(150), id)

	for id := uint64(1); id <= 150; id++ {
		entity, ok, err := svc.store.Checkpoints().Entity(ctx, id)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, id, uint64(entity.Id))
	}
}

func (suite *ServiceTestSuite) TestSpans() {
	ctx := suite.ctx
	t := suite.T()
	svc := suite.service

	id, ok, err := svc.store.Spans().LastEntityId(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1280), id)

	for id := uint64(0); id <= 1280; id++ {
		entity, ok, err := svc.store.Spans().Entity(ctx, id)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, id, uint64(entity.Id))
	}
}

func (suite *ServiceTestSuite) TestRegisterSpanObserver() {
	require.Len(suite.T(), suite.observedSpans, 1281)
}

func (suite *ServiceTestSuite) TestProducers() {
	// span 0
	suite.producersSubTest(1)   // start
	suite.producersSubTest(255) // end
	// span 167
	suite.producersSubTest(1062656) // start
	suite.producersSubTest(1069055) // end
	// span 168 - first span that has changes to selected producers
	suite.producersSubTest(1069056) // start
	suite.producersSubTest(1072256) // middle
	suite.producersSubTest(1075455) // end
	// span 169
	suite.producersSubTest(1075456) // start
	suite.producersSubTest(1081855) // end
	// span 182 - second span that has changes to selected producers
	suite.producersSubTest(1158656) // start
	suite.producersSubTest(1165055) // end
	// span 1279
	suite.producersSubTest(8179456) // start
	suite.producersSubTest(8185855) // end
	// span 1280 - span where we discovered the need for this API
	suite.producersSubTest(8185856) // start
	suite.producersSubTest(8187309) // middle where we discovered error
	suite.producersSubTest(8192255) // end
}

func (suite *ServiceTestSuite) producersSubTest(blockNum uint64) {
	suite.Run(fmt.Sprintf("%d", blockNum), func() {
		t := suite.T()
		ctx := suite.ctx
		svc := suite.service

		b, err := os.ReadFile(fmt.Sprintf("%s/blockNum_%d.json", proposerSequenceTestDataDir, blockNum))
		require.NoError(t, err)
		var proposerSequenceResponse getSnapshotProposerSequenceResponse
		err = json.Unmarshal(b, &proposerSequenceResponse)
		require.NoError(t, err)
		want := proposerSequenceResponse.Result

		producers, err := svc.Producers(ctx, blockNum)
		require.NoError(t, err)

		errInfoMsgArgs := []interface{}{"want: %v\nhave: %v\n", want, producers}
		require.Equal(t, len(want.Signers), len(producers.Validators), errInfoMsgArgs...)
		for _, signer := range want.Signers {
			_, producer := producers.GetByAddress(signer.Signer)
			producerDifficulty, err := producers.Difficulty(producer.Address)
			require.NoError(t, err)
			require.Equal(t, signer.Difficulty, producerDifficulty, errInfoMsgArgs...)
		}
	})
}

func (suite *ServiceTestSuite) setupSpans() {
	files, err := dir.ReadDir(spanTestDataDir)
	require.NoError(suite.T(), err)

	slices.SortFunc(files, func(a, b os.DirEntry) int {
		idA := extractIdFromFileName(suite.T(), a.Name(), "span")
		idB := extractIdFromFileName(suite.T(), b.Name(), "span")
		return cmp.Compare(idA, idB)
	})

	// leave few files for sequential fetch
	sequentialFetchFileCount := 3
	lastSpanFileNameForSequentialFetch := files[len(files)-1].Name()
	lastSpanFileNameForBatchFetch := files[len(files)-1-sequentialFetchFileCount].Name()
	batchFetchSpanFiles := files[:len(files)-sequentialFetchFileCount]

	gomock.InOrder(
		suite.client.EXPECT().
			FetchLatestSpan(gomock.Any()).
			DoAndReturn(func(ctx context.Context) (*Span, error) {
				return readEntityFromFile[Span](
					suite.T(),
					fmt.Sprintf("%s/%s", spanTestDataDir, lastSpanFileNameForBatchFetch),
				), nil
			}).
			Times(1),
		suite.client.EXPECT().
			FetchLatestSpan(gomock.Any()).
			DoAndReturn(func(ctx context.Context) (*Span, error) {
				return readEntityFromFile[Span](
					suite.T(),
					fmt.Sprintf("%s/%s", spanTestDataDir, lastSpanFileNameForSequentialFetch),
				), nil
			}).
			AnyTimes(),
	)

	suite.client.EXPECT().
		FetchSpans(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, page, limit uint64) ([]*Span, error) {
			spans := make([]*Span, 0, limit)
			startIdx := (page - 1) * limit
			endIdx := min(startIdx+limit, uint64(len(batchFetchSpanFiles)))
			for i := startIdx; i < endIdx; i++ {
				span := readEntityFromFile[Span](suite.T(), fmt.Sprintf("%s/span_%d.json", spanTestDataDir, i))
				spans = append(spans, span)
			}
			return spans, nil
		}).
		Times(1 + (len(files)+len(files)%SpansFetchLimit)/SpansFetchLimit)

	suite.client.EXPECT().
		FetchSpan(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, id uint64) (*Span, error) {
			return readEntityFromFile[Span](suite.T(), fmt.Sprintf("%s/span_%d.json", spanTestDataDir, id)), nil
		}).
		Times(sequentialFetchFileCount)
}

func (suite *ServiceTestSuite) setupCheckpoints() {
	files, err := dir.ReadDir(checkpointsTestDataDir)
	require.NoError(suite.T(), err)

	checkpoints := make(Checkpoints, len(files))
	for i, file := range files {
		checkpoints[i] = readEntityFromFile[Checkpoint](
			suite.T(),
			fmt.Sprintf("%s/%s", checkpointsTestDataDir, file.Name()),
		)
	}

	sort.Sort(checkpoints)

	// leave few files for sequential fetch
	sequentialFetchCheckpointsCount := 3
	batchFetchCheckpointsCount := len(checkpoints) - sequentialFetchCheckpointsCount

	gomock.InOrder(
		suite.client.EXPECT().
			FetchCheckpointCount(gomock.Any()).
			Return(int64(batchFetchCheckpointsCount), nil).
			Times(1),
		suite.client.EXPECT().
			FetchCheckpointCount(gomock.Any()).
			Return(int64(len(files)), nil).
			AnyTimes(),
	)

	gomock.InOrder(
		suite.client.EXPECT().
			FetchCheckpoints(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(checkpoints, nil).
			Times(1),
		suite.client.EXPECT().
			FetchCheckpoints(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil, nil).
			Times(1),
	)

	suite.client.EXPECT().
		FetchCheckpoint(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, id int64) (*Checkpoint, error) { return checkpoints[id-1], nil }).
		Times(sequentialFetchCheckpointsCount)
}

func (suite *ServiceTestSuite) setupMilestones() {
	files, err := dir.ReadDir(milestonesTestDataDir)
	require.NoError(suite.T(), err)

	slices.SortFunc(files, func(a, b os.DirEntry) int {
		idA := extractIdFromFileName(suite.T(), a.Name(), "milestone")
		idB := extractIdFromFileName(suite.T(), b.Name(), "milestone")
		return cmp.Compare(idA, idB)
	})

	firstMilestoneId := extractIdFromFileName(suite.T(), files[0].Name(), "milestone")
	lastMilestoneId := extractIdFromFileName(suite.T(), files[len(files)-1].Name(), "milestone")

	suite.client.EXPECT().
		FetchMilestoneCount(gomock.Any()).
		Return(lastMilestoneId, nil).
		AnyTimes()

	suite.client.EXPECT().
		FetchFirstMilestoneNum(gomock.Any()).
		Return(firstMilestoneId, nil).
		AnyTimes()

	suite.client.EXPECT().
		FetchMilestone(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, id int64) (*Milestone, error) {
			milestone := readEntityFromFile[Milestone](
				suite.T(),
				fmt.Sprintf("%s/milestone_%d.json", milestonesTestDataDir, id),
			)
			return milestone, nil
		}).
		AnyTimes()
}

func extractIdFromFileName(t *testing.T, fileName string, entityType string) int64 {
	r := regexp.MustCompile(fmt.Sprintf("%s_([0-9]+).json", entityType))
	match := r.FindStringSubmatch(fileName)
	id, err := strconv.ParseInt(match[1], 10, 64)
	require.NoError(t, err)
	return id
}

func readEntityFromFile[T any](t *testing.T, filePath string) *T {
	bytes, err := os.ReadFile(filePath)
	require.NoError(t, err)
	var entity T
	err = json.Unmarshal(bytes, &entity)
	require.NoError(t, err)
	return &entity
}

// getSnapshotProposerSequenceResponse is reflecting the result from BOR API bor_getSnapshotProposerSequence for testing
type getSnapshotProposerSequenceResponse struct {
	Result proposerSequenceResult `json:"result"`
}

type proposerSequenceResult struct {
	Signers []difficultiesKV
}

type difficultiesKV struct {
	Signer     common.Address
	Difficulty uint64
}
