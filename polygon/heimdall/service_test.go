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
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/testlog"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	polychain "github.com/erigontech/erigon/polygon/chain"
)

func TestServiceWithAmoyData(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	suite.Run(t, &ServiceTestSuite{
		testDataDir:                    "testdata/amoy",
		chainConfig:                    polychain.Amoy.Config,
		expectedLastSpan:               1280,
		expectedFirstCheckpoint:        1,
		expectedLastCheckpoint:         150,
		expectedFirstMilestone:         285542,
		expectedLastMilestone:          285641,
		entityNumberToCheckBeforeFirst: 2,
		producersApiBlocksToTest: []uint64{
			// span 0
			1,   // start
			255, // end
			// span 167
			1_062_656, // start
			1_069_055, // end
			// span 168 - first span that has changes to selected producers
			1_069_056, // start
			1_072_256, // middle
			1_075_455, // end
			// span 169
			1_075_456, // start
			1_081_855, // end
			// span 182 - second span that has changes to selected producers
			1_158_656, // start
			1_165_055, // end
			// span 1279
			8_179_456, // start
			8_185_855, // end
			// span 1280 - span where we discovered the need for this API
			8_185_856, // start
			8_187_309, // middle where we discovered error
			8_192_255, // end
		},
	})
}

func TestServiceWithMainnetData(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	suite.Run(t, &ServiceTestSuite{
		testDataDir:                    "testdata/mainnet",
		chainConfig:                    polychain.BorMainnet.Config,
		expectedLastSpan:               2344,
		expectedFirstCheckpoint:        1,
		expectedLastCheckpoint:         1,
		expectedFirstMilestone:         453496,
		expectedLastMilestone:          453496,
		entityNumberToCheckBeforeFirst: 2,
		producersApiBlocksToTest: []uint64{
			1,
			16,
			255,
			256,
			7_000,
			8_173_056,
			8_192_255,
			10_000_000,
			12_000_000,
			13_000_000,
			14_000_000,
			14_250_000,
			14_300_000,
			14_323_456, // span 2239 start (sprint 1 of span 2239)
			14_323_520, // span 2239 start (sprint 2 of span 2239) - to test recent producers lru caching
			14_323_584, // span 2239 start (sprint 3 of span 2239) - to test recent producers lru caching
			14_325_000,
			14_329_854,
			14_329_855, // span 2239 end
			14_329_856, // span 2240 start
			14_337_500,
			14_350_000,
			14_375_000,
			14_500_000,
			15_000_000,
		},
	})
}

type ServiceTestSuite struct {
	// test suite inputs
	testDataDir                    string
	chainConfig                    *chain.Config
	expectedFirstSpan              uint64
	expectedLastSpan               uint64
	expectedFirstCheckpoint        uint64
	expectedLastCheckpoint         uint64
	expectedFirstMilestone         uint64
	expectedLastMilestone          uint64
	entityNumberToCheckBeforeFirst uint64
	producersApiBlocksToTest       []uint64

	// test suite internals
	suite.Suite
	ctx                          context.Context
	logger                       log.Logger
	cancel                       context.CancelFunc
	eg                           errgroup.Group
	client                       *MockClient
	service                      *Service
	observedMilestones           []*Milestone
	observedSpans                []*Span
	spansTestDataDir             string
	checkpointsTestDataDir       string
	milestonesTestDataDir        string
	proposerSequencesTestDataDir string
}

func (suite *ServiceTestSuite) SetupSuite() {
	ctrl := gomock.NewController(suite.T())
	tempDir := suite.T().TempDir()
	dataDir := fmt.Sprintf("%s/datadir", tempDir)
	suite.logger = testlog.Logger(suite.T(), log.LvlCrit)
	store := NewMdbxStore(suite.logger, dataDir, false, 1)
	borConfig := suite.chainConfig.Bor.(*borcfg.BorConfig)
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.spansTestDataDir = filepath.Join(suite.testDataDir, "spans")
	suite.checkpointsTestDataDir = filepath.Join(suite.testDataDir, "checkpoints")
	suite.milestonesTestDataDir = filepath.Join(suite.testDataDir, "milestones")
	suite.proposerSequencesTestDataDir = filepath.Join(suite.testDataDir, "getSnapshotProposerSequence")
	suite.client = NewMockClient(ctrl)
	suite.setupSpans()
	suite.setupCheckpoints()
	suite.setupMilestones()
	suite.service = NewService(ServiceConfig{
		Store:     store,
		BorConfig: borConfig,
		Client:    suite.client,
		Logger:    suite.logger,
	})

	err := suite.service.store.Prepare(suite.ctx)
	suite.Require().NoError(err)

	suite.service.RegisterMilestoneObserver(func(milestone *Milestone) {
		suite.observedMilestones = append(suite.observedMilestones, milestone)
	})

	suite.service.RegisterSpanObserver(func(span *Span) {
		suite.observedSpans = append(suite.observedSpans, span)
	})

	suite.eg.Go(func() error {
		defer suite.cancel()
		err := suite.service.Run(suite.ctx)
		require.ErrorIs(suite.T(), err, context.Canceled)
		return err
	})

	lastMilestone, ok, err := suite.service.SynchronizeMilestones(suite.ctx)
	suite.Require().NoError(err)
	suite.Require().True(ok)
	suite.Require().Equal(suite.expectedLastMilestone, uint64(lastMilestone.Id))

	lastCheckpoint, ok, err := suite.service.SynchronizeCheckpoints(suite.ctx)
	suite.Require().NoError(err)
	suite.Require().True(ok)
	suite.Require().Equal(suite.expectedLastCheckpoint, uint64(lastCheckpoint.Id))

	err = suite.service.SynchronizeSpans(suite.ctx, math.MaxInt)
	suite.Require().NoError(err)
}

func (suite *ServiceTestSuite) TearDownSuite() {
	suite.logger.Info("tear down test case")
	suite.cancel()
	err := suite.eg.Wait()
	suite.logger.Info("test has been torn down")
	suite.Require().ErrorIs(err, context.Canceled)
}

func (suite *ServiceTestSuite) TestMilestones() {
	ctx := suite.ctx
	t := suite.T()
	svc := suite.service

	id, ok, err := svc.store.Milestones().LastEntityId(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, suite.expectedLastMilestone, id)

	checkFrom := uint64(0)

	if suite.expectedFirstMilestone > suite.entityNumberToCheckBeforeFirst {
		checkFrom = suite.expectedFirstMilestone - suite.entityNumberToCheckBeforeFirst
	}

	for id := checkFrom; id < suite.expectedFirstMilestone; id++ {
		entity, ok, err := svc.store.Milestones().Entity(ctx, id)
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, entity)
	}

	for id := suite.expectedFirstMilestone; id <= suite.expectedLastMilestone; id++ {
		entity, ok, err := svc.store.Milestones().Entity(ctx, id)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, id, uint64(entity.Id))
	}
}

func (suite *ServiceTestSuite) TestRegisterMilestoneObserver() {
	suite.Require().Len(suite.observedMilestones, int(suite.expectedLastMilestone-suite.expectedFirstMilestone+1))
}

func (suite *ServiceTestSuite) TestCheckpoints() {
	ctx := suite.ctx
	t := suite.T()
	svc := suite.service

	id, ok, err := svc.store.Checkpoints().LastEntityId(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, suite.expectedLastCheckpoint, id)

	checkFrom := uint64(0)

	if suite.expectedFirstCheckpoint > suite.entityNumberToCheckBeforeFirst {
		checkFrom = suite.expectedFirstCheckpoint - suite.entityNumberToCheckBeforeFirst
	}

	for id := checkFrom; id < suite.expectedFirstCheckpoint; id++ {
		entity, ok, err := svc.store.Checkpoints().Entity(ctx, id)
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, entity)
	}

	for id := suite.expectedFirstCheckpoint; id <= suite.expectedLastCheckpoint; id++ {
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
	require.Equal(t, suite.expectedLastSpan, id)

	checkFrom := uint64(0)

	if suite.expectedFirstSpan > suite.entityNumberToCheckBeforeFirst {
		checkFrom = suite.expectedFirstSpan - suite.entityNumberToCheckBeforeFirst
	}

	for id := checkFrom; id < suite.expectedFirstSpan; id++ {
		entity, ok, err := svc.store.Spans().Entity(ctx, id)
		require.NoError(t, err)
		require.False(t, ok)
		require.Nil(t, entity)
	}

	for id := suite.expectedFirstSpan; id <= suite.expectedLastSpan; id++ {
		entity, ok, err := svc.store.Spans().Entity(ctx, id)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, id, uint64(entity.Id))
	}
}

func (suite *ServiceTestSuite) TestRegisterSpanObserver() {
	suite.Require().Len(suite.observedSpans, int(suite.expectedLastSpan+1)) // +1 for span 0
}

func (suite *ServiceTestSuite) TestProducers() {
	for _, blockNum := range suite.producersApiBlocksToTest {
		suite.producersSubTest(blockNum)
	}
}

func (suite *ServiceTestSuite) producersSubTest(blockNum uint64) {
	suite.Run(fmt.Sprintf("%d", blockNum), func() {
		t := suite.T()
		ctx := suite.ctx
		svc := suite.service

		b, err := os.ReadFile(fmt.Sprintf("%s/blockNum_%d.json", suite.proposerSequencesTestDataDir, blockNum))
		require.NoError(t, err)
		var proposerSequenceResponse getSnapshotProposerSequenceResponse
		err = json.Unmarshal(b, &proposerSequenceResponse)
		require.NoError(t, err)
		wantProducers := proposerSequenceResponse.Result

		haveProducers, err := svc.Producers(ctx, blockNum)
		require.NoError(t, err)

		errInfoMsgArgs := []interface{}{"wantProducers: %v\nhaveProducers: %v\n", wantProducers, haveProducers}
		require.Len(t, haveProducers.Validators, len(wantProducers.Signers), errInfoMsgArgs...)
		for _, signer := range wantProducers.Signers {
			wantDifficulty := signer.Difficulty
			_, producer := haveProducers.GetByAddress(signer.Signer)
			haveDifficulty, err := haveProducers.Difficulty(producer.Address)
			require.NoError(t, err)

			errInfoMsgArgs = []interface{}{
				"signer:%v\nwantDifficulty: %v\nhaveDifficulty: %v\nwantProducers: %v\nhaveProducers: %v",
				signer,
				wantDifficulty,
				haveDifficulty,
				wantProducers,
				haveProducers,
			}
			require.Equal(t, wantDifficulty, haveDifficulty, errInfoMsgArgs...)
		}
	})
}

func (suite *ServiceTestSuite) setupSpans() {
	files, err := dir.ReadDir(suite.spansTestDataDir)
	suite.Require().NoError(err)
	require.NotEmpty(suite.T(), files)

	slices.SortFunc(files, func(a, b os.DirEntry) int {
		idA := extractIdFromFileName(suite.T(), a.Name(), "span")
		idB := extractIdFromFileName(suite.T(), b.Name(), "span")
		return cmp.Compare(idA, idB)
	})

	// leave a few of the last spans for sequential flow, all spans before them for batch flow
	lastSequentialFetchIdx := len(files) - 1
	lastBatchFetchIdx := max(0, lastSequentialFetchIdx-2)
	latestSpanIdx := lastBatchFetchIdx
	suite.client.EXPECT().
		FetchLatestSpan(gomock.Any()).
		DoAndReturn(func(ctx context.Context) (*Span, error) {
			span := readEntityFromFile[Span](
				suite.T(),
				fmt.Sprintf("%s/%s", suite.spansTestDataDir, files[latestSpanIdx].Name()),
			)
			latestSpanIdx = lastSequentialFetchIdx
			return span, nil
		}).
		AnyTimes()

	suite.client.EXPECT().
		FetchSpans(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, page, limit uint64) ([]*Span, error) {
			spans := make([]*Span, 0, limit)
			startIdx := (page - 1) * limit
			endIdx := min(startIdx+limit, uint64(lastBatchFetchIdx)+1)
			for i := startIdx; i < endIdx; i++ {
				span := readEntityFromFile[Span](suite.T(), fmt.Sprintf("%s/span_%d.json", suite.spansTestDataDir, i))
				spans = append(spans, span)
			}
			return spans, nil
		}).
		AnyTimes()

	suite.client.EXPECT().
		FetchSpan(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, id uint64) (*Span, error) {
			return readEntityFromFile[Span](suite.T(), fmt.Sprintf("%s/span_%d.json", suite.spansTestDataDir, id)), nil
		}).
		AnyTimes()
}

func (suite *ServiceTestSuite) setupCheckpoints() {
	files, err := dir.ReadDir(suite.checkpointsTestDataDir)
	suite.Require().NoError(err)
	require.NotEmpty(suite.T(), files)

	// leave a few of the last checkpoints for sequential flow, all spans before them for batch flow
	lastSequentialFetchIdx := len(files) - 1
	lastBatchFetchIdx := max(0, lastSequentialFetchIdx-2)
	checkpointCount := lastBatchFetchIdx + 1
	suite.client.EXPECT().
		FetchCheckpointCount(gomock.Any()).
		DoAndReturn(func(ctx context.Context) (int64, error) {
			res := int64(checkpointCount)
			checkpointCount = len(files)
			return res, nil
		}).
		AnyTimes()

	suite.client.EXPECT().
		FetchCheckpoints(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, page uint64, limit uint64) ([]*Checkpoint, error) {
			checkpoints := make([]*Checkpoint, 0, limit)
			startIdx := (page - 1) * limit
			endIdx := min(startIdx+limit, uint64(lastBatchFetchIdx)+1)
			for i := startIdx; i < endIdx; i++ {
				checkpoint := readEntityFromFile[Checkpoint](
					suite.T(),
					fmt.Sprintf("%s/checkpoint_%d.json", suite.checkpointsTestDataDir, i+1),
				)
				checkpoints = append(checkpoints, checkpoint)
			}
			return checkpoints, nil
		}).
		AnyTimes()

	suite.client.EXPECT().
		FetchCheckpoint(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, id int64) (*Checkpoint, error) {
			checkpoint := readEntityFromFile[Checkpoint](
				suite.T(),
				fmt.Sprintf("%s/checkpoint_%d.json", suite.checkpointsTestDataDir, id),
			)
			return checkpoint, nil
		}).
		AnyTimes()
}

func (suite *ServiceTestSuite) setupMilestones() {
	files, err := dir.ReadDir(suite.milestonesTestDataDir)
	suite.Require().NoError(err)
	require.NotEmpty(suite.T(), files)

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
				fmt.Sprintf("%s/milestone_%d.json", suite.milestonesTestDataDir, id),
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

func TestIsCatchingUp(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	ctrl := gomock.NewController(t)
	mockClient := NewMockClient(ctrl)

	s := Service{
		client: mockClient,
	}

	mockClient.EXPECT().
		FetchStatus(gomock.Any()).
		DoAndReturn(func(ctx context.Context) (*Status, error) {
			return &Status{
				LatestBlockTime: "",
				CatchingUp:      true,
			}, nil
		})

	isCatchingUp, err := s.IsCatchingUp(context.TODO())
	require.NoError(t, err)
	require.True(t, isCatchingUp)
}

func TestIsCatchingUpLateBlock(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	ctrl := gomock.NewController(t)
	mockClient := NewMockClient(ctrl)

	s := Service{
		client: mockClient,
	}

	mockClient.EXPECT().
		FetchStatus(gomock.Any()).
		DoAndReturn(func(ctx context.Context) (*Status, error) {
			return &Status{
				LatestBlockTime: "2025-02-14T11:45:00.764588Z",
				CatchingUp:      false,
			}, nil
		})

	isCatchingUp, err := s.IsCatchingUp(context.TODO())
	require.NoError(t, err)
	require.True(t, isCatchingUp)
}
