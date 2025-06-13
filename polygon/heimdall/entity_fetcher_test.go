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
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
)

func makeEntities(count uint64) []*Checkpoint {
	var entities []*Checkpoint
	for i := uint64(0); i < count; i++ {
		c := makeCheckpoint(i*256, 256)
		c.Id = CheckpointId(i + 1)
		entities = append(entities, c)
	}
	return entities
}

func makeFetchEntitiesPage(
	entities []*Checkpoint,
) func(ctx context.Context, page uint64, limit uint64) ([]*Checkpoint, error) {
	return func(ctx context.Context, page uint64, limit uint64) ([]*Checkpoint, error) {
		offset := (page - 1) * limit
		totalLen := uint64(len(entities))
		return entities[min(offset, totalLen):min(offset+limit, totalLen)], nil
	}
}

func TestEntityFetcher_FetchAllEntities(t *testing.T) {
	for count := uint64(0); count < 20; count++ {
		testEntityFetcher_FetchAllEntities(t, count, 5)
	}
}

func testEntityFetcher_FetchAllEntities(t *testing.T, count uint64, fetchEntitiesPageLimit uint64) {
	ctx := context.Background()
	logger := log.New()

	expectedEntities := makeEntities(count)
	servedEntities := make([]*Checkpoint, len(expectedEntities))
	copy(servedEntities, expectedEntities)
	common.SliceShuffle(servedEntities)
	fetchEntitiesPage := makeFetchEntitiesPage(servedEntities)

	fetcher := NewEntityFetcher[*Checkpoint](
		"fetcher",
		nil,
		nil,
		nil,
		fetchEntitiesPage,
		fetchEntitiesPageLimit,
		1,
		logger,
	)

	actualEntities, err := fetcher.FetchAllEntities(ctx)
	require.NoError(t, err)
	assert.Equal(t, expectedEntities, actualEntities)
}

type entityFetcherFetchEntitiesRangeTest struct {
	fetcher          *EntityFetcher[*Checkpoint]
	testRange        ClosedRange
	expectedEntities []*Checkpoint
	ctx              context.Context
	logger           log.Logger
}

func newEntityFetcherFetchEntitiesRangeTest(count uint64, withPaging bool, testRange *ClosedRange) entityFetcherFetchEntitiesRangeTest {
	ctx := context.Background()
	logger := log.New()

	if testRange == nil {
		testRange = &ClosedRange{1, count}
	}

	expectedEntities := makeEntities(count)
	fetchEntity := func(ctx context.Context, id int64) (*Checkpoint, error) {
		return expectedEntities[id-1], nil
	}

	fetchEntitiesPage := makeFetchEntitiesPage(expectedEntities)
	if !withPaging {
		fetchEntitiesPage = nil
	}

	fetcher := NewEntityFetcher[*Checkpoint](
		"fetcher",
		nil,
		nil,
		fetchEntity,
		fetchEntitiesPage,
		entityFetcherBatchFetchThreshold,
		1,
		logger,
	)

	return entityFetcherFetchEntitiesRangeTest{
		fetcher:          fetcher,
		testRange:        *testRange,
		expectedEntities: expectedEntities,
		ctx:              ctx,
		logger:           logger,
	}
}

func (test entityFetcherFetchEntitiesRangeTest) Run(t *testing.T) {
	actualEntities, err := test.fetcher.FetchEntitiesRange(test.ctx, test.testRange)
	require.NoError(t, err)
	assert.Equal(t, test.expectedEntities[test.testRange.Start-1:test.testRange.End], actualEntities)
}

func TestEntityFetcher_FetchEntitiesRange(t *testing.T) {
	makeTest := newEntityFetcherFetchEntitiesRangeTest
	var many uint64 = entityFetcherBatchFetchThreshold + 1

	t.Run("no paging", makeTest(1, false, nil).Run)
	t.Run("paging few", makeTest(1, true, nil).Run)
	t.Run("paging many", makeTest(many, true, nil).Run)
	t.Run("paging many subrange", makeTest(many, true, &ClosedRange{many / 3, many / 2}).Run)
}
