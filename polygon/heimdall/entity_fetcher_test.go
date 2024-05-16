package heimdall

import (
	"context"
	"testing"

	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

func TestEntityFetcher_FetchAllEntities(t *testing.T) {
	for count := uint64(0); count < 20; count++ {
		testEntityFetcher_FetchAllEntities(t, count, 5)
	}
}

func testEntityFetcher_FetchAllEntities(t *testing.T, count uint64, fetchEntitiesPageLimit uint64) {
	ctx := context.Background()
	logger := log.New()

	var expectedEntities []*Checkpoint
	for i := uint64(0); i < count; i++ {
		c := makeCheckpoint(i*256, 256)
		c.Id = CheckpointId(i + 1)
		expectedEntities = append(expectedEntities, c)
	}

	servedEntities := make([]*Checkpoint, len(expectedEntities))
	copy(servedEntities, expectedEntities)
	libcommon.SliceShuffle(servedEntities)
	fetchEntitiesPage := func(ctx context.Context, page uint64, limit uint64) ([]*Checkpoint, error) {
		offset := (page - 1) * limit
		totalLen := uint64(len(servedEntities))
		return servedEntities[min(offset, totalLen):min(offset+limit, totalLen)], nil
	}

	fetcher := newEntityFetcher[*Checkpoint](
		"fetcher",
		nil,
		nil,
		fetchEntitiesPage,
		fetchEntitiesPageLimit,
		logger,
	)

	actualEntities, err := fetcher.FetchAllEntities(ctx)
	require.NoError(t, err)
	assert.Equal(t, expectedEntities, actualEntities)
}
