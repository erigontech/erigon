package heimdall

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/ledgerwatch/log/v3"
)

type entityFetcher interface {
	FetchLastEntityId(ctx context.Context) (uint64, error)
	FetchEntitiesRange(ctx context.Context, idRange ClosedRange) ([]Entity, error)
}

type entityFetcherImpl struct {
	name string

	fetchLastEntityId func(ctx context.Context) (int64, error)
	fetchEntity       func(ctx context.Context, id int64) (Entity, error)
	fetchEntitiesPage func(ctx context.Context, page uint64, limit uint64) ([]Entity, error)

	logger log.Logger
}

func newEntityFetcher(
	name string,
	fetchLastEntityId func(ctx context.Context) (int64, error),
	fetchEntity func(ctx context.Context, id int64) (Entity, error),
	fetchEntitiesPage func(ctx context.Context, page uint64, limit uint64) ([]Entity, error),
	logger log.Logger,
) entityFetcher {
	return &entityFetcherImpl{
		name:              name,
		fetchLastEntityId: fetchLastEntityId,
		fetchEntity:       fetchEntity,
		fetchEntitiesPage: fetchEntitiesPage,
		logger:            logger,
	}
}

func (f *entityFetcherImpl) FetchLastEntityId(ctx context.Context) (uint64, error) {
	id, err := f.fetchLastEntityId(ctx)
	return uint64(id), err
}

func (f *entityFetcherImpl) FetchEntitiesRange(ctx context.Context, idRange ClosedRange) ([]Entity, error) {
	count := idRange.Len()

	const batchFetchThreshold = 100
	if (count > batchFetchThreshold) && (f.fetchEntitiesPage != nil) {
		allEntities, err := f.FetchAllEntities(ctx)
		if err != nil {
			return nil, err
		}
		startIndex := idRange.Start - 1
		return allEntities[startIndex : startIndex+count], nil
	}

	return f.FetchEntitiesRangeSequentially(ctx, idRange)
}

func (f *entityFetcherImpl) FetchEntitiesRangeSequentially(ctx context.Context, idRange ClosedRange) ([]Entity, error) {
	return ClosedRangeMap(idRange, func(id uint64) (Entity, error) {
		return f.fetchEntity(ctx, int64(id))
	})
}

func (f *entityFetcherImpl) FetchAllEntities(ctx context.Context) ([]Entity, error) {
	// TODO: once heimdall API is fixed to return sorted items in pages we can only fetch
	//
	//	the new pages after lastStoredCheckpointId using the checkpoints/list paging API
	//	(for now we have to fetch all of them)
	//	and also remove sorting we do after fetching

	var entities []Entity

	fetchStartTime := time.Now()
	progressLogTicker := time.NewTicker(30 * time.Second)
	defer progressLogTicker.Stop()

	for page := uint64(1); ; page++ {
		entitiesPage, err := f.fetchEntitiesPage(ctx, page, 10_000)
		if err != nil {
			return nil, err
		}
		if len(entitiesPage) == 0 {
			break
		}

		for _, entity := range entitiesPage {
			entities = append(entities, entity)
		}

		select {
		case <-progressLogTicker.C:
			f.logger.Debug(
				heimdallLogPrefix(fmt.Sprintf("%s progress", f.name)),
				"page", page,
				"len", len(entities),
			)
		default:
			// carry-on
		}
	}

	slices.SortFunc(entities, func(e1, e2 Entity) int {
		n1 := e1.BlockNumRange().Start
		n2 := e2.BlockNumRange().Start
		return cmp.Compare(n1, n2)
	})

	f.logger.Debug(
		heimdallLogPrefix(fmt.Sprintf("%s done", f.name)),
		"len", len(entities),
		"duration", time.Since(fetchStartTime),
	)

	return entities, nil
}
