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
	FetchEntitiesRange(ctx context.Context, idRange EntityIdRange) ([]any, error)
}

type genericEntityFetcher[TEntity entity] struct {
	name string

	fetchLastEntityId func(ctx context.Context) (int64, error)
	fetchEntity       func(ctx context.Context, id int64) (*TEntity, error)
	fetchEntitiesPage func(ctx context.Context, page uint64, limit uint64) ([]*TEntity, error)
	getStartBlockNum  func(entity *TEntity) uint64

	logger log.Logger
}

func newGenericEntityFetcher[TEntity entity](
	name string,
	fetchLastEntityId func(ctx context.Context) (int64, error),
	fetchEntity func(ctx context.Context, id int64) (*TEntity, error),
	fetchEntitiesPage func(ctx context.Context, page uint64, limit uint64) ([]*TEntity, error),
	getStartBlockNum func(entity *TEntity) uint64,
	logger log.Logger,
) entityFetcher {
	return &genericEntityFetcher[TEntity]{
		name:              name,
		fetchLastEntityId: fetchLastEntityId,
		fetchEntity:       fetchEntity,
		fetchEntitiesPage: fetchEntitiesPage,
		getStartBlockNum:  getStartBlockNum,
		logger:            logger,
	}
}

func (f *genericEntityFetcher[TEntity]) FetchLastEntityId(ctx context.Context) (uint64, error) {
	id, err := f.fetchLastEntityId(ctx)
	return uint64(id), err
}

func (f *genericEntityFetcher[TEntity]) FetchEntitiesRange(ctx context.Context, idRange EntityIdRange) ([]any, error) {
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

func (f *genericEntityFetcher[TEntity]) FetchEntitiesRangeSequentially(ctx context.Context, idRange EntityIdRange) ([]any, error) {
	entities := make([]any, 0, idRange.Len())

	for id := idRange.Start; id <= idRange.End; id++ {
		entity, err := f.fetchEntity(ctx, int64(id))
		if err != nil {
			return nil, err
		}
		entities = append(entities, entity)
	}

	return entities, nil
}

func (f *genericEntityFetcher[TEntity]) FetchAllEntities(ctx context.Context) ([]any, error) {
	// TODO: once heimdall API is fixed to return sorted items in pages we can only fetch
	//
	//	the new pages after lastStoredCheckpointId using the checkpoints/list paging API
	//	(for now we have to fetch all of them)
	//	and also remove sorting we do after fetching

	var entities []any

	fetchStartTime := time.Now()
	progressLogTicker := time.NewTicker(30 * time.Second)
	defer progressLogTicker.Stop()

	for page := uint64(1); page < 10_000; page++ {
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

	slices.SortFunc(entities, func(e1, e2 any) int {
		n1 := f.getStartBlockNum(e1.(*TEntity))
		n2 := f.getStartBlockNum(e2.(*TEntity))
		return cmp.Compare(n1, n2)
	})

	f.logger.Debug(
		heimdallLogPrefix(fmt.Sprintf("%s done", f.name)),
		"len", len(entities),
		"duration", time.Since(fetchStartTime),
	)

	return entities, nil
}
