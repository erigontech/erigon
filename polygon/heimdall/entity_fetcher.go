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
	"slices"
	"time"

	"github.com/erigontech/erigon-lib/log/v3"
)

type EntityFetcher[TEntity Entity] struct {
	name                      string
	fetchFirstEntityId        func(ctx context.Context) (int64, error)
	fetchLastEntityId         func(ctx context.Context) (int64, error)
	fetchEntity               func(ctx context.Context, id int64) (TEntity, error)
	fetchEntitiesPage         func(ctx context.Context, page uint64, limit uint64) ([]TEntity, error)
	fetchEntitiesPageLimit    uint64
	fetchAllEntitiesIdxOffset uint64
	logger                    log.Logger
}

func NewEntityFetcher[TEntity Entity](
	name string,
	fetchFirstEntityId func(ctx context.Context) (int64, error),
	fetchLastEntityId func(ctx context.Context) (int64, error),
	fetchEntity func(ctx context.Context, id int64) (TEntity, error),
	fetchEntitiesPage func(ctx context.Context, page uint64, limit uint64) ([]TEntity, error),
	fetchEntitiesPageLimit uint64,
	fetchAllEntitiesIdxOffset uint64,
	logger log.Logger,
) *EntityFetcher[TEntity] {
	return &EntityFetcher[TEntity]{
		name:                      name,
		fetchFirstEntityId:        fetchFirstEntityId,
		fetchLastEntityId:         fetchLastEntityId,
		fetchEntity:               fetchEntity,
		fetchEntitiesPage:         fetchEntitiesPage,
		fetchEntitiesPageLimit:    fetchEntitiesPageLimit,
		fetchAllEntitiesIdxOffset: fetchAllEntitiesIdxOffset,
		logger:                    logger,
	}
}

func (f *EntityFetcher[TEntity]) FetchEntityIdRange(ctx context.Context) (ClosedRange, error) {
	first, err := f.fetchFirstEntityId(ctx)
	if err != nil {
		return ClosedRange{}, err
	}

	last, err := f.fetchLastEntityId(ctx)
	if err != nil {
		return ClosedRange{}, err
	}

	res := ClosedRange{
		Start: uint64(first),
		End:   uint64(last),
	}
	return res, nil
}

const entityFetcherBatchFetchThreshold = 100

func (f *EntityFetcher[TEntity]) FetchEntitiesRange(ctx context.Context, idRange ClosedRange) ([]TEntity, error) {
	count := idRange.Len()

	if (count > entityFetcherBatchFetchThreshold) && (f.fetchEntitiesPage != nil) {
		allEntities, err := f.FetchAllEntities(ctx)
		if err != nil {
			return nil, err
		}
		startIndex := idRange.Start - f.fetchAllEntitiesIdxOffset
		return allEntities[startIndex : startIndex+count], nil
	}

	return f.FetchEntitiesRangeSequentially(ctx, idRange)
}

func (f *EntityFetcher[TEntity]) FetchEntitiesRangeSequentially(ctx context.Context, idRange ClosedRange) ([]TEntity, error) {
	progressLogTicker := time.NewTicker(30 * time.Second)
	defer progressLogTicker.Stop()

	entities := make([]TEntity, 0, idRange.Len())
	for id := idRange.Start; id <= idRange.End; id++ {
		select {
		case <-progressLogTicker.C:
			f.logger.Info(
				heimdallLogPrefix(f.name+" fetch entities sequentially periodic progress"),
				"current", id,
				"start", idRange.Start,
				"end", idRange.End,
			)
		default:
			// carry-on
		}

		entity, err := f.fetchEntity(ctx, int64(id))
		if err != nil {
			// return fetched entities up to this point in case of transient errors
			return entities, err
		}

		entities = append(entities, entity)
	}

	return entities, nil
}

func (f *EntityFetcher[TEntity]) FetchAllEntities(ctx context.Context) ([]TEntity, error) {
	// TODO: once heimdall API is fixed to return sorted items in pages we can only fetch
	//
	//	the new pages after lastStoredCheckpointId using the checkpoints/list paging API
	//	(for now we have to fetch all of them)
	//	and also remove sorting we do after fetching

	var entities []TEntity

	fetchStartTime := time.Now()
	progressLogTicker := time.NewTicker(30 * time.Second)
	defer progressLogTicker.Stop()

	for page := uint64(1); ; page++ {
		entitiesPage, err := f.fetchEntitiesPage(ctx, page, f.fetchEntitiesPageLimit)
		if err != nil {
			return nil, err
		}
		if len(entitiesPage) == 0 {
			break
		}

		entities = append(entities, entitiesPage...)

		select {
		case <-progressLogTicker.C:
			f.logger.Info(
				heimdallLogPrefix(f.name+" fetch all entities periodic progress"),
				"page", page,
				"len", len(entities),
			)
		default:
			// carry-on
		}
	}

	// Due to VeBlop, span.StartBlock is no longer strictly increasing,
	// so this kind of breaks the "Entity" abstraction.
	// So for spans we skip the sorting and just rely on span.Id for the ordering.
	var entity TEntity
	if _, ok := any(entity).(*Span); !ok {
		slices.SortFunc(entities, func(e1, e2 TEntity) int {
			n1 := e1.BlockNumRange().Start
			n2 := e2.BlockNumRange().Start
			return cmp.Compare(n1, n2)
		})
		for i, entity := range entities {
			entity.SetRawId(uint64(i + 1))
		}
	}

	f.logger.Debug(
		heimdallLogPrefix(f.name+" fetch all entities done"),
		"len", len(entities),
		"duration", time.Since(fetchStartTime),
	)

	return entities, nil
}

//go:generate mockgen -typed=true -source=./entity_fetcher.go -destination=./entity_fetcher_mock.go -package=heimdall
type entityFetcher[TEntity Entity] interface {
	FetchEntityIdRange(ctx context.Context) (ClosedRange, error)
	FetchEntitiesRange(ctx context.Context, idRange ClosedRange) ([]TEntity, error)
	FetchAllEntities(ctx context.Context) ([]TEntity, error)
}
