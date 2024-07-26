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

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/common/generics"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/polygon/polygoncommon"
)

type ServiceStore interface {
	Checkpoints() EntityStore[*Checkpoint]
	Milestones() EntityStore[*Milestone]
	Spans() EntityStore[*Span]
	Prepare(ctx context.Context) error
	Close()
}

func NewMdbxServiceStore(logger log.Logger, dataDir string, tmpDir string) *MdbxServiceStore {
	db := polygoncommon.NewDatabase(dataDir, kv.HeimdallDB, databaseTablesCfg, logger)
	blockNumToIdIndexFactory := func(ctx context.Context) (*RangeIndex, error) {
		return NewRangeIndex(ctx, tmpDir, logger)
	}

	return &MdbxServiceStore{
		db:          db,
		checkpoints: newMdbxEntityStore(db, kv.BorCheckpoints, generics.New[Checkpoint], blockNumToIdIndexFactory),
		milestones:  newMdbxEntityStore(db, kv.BorMilestones, generics.New[Milestone], blockNumToIdIndexFactory),
		spans:       newMdbxEntityStore(db, kv.BorSpans, generics.New[Span], blockNumToIdIndexFactory),
	}
}

type MdbxServiceStore struct {
	db          *polygoncommon.Database
	checkpoints EntityStore[*Checkpoint]
	milestones  EntityStore[*Milestone]
	spans       EntityStore[*Span]
}

func (s *MdbxServiceStore) Checkpoints() EntityStore[*Checkpoint] {
	return s.checkpoints
}

func (s *MdbxServiceStore) Milestones() EntityStore[*Milestone] {
	return s.milestones
}

func (s *MdbxServiceStore) Spans() EntityStore[*Span] {
	return s.spans
}

func (s *MdbxServiceStore) Prepare(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error { return s.checkpoints.Prepare(ctx) })
	eg.Go(func() error { return s.milestones.Prepare(ctx) })
	eg.Go(func() error { return s.spans.Prepare(ctx) })
	return eg.Wait()
}

func (s *MdbxServiceStore) Close() {
	s.db.Close()
	s.checkpoints.Close()
	s.milestones.Close()
	s.spans.Close()
}
