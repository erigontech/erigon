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
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/polygon/polygoncommon"
)

type Store interface {
	Checkpoints() EntityStore[*Checkpoint]
	Milestones() EntityStore[*Milestone]
	Spans() EntityStore[*Span]
	SpanBlockProducerSelections() EntityStore[*SpanBlockProducerSelection]
	Prepare(ctx context.Context) error
	Close()
}

func NewMdbxStore(logger log.Logger, dataDir string, accede bool, roTxLimit int64) *MdbxStore {
	return newMdbxStore(polygoncommon.NewDatabase(dataDir, kv.HeimdallDB, databaseTablesCfg, logger, accede, roTxLimit))
}

func newMdbxStore(db *polygoncommon.Database) *MdbxStore {
	spanIndex := NewSpanRangeIndex(db, kv.BorSpansIndex)
	producerSelectionIndex := NewSpanRangeIndex(db, kv.BorProducerSelectionsIndex)

	return &MdbxStore{
		db: db,
		checkpoints: newMdbxEntityStore(
			db, kv.BorCheckpoints, Checkpoints, generics.New[Checkpoint],
			NewRangeIndex(db, kv.BorCheckpointEnds)),
		milestones: newMdbxEntityStore(
			db, kv.BorMilestones, Milestones, generics.New[Milestone],
			NewRangeIndex(db, kv.BorMilestoneEnds)),
		spans: newMdbxEntityStore(
			db, kv.BorSpans, Spans, generics.New[Span], spanIndex),
		spanBlockProducerSelections: newMdbxEntityStore(
			db, kv.BorProducerSelections, nil, generics.New[SpanBlockProducerSelection], producerSelectionIndex),
	}
}

func NewDbStore(db kv.RoDB) *MdbxStore {
	return newMdbxStore(polygoncommon.AsDatabase(db))
}

type MdbxStore struct {
	db                          *polygoncommon.Database
	checkpoints                 EntityStore[*Checkpoint]
	milestones                  EntityStore[*Milestone]
	spans                       EntityStore[*Span]
	spanBlockProducerSelections EntityStore[*SpanBlockProducerSelection]
}

func (s *MdbxStore) Checkpoints() EntityStore[*Checkpoint] {
	return s.checkpoints
}

func (s *MdbxStore) Milestones() EntityStore[*Milestone] {
	return s.milestones
}

func (s *MdbxStore) Spans() EntityStore[*Span] {
	return s.spans
}

func (s *MdbxStore) SpanBlockProducerSelections() EntityStore[*SpanBlockProducerSelection] {
	return s.spanBlockProducerSelections
}

func (s *MdbxStore) Prepare(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error { return s.checkpoints.Prepare(ctx) })
	eg.Go(func() error { return s.milestones.Prepare(ctx) })
	eg.Go(func() error { return s.spans.Prepare(ctx) })
	eg.Go(func() error { return s.spanBlockProducerSelections.Prepare(ctx) })
	return eg.Wait()
}

func (s *MdbxStore) Close() {
	s.db.Close()
	s.checkpoints.Close()
	s.milestones.Close()
	s.spans.Close()
	s.spanBlockProducerSelections.Close()
}
