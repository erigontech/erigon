package heimdall

import (
	"context"

	"golang.org/x/sync/errgroup"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/log/v3"
	"github.com/ledgerwatch/erigon/polygon/polygoncommon"
)

type ServicePersistence interface {
	Checkpoints() EntityStore[*Checkpoint]
	Milestones() EntityStore[*Milestone]
	Spans() EntityStore[*Span]
	Prepare(ctx context.Context) error
	Close()
}

func NewMdbxPersistence(logger log.Logger, dataDir string, tmpDir string) *MdbxServicePersistence {
	db := polygoncommon.NewDatabase(dataDir, logger)
	blockNumToIdIndexFactory := func(ctx context.Context) (*RangeIndex, error) {
		return NewRangeIndex(ctx, tmpDir, logger)
	}

	return &MdbxServicePersistence{
		db:          db,
		checkpoints: newMdbxEntityStore(db, kv.HeimdallDB, kv.BorCheckpoints, makeType[Checkpoint], blockNumToIdIndexFactory),
		milestones:  newMdbxEntityStore(db, kv.HeimdallDB, kv.BorMilestones, makeType[Milestone], blockNumToIdIndexFactory),
		spans:       newMdbxEntityStore(db, kv.HeimdallDB, kv.BorSpans, makeType[Span], blockNumToIdIndexFactory),
	}
}

type MdbxServicePersistence struct {
	db          *polygoncommon.Database
	checkpoints EntityStore[*Checkpoint]
	milestones  EntityStore[*Milestone]
	spans       EntityStore[*Span]
}

func (s *MdbxServicePersistence) Checkpoints() EntityStore[*Checkpoint] {
	return s.checkpoints
}

func (s *MdbxServicePersistence) Milestones() EntityStore[*Milestone] {
	return s.milestones
}

func (s *MdbxServicePersistence) Spans() EntityStore[*Span] {
	return s.spans
}

func (s *MdbxServicePersistence) Prepare(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error { return s.checkpoints.Prepare(ctx) })
	eg.Go(func() error { return s.milestones.Prepare(ctx) })
	eg.Go(func() error { return s.spans.Prepare(ctx) })
	return eg.Wait()
}

func (s *MdbxServicePersistence) Close() {
	s.db.Close()
	s.checkpoints.Close()
	s.milestones.Close()
	s.spans.Close()
}
