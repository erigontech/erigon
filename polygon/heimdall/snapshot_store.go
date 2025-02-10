package heimdall

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common/generics"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/turbo/snapshotsync"
)

func NewSnapshotStore(base Store, snapshots *RoSnapshots) *SnapshotStore {
	return &SnapshotStore{
		Store:                       base,
		checkpoints:                 NewCheckpointSnapshotStore(base.Checkpoints(), snapshots),
		milestones:                  NewMilestoneSnapshotStore(base.Milestones(), snapshots),
		spans:                       NewSpanSnapshotStore(base.Spans(), snapshots),
		spanBlockProducerSelections: base.SpanBlockProducerSelections(),
	}
}

type SnapshotStore struct {
	Store
	checkpoints                 EntityStore[*Checkpoint]
	milestones                  EntityStore[*Milestone]
	spans                       EntityStore[*Span]
	spanBlockProducerSelections EntityStore[*SpanBlockProducerSelection]
}

func (s *SnapshotStore) Checkpoints() EntityStore[*Checkpoint] {
	return s.checkpoints
}

func (s *SnapshotStore) Milestones() EntityStore[*Milestone] {
	return s.milestones
}

func (s *SnapshotStore) Spans() EntityStore[*Span] {
	return s.spans
}

func (s *SnapshotStore) SpanBlockProducerSelections() EntityStore[*SpanBlockProducerSelection] {
	return s.spanBlockProducerSelections
}

func (s *SnapshotStore) Prepare(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error { return s.checkpoints.Prepare(ctx) })
	eg.Go(func() error { return s.milestones.Prepare(ctx) })
	eg.Go(func() error { return s.spans.Prepare(ctx) })
	eg.Go(func() error { return s.spanBlockProducerSelections.Prepare(ctx) })
	return eg.Wait()
}

var ErrSpanNotFound = errors.New("span not found")

type SpanSnapshotStore struct {
	EntityStore[*Span]
	snapshots *RoSnapshots
}

func NewSpanSnapshotStore(base EntityStore[*Span], snapshots *RoSnapshots) *SpanSnapshotStore {
	return &SpanSnapshotStore{base, snapshots}
}

func (s *SpanSnapshotStore) Prepare(ctx context.Context) error {
	if err := s.EntityStore.Prepare(ctx); err != nil {
		return err
	}

	return <-s.snapshots.Ready(ctx)
}

func (s *SpanSnapshotStore) WithTx(tx kv.Tx) EntityStore[*Span] {
	return &SpanSnapshotStore{txEntityStore[*Span]{s.EntityStore.(*mdbxEntityStore[*Span]), tx}, s.snapshots}
}

func (s *SpanSnapshotStore) RangeExtractor() snaptype.RangeExtractor {
	return snaptype.RangeExtractorFunc(
		func(ctx context.Context, blockFrom, blockTo uint64, firstKey snaptype.FirstKeyGetter, db kv.RoDB, chainConfig *chain.Config, collect func([]byte) error, workers int, lvl log.Lvl, logger log.Logger) (uint64, error) {
			return s.SnapType().RangeExtractor().Extract(ctx, blockFrom, blockTo, firstKey,
				s.EntityStore.(*mdbxEntityStore[*Span]).db.RoDB(), chainConfig, collect, workers, lvl, logger)
		})
}

func (s *SpanSnapshotStore) LastFrozenEntityId() uint64 {
	if s.snapshots == nil {
		return 0
	}

	tx := s.snapshots.ViewType(s.SnapType())
	defer tx.Close()
	segments := tx.Segments

	if len(segments) == 0 {
		return 0
	}
	// find the last segment which has a built non-empty index
	var lastSegment *snapshotsync.VisibleSegment
	for i := len(segments) - 1; i >= 0; i-- {
		if segments[i].Src().Index() != nil {
			gg := segments[i].Src().MakeGetter()
			if gg.HasNext() {
				lastSegment = segments[i]
				break
			}
		}
	}
	if lastSegment == nil {
		return 0
	}

	lastSpanID := SpanIdAt(lastSegment.To())
	if lastSpanID > 0 {
		lastSpanID--
	}
	return uint64(lastSpanID)
}

func (s *SpanSnapshotStore) Entity(ctx context.Context, id uint64) (*Span, bool, error) {
	var endBlock uint64
	if id > 0 {
		endBlock = SpanEndBlockNum(SpanId(id))
	}

	maxBlockNumInFiles := s.snapshots.VisibleBlocksAvailable(s.SnapType().Enum())
	if maxBlockNumInFiles == 0 || endBlock > maxBlockNumInFiles {
		return s.EntityStore.Entity(ctx, id)
	}

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], id)

	tx := s.snapshots.ViewType(s.SnapType())
	defer tx.Close()
	segments := tx.Segments

	for i := len(segments) - 1; i >= 0; i-- {
		sn := segments[i]
		idx := sn.Src().Index()

		if idx == nil {
			continue
		}
		spanFrom := uint64(SpanIdAt(sn.From()))
		if id < spanFrom {
			continue
		}
		spanTo := uint64(SpanIdAt(sn.To()))
		if id >= spanTo {
			continue
		}
		if idx.KeyCount() == 0 {
			continue
		}
		offset := idx.OrdinalLookup(id - idx.BaseDataID())
		gg := sn.Src().MakeGetter()
		gg.Reset(offset)
		result, _ := gg.Next(nil)

		var span Span
		if err := json.Unmarshal(result, &span); err != nil {
			return nil, false, err
		}

		return &span, true, nil
	}

	return nil, false, fmt.Errorf("span %d: %w (snapshots)", id, ErrSpanNotFound)
}

func (s *SpanSnapshotStore) LastEntityId(ctx context.Context) (uint64, bool, error) {
	lastId, ok, err := s.EntityStore.LastEntityId(ctx)

	snapshotLastId := s.LastFrozenEntityId()
	if snapshotLastId > lastId {
		return snapshotLastId, true, nil
	}

	return lastId, ok, err
}

func (s *SpanSnapshotStore) ValidateSnapshots(logger log.Logger, failFast bool) error {
	return validateSnapshots(logger, failFast, s.snapshots, s.SnapType(), generics.New[Span])
}

type MilestoneSnapshotStore struct {
	EntityStore[*Milestone]
	snapshots *RoSnapshots
}

func NewMilestoneSnapshotStore(base EntityStore[*Milestone], snapshots *RoSnapshots) *MilestoneSnapshotStore {
	return &MilestoneSnapshotStore{base, snapshots}
}

func (s *MilestoneSnapshotStore) Prepare(ctx context.Context) error {
	if err := s.EntityStore.Prepare(ctx); err != nil {
		return err
	}

	return <-s.snapshots.Ready(ctx)
}

func (s *MilestoneSnapshotStore) WithTx(tx kv.Tx) EntityStore[*Milestone] {
	return &MilestoneSnapshotStore{txEntityStore[*Milestone]{s.EntityStore.(*mdbxEntityStore[*Milestone]), tx}, s.snapshots}
}

func (s *MilestoneSnapshotStore) RangeExtractor() snaptype.RangeExtractor {
	return snaptype.RangeExtractorFunc(
		func(ctx context.Context, blockFrom, blockTo uint64, firstKey snaptype.FirstKeyGetter, db kv.RoDB, chainConfig *chain.Config, collect func([]byte) error, workers int, lvl log.Lvl, logger log.Logger) (uint64, error) {
			return s.SnapType().RangeExtractor().Extract(ctx, blockFrom, blockTo, firstKey,
				s.EntityStore.(*mdbxEntityStore[*Milestone]).db.RoDB(), chainConfig, collect, workers, lvl, logger)
		})
}

func (s *MilestoneSnapshotStore) LastFrozenEntityId() uint64 {
	if s.snapshots == nil {
		return 0
	}

	tx := s.snapshots.ViewType(s.SnapType())
	defer tx.Close()
	segments := tx.Segments

	if len(segments) == 0 {
		return 0
	}
	// find the last segment which has a built non-empty index
	var lastSegment *snapshotsync.VisibleSegment
	for i := len(segments) - 1; i >= 0; i-- {
		if segments[i].Src().Index() != nil {
			gg := segments[i].Src().MakeGetter()
			if gg.HasNext() {
				lastSegment = segments[i]
				break
			}
		}
	}
	if lastSegment == nil {
		return 0
	}

	index := lastSegment.Src().Index()

	return index.BaseDataID() + index.KeyCount() - 1
}

func (s *MilestoneSnapshotStore) LastEntityId(ctx context.Context) (uint64, bool, error) {
	lastId, ok, err := s.EntityStore.LastEntityId(ctx)

	snapshotLastId := s.LastFrozenEntityId()
	if snapshotLastId > lastId {
		return snapshotLastId, true, nil
	}

	return lastId, ok, err
}

func (s *MilestoneSnapshotStore) Entity(ctx context.Context, id uint64) (*Milestone, bool, error) {
	entity, ok, err := s.EntityStore.Entity(ctx, id)

	if ok {
		return entity, ok, err
	}

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], id)

	tx := s.snapshots.ViewType(s.SnapType())
	defer tx.Close()
	segments := tx.Segments

	for i := len(segments) - 1; i >= 0; i-- {
		sn := segments[i]
		idx := sn.Src().Index()

		if idx == nil {
			continue
		}

		if idx.KeyCount() == 0 {
			continue
		}

		if id < idx.BaseDataID() {
			continue
		}

		offset := idx.OrdinalLookup(id - idx.BaseDataID())
		gg := sn.Src().MakeGetter()
		gg.Reset(offset)
		result, _ := gg.Next(nil)

		var entity Milestone
		if err := json.Unmarshal(result, &entity); err != nil {
			return nil, false, err
		}

		return &entity, true, nil
	}

	err = fmt.Errorf("milestone %d not found", id)
	return nil, false, fmt.Errorf("%w: %w", ErrMilestoneNotFound, err)
}

func (s *MilestoneSnapshotStore) ValidateSnapshots(logger log.Logger, failFast bool) error {
	return validateSnapshots(logger, failFast, s.snapshots, s.SnapType(), generics.New[Milestone])
}

type CheckpointSnapshotStore struct {
	EntityStore[*Checkpoint]
	snapshots *RoSnapshots
}

func NewCheckpointSnapshotStore(base EntityStore[*Checkpoint], snapshots *RoSnapshots) *CheckpointSnapshotStore {
	return &CheckpointSnapshotStore{base, snapshots}
}

func (s *CheckpointSnapshotStore) RangeExtractor() snaptype.RangeExtractor {
	return snaptype.RangeExtractorFunc(
		func(ctx context.Context, blockFrom, blockTo uint64, firstKey snaptype.FirstKeyGetter, db kv.RoDB, chainConfig *chain.Config, collect func([]byte) error, workers int, lvl log.Lvl, logger log.Logger) (uint64, error) {
			return s.SnapType().RangeExtractor().Extract(ctx, blockFrom, blockTo, firstKey,
				s.EntityStore.(*mdbxEntityStore[*Checkpoint]).db.RoDB(), chainConfig, collect, workers, lvl, logger)
		})
}

func (s *CheckpointSnapshotStore) Prepare(ctx context.Context) error {
	if err := s.EntityStore.Prepare(ctx); err != nil {
		return err
	}

	return <-s.snapshots.Ready(ctx)
}

func (s *CheckpointSnapshotStore) WithTx(tx kv.Tx) EntityStore[*Checkpoint] {
	return &CheckpointSnapshotStore{txEntityStore[*Checkpoint]{s.EntityStore.(*mdbxEntityStore[*Checkpoint]), tx}, s.snapshots}
}

func (s *CheckpointSnapshotStore) LastCheckpointId(ctx context.Context, tx kv.Tx) (uint64, bool, error) {
	lastId, ok, err := s.EntityStore.LastEntityId(ctx)

	snapshotLastCheckpointId := s.LastFrozenEntityId()

	if snapshotLastCheckpointId > lastId {
		return snapshotLastCheckpointId, true, nil
	}

	return lastId, ok, err
}

func (s *CheckpointSnapshotStore) Entity(ctx context.Context, id uint64) (*Checkpoint, bool, error) {
	entity, ok, err := s.EntityStore.Entity(ctx, id)

	if ok {
		return entity, ok, err
	}

	tx := s.snapshots.ViewType(s.SnapType())
	defer tx.Close()
	segments := tx.Segments

	for i := len(segments) - 1; i >= 0; i-- {
		sn := segments[i]
		index := sn.Src().Index()

		if index == nil || index.KeyCount() == 0 || id < index.BaseDataID() {
			continue
		}

		offset := index.OrdinalLookup(id - index.BaseDataID())
		gg := sn.Src().MakeGetter()
		gg.Reset(offset)
		result, _ := gg.Next(nil)

		var entity Checkpoint
		if err := json.Unmarshal(result, &entity); err != nil {
			return nil, false, err
		}

		return &entity, true, nil
	}

	return nil, false, fmt.Errorf("checkpoint %d: %w", id, ErrCheckpointNotFound)
}

func (s *CheckpointSnapshotStore) LastFrozenEntityId() uint64 {
	if s.snapshots == nil {
		return 0
	}

	tx := s.snapshots.ViewType(s.SnapType())
	defer tx.Close()
	segments := tx.Segments

	if len(segments) == 0 {
		return 0
	}
	// find the last segment which has a built non-empty index
	var lastSegment *snapshotsync.VisibleSegment
	for i := len(segments) - 1; i >= 0; i-- {
		if segments[i].Src().Index() != nil {
			gg := segments[i].Src().MakeGetter()
			if gg.HasNext() {
				lastSegment = segments[i]
				break
			}
		}
	}

	if lastSegment == nil {
		return 0
	}

	index := lastSegment.Src().Index()

	return index.BaseDataID() + index.KeyCount() - 1
}

func (s *CheckpointSnapshotStore) ValidateSnapshots(logger log.Logger, failFast bool) error {
	return validateSnapshots(logger, failFast, s.snapshots, s.SnapType(), generics.New[Checkpoint])
}

func validateSnapshots[T Entity](logger log.Logger, failFast bool, snaps *RoSnapshots, t snaptype.Type, makeEntity func() T) error {
	tx := snaps.ViewType(t)
	defer tx.Close()

	segs := tx.Segments
	if len(segs) == 0 {
		return errors.New("no segments")
	}

	var accumulatedErr error
	var prev *T
	for _, seg := range segs {
		idx := seg.Src().Index()
		if idx == nil || idx.KeyCount() == 0 {
			continue
		}

		segGetter := seg.Src().MakeGetter()
		for segGetter.HasNext() {
			buf, _ := segGetter.Next(nil)
			entity := makeEntity()
			if err := json.Unmarshal(buf, entity); err != nil {
				return err
			}

			logger.Trace("validating entity", "id", entity.RawId(), "kind", reflect.TypeOf(entity))

			if prev == nil {
				prev = &entity
				continue
			}

			expectedId := (*prev).RawId() + 1
			if expectedId == entity.RawId() {
				prev = &entity
				continue
			}

			if accumulatedErr == nil {
				accumulatedErr = errors.New("missing entities")
			}

			accumulatedErr = fmt.Errorf("%w: [%d, %d)", accumulatedErr, expectedId, entity.RawId())
			if failFast {
				return accumulatedErr
			}

			prev = &entity
		}
	}

	return accumulatedErr
}
