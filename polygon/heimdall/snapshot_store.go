package heimdall

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/common/generics"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/execution/chain"
)

var (
	ErrSpanNotFound = errors.New("span not found")
)

func NewSnapshotStore(base Store, snapshots *RoSnapshots) *SnapshotStore {
	return &SnapshotStore{
		Store:                       base,
		checkpoints:                 NewCheckpointSnapshotStore(base.Checkpoints(), snapshots),
		milestones:                  base.Milestones(),
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

	err := <-s.snapshots.Ready(ctx)
	if err != nil {
		return err
	}

	err = s.buildSpanIndexFromSnapshots(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (s *SpanSnapshotStore) buildSpanIndexFromSnapshots(ctx context.Context) error {
	rangeIndex := s.RangeIndex()
	rangeIndexer, ok := rangeIndex.(RangeIndexer)
	if !ok {
		return errors.New("could not cast RangeIndex to RangeIndexer")
	}
	lastBlockNumInIndex, ok, err := rangeIndexer.Last(ctx)
	if err != nil {
		return err
	}
	if !ok { // index table is empty
		lastBlockNumInIndex = 0
	}

	lastSpanIdInIndex, ok, err := rangeIndex.Lookup(ctx, lastBlockNumInIndex)
	if err != nil {
		return err
	}

	if !ok { // index table is empty
		lastSpanIdInIndex = 0
	}

	updateSpanIndexFunc := func(span Span) (stop bool, err error) {
		// this is already written to index
		if span.Id <= SpanId(lastSpanIdInIndex) {
			return true, nil // we can stop because all subsequent span ids will already be in the SpanIndex
		}
		err = rangeIndexer.Put(ctx, span.BlockNumRange(), uint64(span.Id))
		if err != nil {
			return true, nil // happy case, we can continue updating
		} else {
			return false, err // we need to stop if we encounter an error, so that the function doesn't get called again
		}
	}
	// fill the index walking backwards from
	return s.snapshotsReverseForEach(updateSpanIndexFunc)
}

// Walk each span in the snapshots from last to first and apply function f as long as no error or stop condition is encountered
func (s *SpanSnapshotStore) snapshotsReverseForEach(f func(span Span) (stop bool, err error)) error {
	if s.snapshots == nil {
		return nil
	}

	tx := s.snapshots.ViewType(s.SnapType())
	defer tx.Close()
	segments := tx.Segments
	// walk the segment files backwards
	for i := len(segments) - 1; i >= 0; i-- {
		sn := segments[i]
		idx := sn.Src().Index()
		if idx == nil || idx.KeyCount() == 0 {
			continue
		}
		keyCount := idx.KeyCount()
		// walk the segment file backwards
		for j := int(keyCount - 1); j >= 0; j-- {
			offset := idx.OrdinalLookup(uint64(j))
			gg := sn.Src().MakeGetter()
			gg.Reset(offset)
			result, _ := gg.Next(nil)
			var span Span
			err := json.Unmarshal(result, &span)
			if err != nil {
				return err
			}
			stop, err := f(span)
			if err != nil {
				return err
			}
			if stop {
				return nil
			}
		}
	}
	return nil
}

func (s *SpanSnapshotStore) WithTx(tx kv.Tx) EntityStore[*Span] {
	return &SpanSnapshotStore{txEntityStore[*Span]{s.EntityStore.(*mdbxEntityStore[*Span]), tx}, s.snapshots}
}

func (s *SpanSnapshotStore) RangeExtractor() snaptype.RangeExtractor {
	return snaptype.RangeExtractorFunc(
		func(ctx context.Context, blockFrom, blockTo uint64, firstKey snaptype.FirstKeyGetter, db kv.RoDB, chainConfig *chain.Config, collect func([]byte) error, workers int, lvl log.Lvl, logger log.Logger, hashResolver snaptype.BlockHashResolver) (uint64, error) {
			return s.SnapType().RangeExtractor().Extract(ctx, blockFrom, blockTo, firstKey,
				s.EntityStore.(*mdbxEntityStore[*Span]).db.RoDB(), chainConfig, collect, workers, lvl, logger, hashResolver)
		})
}

func (s *SpanSnapshotStore) LastFrozenEntityId() (uint64, bool, error) {
	if s.snapshots == nil {
		return 0, false, nil
	}

	tx := s.snapshots.ViewType(s.SnapType())
	defer tx.Close()
	segments := tx.Segments

	if len(segments) == 0 {
		return 0, false, nil
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
		return 0, false, nil
	}

	idx := lastSegment.Src().Index()
	offset := idx.OrdinalLookup(idx.KeyCount() - 1) // check for the last element in this last seg file
	gg := lastSegment.Src().MakeGetter()
	gg.Reset(offset)
	result, _ := gg.Next(nil)

	var span Span
	if err := json.Unmarshal(result, &span); err != nil {
		return 0, false, err
	}

	return uint64(span.Id), true, nil
}

func (s *SpanSnapshotStore) Entity(ctx context.Context, id uint64) (*Span, bool, error) {

	lastSpanIdInSnapshots, found, err := s.LastFrozenEntityId()
	if err != nil {
		return nil, false, fmt.Errorf("could not load last span id in snapshots: %w", err)
	}

	if !found || id > lastSpanIdInSnapshots { // the span with this id is in MDBX and not in snapshots
		return s.EntityStore.Entity(ctx, id)
	}

	tx := s.snapshots.ViewType(s.SnapType())
	defer tx.Close()
	segments := tx.Segments

	for i := len(segments) - 1; i >= 0; i-- {
		sn := segments[i]
		idx := sn.Src().Index()

		if idx == nil || idx.KeyCount() == 0 {
			continue
		}

		gg := sn.Src().MakeGetter()
		firstOffset := idx.OrdinalLookup(0)
		gg.Reset(firstOffset)
		firstSpanRaw, _ := gg.Next(nil)
		var firstSpanInSeg Span
		if err := json.Unmarshal(firstSpanRaw, &firstSpanInSeg); err != nil {
			return nil, false, err
		}
		// skip : we need to look in an earlier .seg file
		if id < uint64(firstSpanInSeg.Id) {
			continue
		}

		offset := idx.OrdinalLookup(id - idx.BaseDataID())
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
	if err != nil {
		return lastId, false, err
	}

	if ok { // found in mdbx , return immediately
		return lastId, ok, nil
	}

	// check in snapshots
	return s.LastFrozenEntityId()
}

func (s *SpanSnapshotStore) LastEntity(ctx context.Context) (*Span, bool, error) {
	return snapshotStoreLastEntity(ctx, s)
}

func (s *SpanSnapshotStore) RangeFromBlockNum(ctx context.Context, startBlockNum uint64) ([]*Span, error) {
	return snapshotStoreRangeFromBlockNum(ctx, startBlockNum, s.EntityStore, s.snapshots, s.SnapType(), generics.New[Span])
}

func (s *SpanSnapshotStore) ValidateSnapshots(ctx context.Context, logger log.Logger, failFast bool) error {
	return validateSnapshots(ctx, logger, s.EntityStore, failFast, s.snapshots, s.SnapType(), generics.New[Span], 0, true)
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
		func(ctx context.Context, blockFrom, blockTo uint64, firstKey snaptype.FirstKeyGetter, db kv.RoDB, chainConfig *chain.Config, collect func([]byte) error, workers int, lvl log.Lvl, logger log.Logger, hashResolver snaptype.BlockHashResolver) (uint64, error) {
			return s.SnapType().RangeExtractor().Extract(ctx, blockFrom, blockTo, firstKey,
				s.EntityStore.(*mdbxEntityStore[*Milestone]).db.RoDB(), chainConfig, collect, workers, lvl, logger, hashResolver)
		})
}

func (s *MilestoneSnapshotStore) LastFrozenEntityId() (uint64, bool, error) {
	if s.snapshots == nil {
		return 0, false, nil
	}

	tx := s.snapshots.ViewType(s.SnapType())
	defer tx.Close()
	segments := tx.Segments

	if len(segments) == 0 {
		return 0, false, nil
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
		return 0, false, nil
	}

	index := lastSegment.Src().Index()

	return index.BaseDataID() + index.KeyCount() - 1, true, nil
}

func (s *MilestoneSnapshotStore) LastEntityId(ctx context.Context) (uint64, bool, error) {
	lastId, foundInMdbx, err := s.EntityStore.LastEntityId(ctx)
	if err != nil {
		return lastId, foundInMdbx, err
	}

	if foundInMdbx { // found in mdbx return immediately
		return lastId, true, nil
	}
	return s.LastFrozenEntityId()
}

func (s *MilestoneSnapshotStore) Entity(ctx context.Context, id uint64) (*Milestone, bool, error) {
	entity, ok, err := s.EntityStore.Entity(ctx, id)
	if ok {
		return entity, ok, err
	}

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

func (s *MilestoneSnapshotStore) LastEntity(ctx context.Context) (*Milestone, bool, error) {
	return snapshotStoreLastEntity(ctx, s)
}

func (s *MilestoneSnapshotStore) RangeFromBlockNum(ctx context.Context, startBlockNum uint64) ([]*Milestone, error) {
	return snapshotStoreRangeFromBlockNum(ctx, startBlockNum, s.EntityStore, s.snapshots, s.SnapType(), generics.New[Milestone])
}

func (s *MilestoneSnapshotStore) ValidateSnapshots(ctx context.Context, logger log.Logger, failFast bool) error {
	return validateSnapshots(ctx, logger, s.EntityStore, failFast, s.snapshots, s.SnapType(), generics.New[Milestone], 1, true)
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
		func(ctx context.Context, blockFrom, blockTo uint64, firstKey snaptype.FirstKeyGetter, db kv.RoDB, chainConfig *chain.Config, collect func([]byte) error, workers int, lvl log.Lvl, logger log.Logger, hashResolver snaptype.BlockHashResolver) (uint64, error) {
			return s.SnapType().RangeExtractor().Extract(ctx, blockFrom, blockTo, firstKey,
				s.EntityStore.(*mdbxEntityStore[*Checkpoint]).db.RoDB(), chainConfig, collect, workers, lvl, logger, hashResolver)
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

func (s *CheckpointSnapshotStore) LastEntityId(ctx context.Context) (uint64, bool, error) {
	lastId, foundInMdbx, err := s.EntityStore.LastEntityId(ctx)
	if err != nil {
		return lastId, foundInMdbx, err
	}
	if foundInMdbx { // found in MDBX return immediately
		return lastId, foundInMdbx, err
	}
	return s.LastFrozenEntityId()
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

func (s *CheckpointSnapshotStore) LastFrozenEntityId() (uint64, bool, error) {
	if s.snapshots == nil {
		return 0, false, nil
	}

	tx := s.snapshots.ViewType(s.SnapType())
	defer tx.Close()
	segments := tx.Segments

	if len(segments) == 0 {
		return 0, false, nil
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
		return 0, false, nil
	}

	index := lastSegment.Src().Index()

	return index.BaseDataID() + index.KeyCount() - 1, true, nil
}

func (s *CheckpointSnapshotStore) LastEntity(ctx context.Context) (*Checkpoint, bool, error) {
	return snapshotStoreLastEntity(ctx, s)
}

func (s *CheckpointSnapshotStore) RangeFromBlockNum(ctx context.Context, startBlockNum uint64) ([]*Checkpoint, error) {
	return snapshotStoreRangeFromBlockNum(ctx, startBlockNum, s.EntityStore, s.snapshots, s.SnapType(), generics.New[Checkpoint])
}

func (s *CheckpointSnapshotStore) ValidateSnapshots(ctx context.Context, logger log.Logger, failFast bool) error {
	return validateSnapshots(ctx, logger, s.EntityStore, failFast, s.snapshots, s.SnapType(), generics.New[Checkpoint], 1, true)
}

func validateSnapshots[T Entity](
	ctx context.Context,
	logger log.Logger,
	dbStore EntityStore[T],
	failFast bool,
	snaps *RoSnapshots,
	t snaptype.Type,
	makeEntity func() T,
	firstEntityId uint64,
	alsoCheckDb bool,
) error {
	tx := snaps.ViewType(t)
	defer tx.Close()

	segs := tx.Segments
	if len(segs) == 0 {
		return errors.New("no segments")
	}

	var accumulatedErr error
	expectedId := firstEntityId
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

			logger.Trace(
				"validating entity",
				"id", entity.RawId(),
				"kind", reflect.TypeOf(entity),
				"start", entity.BlockNumRange().Start,
				"end", entity.BlockNumRange().End,
				"segmentFrom", seg.From(),
				"segmentTo", seg.To(),
				"expectedId", expectedId,
			)

			if expectedId == entity.RawId() {
				expectedId++
				continue
			}

			if accumulatedErr == nil {
				accumulatedErr = errors.New("missing entities")
			}

			accumulatedErr = fmt.Errorf("%w: snap [%d, %d, %s)", accumulatedErr, expectedId, entity.RawId(), seg.Src().FileName())
			if failFast {
				return accumulatedErr
			}

			expectedId = entity.RawId() + 1
		}
	}

	if !alsoCheckDb {
		return accumulatedErr
	}

	// make sure snapshots connect with data in the db and there are no gaps at all
	lastInDb, ok, err := dbStore.LastEntityId(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return accumulatedErr
	}
	for i := expectedId; i <= lastInDb; i++ {
		_, ok, err := dbStore.Entity(ctx, i)
		if err != nil {
			return err
		}
		if ok {
			continue
		}
		// we've found a gap between snapshots and db
		if accumulatedErr == nil {
			accumulatedErr = errors.New("missing entities")
		}

		accumulatedErr = fmt.Errorf("%w: db [%d]", accumulatedErr, i)
		if failFast {
			return accumulatedErr
		}
	}

	return accumulatedErr
}

func snapshotStoreLastEntity[T Entity](ctx context.Context, store EntityStore[T]) (T, bool, error) {
	entityId, ok, err := store.LastEntityId(ctx)
	if err != nil || !ok {
		return generics.Zero[T](), false, err
	}

	return store.Entity(ctx, entityId)
}

func snapshotStoreRangeFromBlockNum[T Entity](
	ctx context.Context,
	startBlockNum uint64,
	dbStore EntityStore[T],
	snapshots *RoSnapshots,
	snapType snaptype.Type,
	makeEntity func() T,
) ([]T, error) {
	dbEntities, err := dbStore.RangeFromBlockNum(ctx, startBlockNum)
	if err != nil {
		return nil, err
	}
	if len(dbEntities) > 0 && dbEntities[0].BlockNumRange().End < startBlockNum {
		// this should not happen unless there is a bug in the db store
		return nil, fmt.Errorf("unexpected first entity end in db range: expected >= %d, got %d", startBlockNum, dbEntities[0].BlockNumRange().End)
	}
	if len(dbEntities) > 0 && dbEntities[0].BlockNumRange().Start <= startBlockNum {
		// all entities in the given range have been found in the db store
		return dbEntities, nil
	}

	// otherwise there may be some earlier entities in the range that are in the snapshot files
	// we start scanning backwards until entityEnd < startBlockNum, or we reach the end
	var fromEntityStart uint64
	if len(dbEntities) > 0 {
		fromEntityStart = dbEntities[0].BlockNumRange().Start
	}
	toEntityEnd := startBlockNum
	tx := snapshots.ViewType(snapType)
	defer tx.Close()
	segments := tx.Segments
	var snapshotEntities []T

OUTER:
	for i := len(segments) - 1; i >= 0; i-- {
		sn := segments[i]
		idx := sn.Src().Index()
		if idx == nil || idx.KeyCount() == 0 {
			continue
		}

		gg := sn.Src().MakeGetter()
		keyCount := idx.KeyCount()
		for j := int64(keyCount) - 1; j >= 0; j-- {
			offset := idx.OrdinalLookup(uint64(j))
			gg.Reset(offset)
			result, _ := gg.Next(nil)
			entity := makeEntity()
			if err := json.Unmarshal(result, &entity); err != nil {
				return nil, err
			}

			entityStart := entity.BlockNumRange().Start
			entityEnd := entity.BlockNumRange().End
			if fromEntityStart > 0 && entityStart >= fromEntityStart {
				continue
			} else if entityEnd < toEntityEnd {
				break OUTER
			} else {
				snapshotEntities = append(snapshotEntities, entity)
			}
		}
	}

	// prepend snapshot dbEntities that fall in the range
	slices.Reverse(snapshotEntities)
	return append(snapshotEntities, dbEntities...), nil
}
