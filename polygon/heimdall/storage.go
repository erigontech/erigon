package heimdall

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/turbo/services"
)

// Generate all mocks in file
//go:generate mockgen -destination=./storage_mock.go -package=heimdall -source=./storage.go

type SpanReader interface {
	LastSpanId(ctx context.Context) (SpanId, bool, error)
	GetSpan(ctx context.Context, spanId SpanId) (*Span, error)
}

type SpanWriter interface {
	PutSpan(ctx context.Context, span *Span) error
}

type SpanStore interface {
	SpanReader
	SpanWriter
}

type MilestoneReader interface {
	LastMilestoneId(ctx context.Context) (MilestoneId, bool, error)
	GetMilestone(ctx context.Context, milestoneId MilestoneId) (*Milestone, error)
}

type MilestoneWriter interface {
	PutMilestone(ctx context.Context, milestoneId MilestoneId, milestone *Milestone) error
}

type MilestoneStore interface {
	MilestoneReader
	MilestoneWriter
}

type CheckpointReader interface {
	LastCheckpointId(ctx context.Context) (CheckpointId, bool, error)
	GetCheckpoint(ctx context.Context, checkpointId CheckpointId) (*Checkpoint, error)
}

type CheckpointWriter interface {
	PutCheckpoint(ctx context.Context, checkpointId CheckpointId, checkpoint *Checkpoint) error
}

type CheckpointStore interface {
	CheckpointReader
	CheckpointWriter
}

type Store interface {
	SpanStore
	MilestoneStore
	CheckpointStore
}

type reader interface {
	services.BorEventReader
	services.BorSpanReader
	services.BorCheckpointReader
	services.BorMilestoneReader
}

type blockReaderStore struct {
	reader reader
	tx     kv.Tx
}

var _ Store = blockReaderStore{}

func NewBlockReaderStore(reader reader, tx kv.Tx) blockReaderStore {
	return blockReaderStore{reader: reader, tx: tx}
}

func (io blockReaderStore) LastSpanId(ctx context.Context) (SpanId, bool, error) {
	spanId, ok, err := io.reader.LastSpanId(ctx, io.tx)
	return SpanId(spanId), ok, err
}

func (io blockReaderStore) GetSpan(ctx context.Context, spanId SpanId) (*Span, error) {
	spanBytes, err := io.reader.Span(ctx, io.tx, uint64(spanId))

	if err != nil {
		return nil, err
	}

	var span Span

	if err := json.Unmarshal(spanBytes, &span); err != nil {
		return nil, err
	}

	return &span, nil
}

func (io blockReaderStore) PutSpan(ctx context.Context, span *Span) error {
	tx, ok := io.tx.(kv.RwTx)

	if !ok {
		return fmt.Errorf("span writer failed: tx is read only")
	}

	spanBytes, err := json.Marshal(span)

	if err != nil {
		return err
	}

	var spanIdBytes [8]byte
	binary.BigEndian.PutUint64(spanIdBytes[:], uint64(span.Id))

	return tx.Put(kv.BorSpans, spanIdBytes[:], spanBytes)
}

func (io blockReaderStore) LastMilestoneId(ctx context.Context) (MilestoneId, bool, error) {
	id, ok, err := io.reader.LastMilestoneId(ctx, io.tx)
	return MilestoneId(id), ok, err
}

func (io blockReaderStore) GetMilestone(ctx context.Context, milestoneId MilestoneId) (*Milestone, error) {
	milestoneBytes, err := io.reader.Milestone(ctx, io.tx, uint64(milestoneId))

	if err != nil {
		return nil, err
	}

	var milestone Milestone

	if err := json.Unmarshal(milestoneBytes, &milestone); err != nil {
		return nil, err
	}

	return &milestone, nil
}

func (io blockReaderStore) PutMilestone(ctx context.Context, milestoneId MilestoneId, milestone *Milestone) error {
	tx, ok := io.tx.(kv.RwTx)

	if !ok {
		return fmt.Errorf("span writer failed: tx is read only")
	}

	spanBytes, err := json.Marshal(milestone)

	if err != nil {
		return err
	}

	var spanIdBytes [8]byte
	binary.BigEndian.PutUint64(spanIdBytes[:], uint64(milestoneId))

	return tx.Put(kv.BorMilestones, spanIdBytes[:], spanBytes)
}

func (io blockReaderStore) LastCheckpointId(ctx context.Context) (CheckpointId, bool, error) {
	id, ok, err := io.reader.LastCheckpointId(ctx, io.tx)
	return CheckpointId(id), ok, err
}

func (io blockReaderStore) GetCheckpoint(ctx context.Context, checkpointId CheckpointId) (*Checkpoint, error) {
	checkpointBytes, err := io.reader.Milestone(ctx, io.tx, uint64(checkpointId))

	if err != nil {
		return nil, err
	}

	var checkpoint Checkpoint

	if err := json.Unmarshal(checkpointBytes, &checkpoint); err != nil {
		return nil, err
	}

	return &checkpoint, nil
}

func (io blockReaderStore) PutCheckpoint(ctx context.Context, checkpointId CheckpointId, checkpoint *Checkpoint) error {
	tx, ok := io.tx.(kv.RwTx)

	if !ok {
		return fmt.Errorf("span writer failed: tx is read only")
	}

	spanBytes, err := json.Marshal(checkpoint)

	if err != nil {
		return err
	}

	var spanIdBytes [8]byte
	binary.BigEndian.PutUint64(spanIdBytes[:], uint64(checkpointId))

	return tx.Put(kv.BorCheckpoints, spanIdBytes[:], spanBytes)
}
