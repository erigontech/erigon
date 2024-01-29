package heimdall

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/turbo/services"
)

type SpanReader interface {
	LastSpanId(ctx context.Context) (SpanId, bool, error)
	ReadSpan(ctx context.Context, spanId SpanId) (*Span, error)
}

type SpanWriter interface {
	WriteSpan(ctx context.Context, span *Span) error
}

type SpanIO interface {
	SpanReader
	SpanWriter
}

type MilestoneReader interface {
	LastMilestoneId(ctx context.Context) (MilestoneId, bool, error)
	ReadMilestone(ctx context.Context, milestoneId MilestoneId) (*Milestone, error)
}

type MilestoneWriter interface {
	WriteMilestone(ctx context.Context, milestoneId MilestoneId, milestone *Milestone) error
}

type MilestoneIO interface {
	MilestoneReader
	MilestoneWriter
}

type CheckpointReader interface {
	LastCheckpointId(ctx context.Context) (CheckpointId, bool, error)
	ReadCheckpoint(ctx context.Context, checkpointId CheckpointId) (*Checkpoint, error)
}

type CheckpointWriter interface {
	WriteCheckpoint(ctx context.Context, checkpointId CheckpointId, checkpoint *Checkpoint) error
}

type CheckpointIO interface {
	CheckpointReader
	CheckpointWriter
}

type IO interface {
	SpanIO
	MilestoneIO
	CheckpointIO
}

type reader interface {
	services.BorEventReader
	services.BorSpanReader
	services.BorMilestoneReader
	services.BorCheckpointReader
}

type blockReaderIO struct {
	reader reader
	tx     kv.Tx
}

func NewBlockReaderIO(reader reader, tx kv.Tx) blockReaderIO {
	return blockReaderIO{reader: reader, tx: tx}
}

func (io blockReaderIO) LastSpanId(ctx context.Context) (uint64, bool, error) {
	return io.reader.LastSpanId(ctx, io.tx)
}

func (io blockReaderIO) ReadSpan(ctx context.Context, spanId SpanId) (*Span, error) {
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

func (io blockReaderIO) WriteSpan(ctx context.Context, span *Span) error {
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

func (io blockReaderIO) LastMilestoneId(ctx context.Context) (MilestoneId, bool, error) {
	id, ok, err := io.reader.LastMilestoneId(ctx, io.tx)
	return MilestoneId(id), ok, err
}

func (io blockReaderIO) ReadMilestone(ctx context.Context, milestoneId MilestoneId) (*Milestone, error) {
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

func (io blockReaderIO) WriteMilestone(ctx context.Context, milestoneId MilestoneId, milestone *Milestone) error {
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

func (io blockReaderIO) LastCheckpointId(ctx context.Context) (CheckpointId, bool, error) {
	id, ok, err := io.reader.LastCheckpointId(ctx, io.tx)
	return CheckpointId(id), ok, err
}

func (io blockReaderIO) ReadCheckpoint(ctx context.Context, checkpointId CheckpointId) (*Checkpoint, error) {
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

func (io blockReaderIO) WriteCheckpoint(ctx context.Context, checkpointId CheckpointId, checkpoint *Checkpoint) error {
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
