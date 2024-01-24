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
	WriteMilestone(ctx context.Context, milestone *Milestone) error
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
	WriteCheckpoint(ctx context.Context, checkpoint *Checkpoint) error
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
