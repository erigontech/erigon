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
	"encoding/binary"
	"encoding/json"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/turbo/services"
)

// Generate all mocks in file
//go:generate mockgen -typed=true -destination=./store_mock.go -package=heimdall -source=./store.go

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

type ReadStore interface {
	SpanReader
	CheckpointReader
	MilestoneReader
}

type reader interface {
	services.BorEventReader
	services.BorSpanReader
	services.BorCheckpointReader
	services.BorMilestoneReader
}

func NewTxReadStore(reader reader, tx kv.Tx) ReadStore {
	return &txReadStore{
		reader: reader,
		tx:     tx,
	}
}

type txReadStore struct {
	reader reader
	tx     kv.Tx
}

func (s txReadStore) LastSpanId(ctx context.Context) (SpanId, bool, error) {
	spanId, ok, err := s.reader.LastSpanId(ctx, s.tx)
	return SpanId(spanId), ok, err
}

func (s txReadStore) GetSpan(ctx context.Context, spanId SpanId) (*Span, error) {
	spanBytes, err := s.reader.Span(ctx, s.tx, uint64(spanId))
	if err != nil {
		return nil, err
	}

	var span Span
	if err := json.Unmarshal(spanBytes, &span); err != nil {
		return nil, err
	}

	return &span, nil
}

func (s txReadStore) LastMilestoneId(ctx context.Context) (MilestoneId, bool, error) {
	id, ok, err := s.reader.LastMilestoneId(ctx, s.tx)
	return MilestoneId(id), ok, err
}

func (s txReadStore) GetMilestone(ctx context.Context, milestoneId MilestoneId) (*Milestone, error) {
	milestoneBytes, err := s.reader.Milestone(ctx, s.tx, uint64(milestoneId))
	if err != nil {
		return nil, err
	}

	var milestone Milestone
	if err := json.Unmarshal(milestoneBytes, &milestone); err != nil {
		return nil, err
	}

	return &milestone, nil
}

func (s txReadStore) LastCheckpointId(ctx context.Context) (CheckpointId, bool, error) {
	id, ok, err := s.reader.LastCheckpointId(ctx, s.tx)
	return CheckpointId(id), ok, err
}

func (s txReadStore) GetCheckpoint(ctx context.Context, checkpointId CheckpointId) (*Checkpoint, error) {
	checkpointBytes, err := s.reader.Checkpoint(ctx, s.tx, uint64(checkpointId))
	if err != nil {
		return nil, err
	}

	var checkpoint Checkpoint
	if err := json.Unmarshal(checkpointBytes, &checkpoint); err != nil {
		return nil, err
	}

	return &checkpoint, nil
}

func NewTxStore(reader reader, tx kv.RwTx) Store {
	return &txStore{
		ReadStore: NewTxReadStore(reader, tx),
		tx:        tx,
	}
}

type txStore struct {
	ReadStore
	tx kv.RwTx
}

func (s txStore) PutSpan(_ context.Context, span *Span) error {
	spanBytes, err := json.Marshal(span)
	if err != nil {
		return err
	}

	var spanIdBytes [8]byte
	binary.BigEndian.PutUint64(spanIdBytes[:], uint64(span.Id))

	return s.tx.Put(kv.BorSpans, spanIdBytes[:], spanBytes)
}

func (s txStore) PutCheckpoint(_ context.Context, checkpointId CheckpointId, checkpoint *Checkpoint) error {
	checkpointBytes, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}

	var checkpointIdBytes [8]byte
	binary.BigEndian.PutUint64(checkpointIdBytes[:], uint64(checkpointId))

	return s.tx.Put(kv.BorCheckpoints, checkpointIdBytes[:], checkpointBytes)
}

func (s txStore) PutMilestone(_ context.Context, milestoneId MilestoneId, milestone *Milestone) error {
	milestoneBytes, err := json.Marshal(milestone)
	if err != nil {
		return err
	}

	var milestoneIdBytes [8]byte
	binary.BigEndian.PutUint64(milestoneIdBytes[:], uint64(milestoneId))

	return s.tx.Put(kv.BorMilestones, milestoneIdBytes[:], milestoneBytes)
}

func NewNoopStore() Store {
	return &noopStore{}
}

type noopStore struct {
}

func (s noopStore) LastCheckpointId(context.Context) (CheckpointId, bool, error) {
	return 0, false, nil
}

func (s noopStore) GetCheckpoint(context.Context, CheckpointId) (*Checkpoint, error) {
	return nil, nil
}

func (s noopStore) PutCheckpoint(context.Context, CheckpointId, *Checkpoint) error {
	return nil
}

func (s noopStore) LastMilestoneId(context.Context) (MilestoneId, bool, error) {
	return 0, false, nil
}

func (s noopStore) GetMilestone(context.Context, MilestoneId) (*Milestone, error) {
	return nil, nil
}

func (s noopStore) PutMilestone(context.Context, MilestoneId, *Milestone) error {
	return nil
}

func (s noopStore) LastSpanId(context.Context) (SpanId, bool, error) {
	return 0, false, nil
}

func (s noopStore) GetSpan(context.Context, SpanId) (*Span, error) {
	return nil, nil
}

func (s noopStore) PutSpan(context.Context, *Span) error {
	return nil
}
