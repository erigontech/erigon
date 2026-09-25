// Copyright 2026 The Erigon Authors
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

package runner

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/internal/commitmenttest"
)

type ReadGateFunc func(context.Context, []byte) error

type ContextSpec struct {
	Borrowed         bool
	Owned            bool
	ForbidStateReads bool
	CaptureDeltas    bool
	CheckPrevious    bool
	BeforeRead       ReadGateFunc
	ReadError        error
	PutError         error
}

type Counts struct {
	BranchReads  int
	AccountReads int
	StorageReads int
	Writes       int
	Readers      int
}

type Memory struct {
	spec    ContextSpec
	store   *memoryStore
	mu      sync.Mutex
	scratch []byte
	ctx     context.Context
}

type memoryStore struct {
	sync.Mutex
	records map[string][]byte
	state   commitmenttest.State
	counts  Counts
	deltas  []commitment.BranchDelta
}

func NewMemory(spec ContextSpec) *Memory {
	return &Memory{spec: spec, ctx: context.Background(), store: &memoryStore{records: make(map[string][]byte), state: make(commitmenttest.State)}}
}

func (m *Memory) Open(ctx context.Context) (commitment.PatriciaContext, func()) {
	m.store.Lock()
	m.store.counts.Readers++
	m.store.Unlock()
	reader := &Memory{spec: m.spec, store: m.store, ctx: ctx}
	if m.spec.Owned {
		return ownedMemory{reader}, nil
	}
	return reader, nil
}

type ownedMemory struct{ *Memory }

func (m ownedMemory) BranchOwned(key []byte) ([]byte, kv.Step, error) { return m.read(key) }

func (m *Memory) read(key []byte) ([]byte, kv.Step, error) {
	if m.spec.BeforeRead != nil {
		if err := m.spec.BeforeRead(m.ctx, key); err != nil {
			return nil, 0, err
		}
	}
	m.store.Lock()
	defer m.store.Unlock()
	m.store.counts.BranchReads++
	if m.spec.ReadError != nil {
		return nil, 0, m.spec.ReadError
	}
	return m.store.records[string(key)], 0, nil
}

func (m *Memory) Branch(key []byte) ([]byte, kv.Step, error) {
	data, step, err := m.read(key)
	if err != nil || data == nil {
		return nil, step, err
	}
	if !m.spec.Borrowed {
		return bytes.Clone(data), step, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.scratch == nil {
		m.scratch = make([]byte, 0, len(data))
	}
	m.scratch = append(m.scratch[:0], data...)
	return m.scratch, step, nil
}

func (m *Memory) PutBranch(key, data, prev []byte) error {
	m.store.Lock()
	defer m.store.Unlock()
	if m.spec.PutError != nil {
		return m.spec.PutError
	}
	if m.spec.CheckPrevious && !bytes.Equal(m.store.records[string(key)], prev) {
		return fmt.Errorf("previous value mismatch for %x", key)
	}
	m.store.counts.Writes++
	if m.spec.CaptureDeltas {
		m.store.deltas = append(m.store.deltas, commitment.BranchDelta{Key: bytes.Clone(key), Data: bytes.Clone(data), Prev: bytes.Clone(prev)})
	}
	if len(data) == 0 {
		data = []byte{}
	}
	m.store.records[string(key)] = bytes.Clone(data)
	return nil
}

func (m *Memory) Account(key []byte) (*commitment.Update, error) { return m.stateRead(key, true) }
func (m *Memory) Storage(key []byte) (*commitment.Update, error) { return m.stateRead(key, false) }
func (m *Memory) stateRead(key []byte, account bool) (*commitment.Update, error) {
	m.store.Lock()
	defer m.store.Unlock()
	if account {
		m.store.counts.AccountReads++
	} else {
		m.store.counts.StorageReads++
	}
	if m.spec.ForbidStateReads {
		return nil, fmt.Errorf("unexpected state read: %x", key)
	}
	op, ok := m.store.state[string(key)]
	if !ok {
		return &commitment.Update{Flags: commitment.DeleteUpdate}, nil
	}
	return Update(op), nil
}

func (m *Memory) Apply(ops []commitmenttest.Op) {
	m.store.Lock()
	defer m.store.Unlock()
	m.store.state.Apply(ops)
}

func (m *Memory) Counts() Counts {
	m.store.Lock()
	defer m.store.Unlock()
	return m.store.counts
}

func (m *Memory) Records() map[string][]byte {
	m.store.Lock()
	defer m.store.Unlock()
	out := make(map[string][]byte, len(m.store.records))
	for key, value := range m.store.records {
		out[key] = bytes.Clone(value)
	}
	return out
}

func (m *Memory) Deltas() []commitment.BranchDelta {
	m.store.Lock()
	defer m.store.Unlock()
	out := make([]commitment.BranchDelta, len(m.store.deltas))
	for i, d := range m.store.deltas {
		out[i] = commitment.BranchDelta{Key: bytes.Clone(d.Key), Data: bytes.Clone(d.Data), Prev: bytes.Clone(d.Prev)}
	}
	return out
}
