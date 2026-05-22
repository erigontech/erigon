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

package harness

import (
	"sync"

	"github.com/erigontech/erigon/node/components/storage/flow"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// MockStorage is the harness-side implementation of flow.Storage. It holds
// an in-memory Inventory and records every RecordFile call so tests can
// assert what the orchestrator wrote without reaching back into the
// inventory's concrete state.
//
// An optional RecordHook fires on every RecordFile before the file is
// committed to the inventory — returning a non-nil error suppresses the
// commit and propagates to the orchestrator. Use this to inject storage
// failures and verify the orchestrator's rollback behaviour.
type MockStorage struct {
	mu         sync.Mutex
	inv        *snapshot.Inventory
	recorded   []*snapshot.FileEntry
	recordHook func(*snapshot.FileEntry) error
}

// NewMockStorage constructs a MockStorage wrapping a fresh inventory.
func NewMockStorage() *MockStorage {
	return &MockStorage{inv: snapshot.NewInventory()}
}

// NewMockStorageWithInventory constructs a MockStorage using the provided
// inventory. Useful when a scenario seeds the local node's initial state
// via a fixture before the orchestrator starts.
func NewMockStorageWithInventory(inv *snapshot.Inventory) *MockStorage {
	return &MockStorage{inv: inv}
}

// SetRecordHook installs a predicate invoked on every RecordFile. Returning
// a non-nil error from the hook causes RecordFile to return that error
// without committing the entry to the inventory. Passing nil clears the
// hook.
func (m *MockStorage) SetRecordHook(fn func(*snapshot.FileEntry) error) {
	m.mu.Lock()
	m.recordHook = fn
	m.mu.Unlock()
}

// Inventory returns the in-memory inventory this storage wraps.
func (m *MockStorage) Inventory() *snapshot.Inventory { return m.inv }

// RecordFile commits the entry to the inventory after running any hook.
// Safe for concurrent invocation.
//
// The hook runs outside the lock so a hook that inspects MockStorage state
// (e.g. Recorded) doesn't deadlock. The commit (append + AddFile) runs
// inside a single critical section so concurrent Recorded() readers never
// observe the slice updated without the inventory having the same entry.
func (m *MockStorage) RecordFile(e *snapshot.FileEntry) error {
	m.mu.Lock()
	hook := m.recordHook
	m.mu.Unlock()
	if hook != nil {
		if err := hook(e); err != nil {
			return err
		}
	}
	m.mu.Lock()
	m.recorded = append(m.recorded, e)
	m.inv.AddFile(e)
	m.mu.Unlock()
	return nil
}

// Recorded returns a snapshot of every file RecordFile has been invoked with
// that was not rejected by the hook. Tests may assert length, order, or
// per-entry fields.
func (m *MockStorage) Recorded() []*snapshot.FileEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*snapshot.FileEntry, len(m.recorded))
	copy(out, m.recorded)
	return out
}

var _ flow.Storage = (*MockStorage)(nil)
