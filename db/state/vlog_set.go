// Copyright 2025 The Erigon Authors
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

package state

import (
	"sync"

	"github.com/erigontech/erigon/db/kv"
)

// VLogSet manages vlog writers with separation between dirty (all) and visible (ready) vlogs
// Similar to how FilesItem manages files with dirtyFiles and visibleFiles
type VLogSet struct {
	// All vlog writers (dirty + visible)
	// Contains both: work-in-progress vlogs and ready vlogs
	dirty map[kv.Step]*VLogWriter

	// Subset of dirty that are ready for reading
	// Writers that have been fsynced and can be safely read
	visible map[kv.Step]*VLogWriter

	dir  string // directory where vlog files are stored
	lock sync.RWMutex
}

// NewVLogSet creates a new VLogSet
func NewVLogSet(dir string) *VLogSet {
	return &VLogSet{
		dir:     dir,
		dirty:   make(map[kv.Step]*VLogWriter),
		visible: make(map[kv.Step]*VLogWriter),
	}
}

// GetOrCreate gets an existing vlog writer or creates a new one for the given step
// New writers are added to dirty set only
func (s *VLogSet) GetOrCreate(step kv.Step) (*VLogWriter, error) {
	s.lock.RLock()
	if writer, exists := s.dirty[step]; exists {
		s.lock.RUnlock()
		return writer, nil
	}
	s.lock.RUnlock()

	// Need to create new writer
	s.lock.Lock()
	defer s.lock.Unlock()

	// Double-check after acquiring write lock
	if writer, exists := s.dirty[step]; exists {
		return writer, nil
	}

	// Create new writer in the configured directory
	vlogPath := vlogPathForStep(s.dir, step)
	writer, err := CreateVLogWriter(vlogPath)
	if err != nil {
		return nil, err
	}

	// Add to dirty set only (not visible until fsynced)
	s.dirty[step] = writer

	return writer, nil
}

// Get returns a vlog writer for the given step from dirty set
// Used during flush to access writers created earlier in the transaction
func (s *VLogSet) Get(step kv.Step) *VLogWriter {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.dirty[step]
}

// GetVisible returns a visible vlog writer for the given step (read-only access)
func (s *VLogSet) GetVisible(step kv.Step) *VLogWriter {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.visible[step]
}

// FsyncAll fsyncs all dirty vlog writers
// Returns the first error encountered, if any
func (s *VLogSet) FsyncAll() error {
	s.lock.RLock()
	defer s.lock.RUnlock()

	for _, writer := range s.dirty {
		if err := writer.Fsync(); err != nil {
			return err
		}
	}
	return nil
}

// RecalcVisible recalculates the visible set from dirty set
// Called after fsync to promote dirty writers to visible
func (s *VLogSet) RecalcVisible() {
	s.lock.Lock()
	defer s.lock.Unlock()

	// For now, simple strategy: all dirty vlogs become visible
	// In future, can add logic to check if vlog is properly fsynced, indexed, etc.
	s.visible = make(map[kv.Step]*VLogWriter, len(s.dirty))
	for step, writer := range s.dirty {
		s.visible[step] = writer
	}
}

// Clone creates a shallow copy of the VLogSet for transaction isolation
// Each transaction gets its own view of vlogs
func (s *VLogSet) Clone() *VLogSet {
	s.lock.RLock()
	defer s.lock.RUnlock()

	clone := &VLogSet{
		dir:     s.dir,
		dirty:   make(map[kv.Step]*VLogWriter, len(s.dirty)),
		visible: make(map[kv.Step]*VLogWriter, len(s.visible)),
	}

	// Copy pointers (shallow copy - writers are shared)
	for step, writer := range s.dirty {
		clone.dirty[step] = writer
	}
	for step, writer := range s.visible {
		clone.visible[step] = writer
	}

	return clone
}

// AddDirty adds a vlog writer to the dirty set
func (s *VLogSet) AddDirty(step kv.Step, writer *VLogWriter) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.dirty[step] = writer
}

// AddVisible adds a vlog writer to both dirty and visible sets
func (s *VLogSet) AddVisible(step kv.Step, writer *VLogWriter) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.dirty[step] = writer
	s.visible[step] = writer
}

// Close closes all vlog writers
func (s *VLogSet) Close() {
	s.lock.Lock()
	defer s.lock.Unlock()

	for _, writer := range s.dirty {
		writer.Close()
	}

	s.dirty = make(map[kv.Step]*VLogWriter)
	s.visible = make(map[kv.Step]*VLogWriter)
}
