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
	"fmt"
	"maps"
	"sync"

	dir2 "github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/db/kv"
)

// VLogSet manages vlog writers with separation between dirty (all) and visible (ready) vlogs
// Similar to how FilesItem manages files with dirtyFiles and visibleFiles
type VLogSet struct {
	// All vlog writers (dirty + visible)
	// Contains both: work-in-progress vlogs and ready vlogs
	dirty        map[kv.Step]*VLogFile
	dirtyWriters map[kv.Step]*VLogWriter

	// Subset of dirty that are ready for reading
	// Writers that have been fsynced and can be safely read
	visible        map[kv.Step]*VLogFile
	visibleWriters map[kv.Step]*VLogWriter

	dir  string // directory where vlog files are stored
	lock sync.RWMutex
}

// NewVLogSet creates a new VLogSet
func NewVLogSet(dir string) *VLogSet {
	dir2.MustExist(dir)
	return &VLogSet{
		dir:            dir,
		dirty:          make(map[kv.Step]*VLogFile),
		visible:        make(map[kv.Step]*VLogFile),
		dirtyWriters:   make(map[kv.Step]*VLogWriter),
		visibleWriters: make(map[kv.Step]*VLogWriter),
	}
}

// GetOrCreateWriter gets an existing vlog writer or creates a new one for the given step
// New writers are added to dirty set only
func (s *VLogSet) GetOrCreateWriter(step kv.Step) (*VLogWriter, error) {
	s.lock.RLock()
	if writer, exists := s.visibleWriters[step]; exists {
		s.lock.RUnlock()
		return writer, nil
	}
	s.lock.RUnlock()

	// Create new writer in the configured directory
	vlogPath := vlogPathForStep(s.dir, step)
	vlog, err := CreateVLogFile(vlogPath)
	if err != nil {
		return nil, err
	}
	writer, err := NewVLogWriter(vlog)
	if err != nil {
		return nil, err
	}

	s.lock.Lock()
	s.dirty[step] = vlog
	s.dirtyWriters[step] = writer
	s.lock.Unlock()

	s.RecalcVisible()

	return writer, nil
}

// Reader returns a visible vlog writer for the given step (read-only access)
func (s *VLogSet) Reader(step kv.Step) *VLogFile {
	s.lock.RLock()
	defer s.lock.RUnlock()
	v, ok := s.visible[step]
	if !ok {
		panic(fmt.Sprintf("VLogSet Reader for step %v does not exist", step))
	}
	return v
}

// FsyncAll fsyncs all dirty vlog writers
// Returns the first error encountered, if any
func (s *VLogSet) FsyncAll() error {
	s.lock.RLock()
	defer s.lock.RUnlock()

	for _, writer := range s.dirtyWriters {
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

	s.visible = make(map[kv.Step]*VLogFile, len(s.dirty))
	maps.Copy(s.visible, s.dirty)

	s.visibleWriters = make(map[kv.Step]*VLogWriter, len(s.dirtyWriters))
	maps.Copy(s.visibleWriters, s.dirtyWriters)
}

// Close closes all vlog writers
func (s *VLogSet) Close() {
	s.lock.Lock()
	defer s.lock.Unlock()

	for _, f := range s.dirty {
		f.Close()
	}
	for _, f := range s.dirtyWriters {
		f.Close()
	}
}
