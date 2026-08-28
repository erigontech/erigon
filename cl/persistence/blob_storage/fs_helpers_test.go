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

package blob_storage

import (
	"errors"
	"os"
	"sync"

	"github.com/spf13/afero"
)

const opRemoveAll = "RemoveAll"

var errInducedFailure = errors.New("induced filesystem failure")

type removeFailingFs struct {
	afero.Fs
	failOn map[string]error
}

func newRemoveFailingFs(fs afero.Fs) *removeFailingFs {
	return &removeFailingFs{Fs: fs, failOn: map[string]error{}}
}

func (r *removeFailingFs) Remove(path string) error {
	if err, ok := r.failOn[path]; ok {
		return err
	}
	return r.Fs.Remove(path)
}

type countingFs struct {
	afero.Fs
	mu    sync.Mutex
	calls map[string][][]string
}

func newCountingFs(fs afero.Fs) *countingFs {
	return &countingFs{Fs: fs, calls: map[string][][]string{}}
}

func (c *countingFs) record(op string, args ...string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls[op] = append(c.calls[op], args)
}

func (c *countingFs) count(op string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.calls[op])
}

func (c *countingFs) paths(op string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.calls[op]))
	for _, args := range c.calls[op] {
		out = append(out, args[0])
	}
	return out
}

func (c *countingFs) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	clear(c.calls)
}

func (c *countingFs) RemoveAll(path string) error {
	c.record(opRemoveAll, path)
	return c.Fs.RemoveAll(path)
}

type failRule struct {
	writeBudget int
	writeErr    error
	syncErr     error
	shortWrite  bool
}

type failingFs struct {
	afero.Fs
	mu    sync.Mutex
	rules map[string]failRule
}

func newFailingFs(fs afero.Fs) *failingFs {
	return &failingFs{Fs: fs, rules: map[string]failRule{}}
}

func (f *failingFs) failWritesAfter(name string, budget int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := f.rules[name]
	r.writeBudget, r.writeErr = budget, err
	f.rules[name] = r
}

func (f *failingFs) failSyncAt(name string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := f.rules[name]
	r.syncErr = err
	f.rules[name] = r
}

func (f *failingFs) failShortWrite(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := f.rules[name]
	r.shortWrite = true
	f.rules[name] = r
}

func (f *failingFs) clearFailures() {
	f.mu.Lock()
	defer f.mu.Unlock()
	clear(f.rules)
}

func (f *failingFs) ruleFor(name string) failRule {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rules[name]
}

func (f *failingFs) Create(name string) (afero.File, error) {
	fh, err := f.Fs.Create(name)
	if err != nil {
		return nil, err
	}
	return f.wrap(name, fh), nil
}

func (f *failingFs) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	fh, err := f.Fs.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return f.wrap(name, fh), nil
}

func (f *failingFs) wrap(name string, fh afero.File) afero.File {
	r := f.ruleFor(name)
	if r.writeErr == nil && r.syncErr == nil && !r.shortWrite {
		return fh
	}
	return &failingFile{File: fh, budget: r.writeBudget, writeErr: r.writeErr, syncErr: r.syncErr, shortWrite: r.shortWrite}
}

type failingFile struct {
	afero.File
	budget     int
	writeErr   error
	syncErr    error
	shortWrite bool
}

func (f *failingFile) Write(p []byte) (int, error) {
	return f.write(p)
}

func (f *failingFile) WriteString(s string) (int, error) {
	return f.write([]byte(s))
}

func (f *failingFile) write(p []byte) (int, error) {
	if f.shortWrite && len(p) > 0 {
		return f.File.Write(p[:len(p)-1])
	}
	if f.writeErr == nil {
		return f.File.Write(p)
	}
	if f.budget <= 0 {
		return 0, f.writeErr
	}
	written, err := f.File.Write(p[:min(f.budget, len(p))])
	f.budget -= written
	if err != nil {
		return written, err
	}
	if written < len(p) {
		return written, f.writeErr
	}
	return written, nil
}

func (f *failingFile) Sync() error {
	if f.syncErr != nil {
		return f.syncErr
	}
	return f.File.Sync()
}

type renameFailingFs struct {
	afero.Fs
	err             error
	dropDestination bool
}

func newRenameFailingFs(fs afero.Fs, err error) *renameFailingFs {
	return &renameFailingFs{Fs: fs, err: err}
}

func (r *renameFailingFs) Rename(oldname, newname string) error {
	if r.dropDestination {
		if err := r.Fs.Remove(newname); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if r.err != nil {
		return r.err
	}
	return r.Fs.Rename(oldname, newname)
}
