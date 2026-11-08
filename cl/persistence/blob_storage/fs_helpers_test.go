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
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

const (
	opCreate    = "Create"
	opOpen      = "Open"
	opRemoveAll = "RemoveAll"
	opRename    = "Rename"
	opStat      = "Stat"
)

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

// paths returns the first argument of every recorded op call, in call order.
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

func (c *countingFs) Create(name string) (afero.File, error) {
	c.record(opCreate, name)
	return c.Fs.Create(name)
}

func (c *countingFs) Open(name string) (afero.File, error) {
	c.record(opOpen, name)
	return c.Fs.Open(name)
}

func (c *countingFs) RemoveAll(path string) error {
	c.record(opRemoveAll, path)
	return c.Fs.RemoveAll(path)
}

func (c *countingFs) Rename(oldname, newname string) error {
	c.record(opRename, oldname, newname)
	return c.Fs.Rename(oldname, newname)
}

func (c *countingFs) Stat(name string) (os.FileInfo, error) {
	c.record(opStat, name)
	return c.Fs.Stat(name)
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

// failWritesAfter lets name accept budget bytes and fails every write past that.
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

// failShortWrite makes every write to name return one byte fewer than requested with a
// nil error, as a filesystem or pipe can do without failing outright.
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

// wrap snapshots the rule, so each open of a path starts from a fresh write budget.
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

type slowFs struct {
	afero.Fs
	removeAllDelay time.Duration
}

func newSlowFs(fs afero.Fs, removeAllDelay time.Duration) *slowFs {
	return &slowFs{Fs: fs, removeAllDelay: removeAllDelay}
}

func (s *slowFs) RemoveAll(path string) error {
	time.Sleep(s.removeAllDelay)
	return s.Fs.RemoveAll(path)
}

func TestCountingFsTalliesRecordedCalls(t *testing.T) {
	fs := newCountingFs(afero.NewMemMapFs())

	fh, err := fs.Create("7/a")
	require.NoError(t, err)
	require.NoError(t, fh.Close())
	_, err = fs.Stat("7/a")
	require.NoError(t, err)
	require.NoError(t, fs.Rename("7/a", "7/b"))
	rh, err := fs.Open("7/b")
	require.NoError(t, err)
	require.NoError(t, rh.Close())
	require.NoError(t, fs.RemoveAll("7"))

	require.Equal(t, 1, fs.count(opCreate))
	require.Equal(t, 1, fs.count(opStat))
	require.Equal(t, 1, fs.count(opRename))
	require.Equal(t, 1, fs.count(opOpen))
	require.Equal(t, 1, fs.count(opRemoveAll))

	require.Equal(t, []string{"7/a"}, fs.paths(opCreate))
	require.Equal(t, []string{"7/a"}, fs.paths(opRename))
	require.Equal(t, []string{"7/b"}, fs.paths(opOpen))
	require.Equal(t, []string{"7"}, fs.paths(opRemoveAll))
}

func TestCountingFsTalliesRepeatedCalls(t *testing.T) {
	fs := newCountingFs(afero.NewMemMapFs())

	require.NoError(t, fs.RemoveAll("1"))
	require.NoError(t, fs.RemoveAll("2"))
	require.NoError(t, fs.RemoveAll("1"))

	require.Equal(t, 3, fs.count(opRemoveAll))
	require.Equal(t, []string{"1", "2", "1"}, fs.paths(opRemoveAll))
}

func TestCountingFsDelegatesUnrecordedCalls(t *testing.T) {
	fs := newCountingFs(afero.NewMemMapFs())

	require.NoError(t, fs.MkdirAll("7/inner", 0o755))
	require.NoError(t, afero.WriteFile(fs, "7/inner/a", []byte("payload"), 0o644))
	require.NoError(t, fs.Remove("7/inner/a"))

	require.Zero(t, fs.count(opRemoveAll))
	require.Zero(t, fs.count(opRename))

	dirExists, err := afero.DirExists(fs, "7/inner")
	require.NoError(t, err)
	require.True(t, dirExists)

	fileExists, err := afero.Exists(fs, "7/inner/a")
	require.NoError(t, err)
	require.False(t, fileExists)
}

func TestCountingFsReset(t *testing.T) {
	fs := newCountingFs(afero.NewMemMapFs())

	require.NoError(t, fs.RemoveAll("7"))
	require.Equal(t, 1, fs.count(opRemoveAll))

	fs.reset()

	require.Zero(t, fs.count(opRemoveAll))
	require.Empty(t, fs.paths(opRemoveAll))

	require.NoError(t, fs.RemoveAll("8"))
	require.Equal(t, 1, fs.count(opRemoveAll))
	require.Equal(t, []string{"8"}, fs.paths(opRemoveAll))
}

func TestFailingFsTruncatesWriteAtChosenPath(t *testing.T) {
	fs := newFailingFs(afero.NewMemMapFs())
	fs.failWritesAfter("7/a", 4, errInducedFailure)

	payload := []byte("0123456789")
	fh, err := fs.Create("7/a")
	require.NoError(t, err)
	n, err := fh.Write(payload)
	require.ErrorIs(t, err, errInducedFailure)
	require.Equal(t, 4, n)
	require.NoError(t, fh.Close())

	content, err := afero.ReadFile(fs, "7/a")
	require.NoError(t, err)
	require.Equal(t, []byte("0123"), content)
}

func TestFailingFsRejectsEveryWriteWithZeroBudget(t *testing.T) {
	fs := newFailingFs(afero.NewMemMapFs())
	fs.failWritesAfter("7/a", 0, errInducedFailure)

	fh, err := fs.Create("7/a")
	require.NoError(t, err)
	n, err := fh.WriteString("0123")
	require.ErrorIs(t, err, errInducedFailure)
	require.Zero(t, n)
	require.NoError(t, fh.Close())

	content, err := afero.ReadFile(fs, "7/a")
	require.NoError(t, err)
	require.Empty(t, content)
}

func TestFailingFsBudgetIsPerOpenFile(t *testing.T) {
	fs := newFailingFs(afero.NewMemMapFs())
	fs.failWritesAfter("7/a", 2, errInducedFailure)

	for range 2 {
		fh, err := fs.Create("7/a")
		require.NoError(t, err)
		n, err := fh.WriteString("0123")
		require.ErrorIs(t, err, errInducedFailure)
		require.Equal(t, 2, n)
		require.NoError(t, fh.Close())
	}
}

func TestFailingFsFailsSyncAtChosenPath(t *testing.T) {
	fs := newFailingFs(afero.NewMemMapFs())
	fs.failSyncAt("7/a", errInducedFailure)

	fh, err := fs.Create("7/a")
	require.NoError(t, err)
	_, err = fh.WriteString("0123")
	require.NoError(t, err)
	require.ErrorIs(t, fh.Sync(), errInducedFailure)
	require.NoError(t, fh.Close())

	content, err := afero.ReadFile(fs, "7/a")
	require.NoError(t, err)
	require.Equal(t, []byte("0123"), content)
}

func TestFailingFsLeavesOtherPathsAlone(t *testing.T) {
	fs := newFailingFs(afero.NewMemMapFs())
	fs.failWritesAfter("7/a", 0, errInducedFailure)
	fs.failSyncAt("7/a", errInducedFailure)

	fh, err := fs.Create("7/b")
	require.NoError(t, err)
	n, err := fh.WriteString("0123")
	require.NoError(t, err)
	require.Equal(t, 4, n)
	require.NoError(t, fh.Sync())
	require.NoError(t, fh.Close())

	content, err := afero.ReadFile(fs, "7/b")
	require.NoError(t, err)
	require.Equal(t, []byte("0123"), content)
}

func TestFailingFsClearFailures(t *testing.T) {
	fs := newFailingFs(afero.NewMemMapFs())
	fs.failWritesAfter("7/a", 0, errInducedFailure)
	fs.failSyncAt("7/a", errInducedFailure)

	fs.clearFailures()

	fh, err := fs.Create("7/a")
	require.NoError(t, err)
	_, err = fh.WriteString("0123")
	require.NoError(t, err)
	require.NoError(t, fh.Sync())
	require.NoError(t, fh.Close())

	content, err := afero.ReadFile(fs, "7/a")
	require.NoError(t, err)
	require.Equal(t, []byte("0123"), content)
}

func TestFailingFsAppliesToOpenFile(t *testing.T) {
	fs := newFailingFs(afero.NewMemMapFs())
	require.NoError(t, afero.WriteFile(fs, "7/a", nil, 0o644))
	fs.failWritesAfter("7/a", 0, errInducedFailure)

	fh, err := fs.OpenFile("7/a", os.O_WRONLY, 0o644)
	require.NoError(t, err)
	_, err = fh.WriteString("0123")
	require.ErrorIs(t, err, errInducedFailure)
	require.NoError(t, fh.Close())
}

func TestSlowFsDelaysRemoveAll(t *testing.T) {
	const delay = 150 * time.Millisecond

	fs := newSlowFs(afero.NewMemMapFs(), delay)
	require.NoError(t, fs.MkdirAll("7", 0o755))

	start := time.Now()
	require.NoError(t, fs.RemoveAll("7"))
	require.GreaterOrEqual(t, time.Since(start), delay)

	exists, err := afero.DirExists(fs, "7")
	require.NoError(t, err)
	require.False(t, exists)
}

func TestSlowFsDelaysNothingElse(t *testing.T) {
	const delay = 150 * time.Millisecond

	fs := newSlowFs(afero.NewMemMapFs(), delay)

	start := time.Now()
	require.NoError(t, fs.MkdirAll("7", 0o755))
	require.NoError(t, afero.WriteFile(fs, "7/a", []byte("payload"), 0o644))
	_, err := fs.Stat("7/a")
	require.NoError(t, err)
	require.Less(t, time.Since(start), delay/2)
}

// renameFailingFs fails every Rename with err. When dropDestination is set it also removes
// the destination first, standing in for a prune that took the bucket mid-write.
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
