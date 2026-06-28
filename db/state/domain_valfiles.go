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

package state

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/valfile"
)

// dbgValFileStats gates the per-key .cvl distribution stats (fnv1a + per-key map);
// off by default so perf runs don't pay for debug instrumentation in Flush.
var dbgValFileStats = dbg.EnvBool("COMMITMENT_VALFILE_STATS", false)

// domainValFiles is the write-side owner of a domain's per-step external
// value-files (.cvl): append writers plus reader FilesItems surfaced into
// domainVisible.valFiles for reader-safe, generation-pinned access.
type domainValFiles struct {
	name              kv.Domain
	dir               string
	base              string
	verStr            string
	threshold         int
	minKeyLen         int  // keep values with key length <= this inline
	noFsync           bool // mirrors MDBX no-fsync: flush buffers but skip fdatasync
	stepSize          uint64
	stepsInFrozenFile uint64
	logger            log.Logger

	// mu guards the maps; valfile.Reader.Get (pread) runs outside it.
	mu      sync.Mutex
	writers map[kv.Step]*valfile.Writer
	items   map[kv.Step]*FilesItem // per-step .cvl reader FilesItems

	statsMu sync.Mutex
	stats   map[kv.Step]map[string]*cvlKeyStat // .cvl repetition debug (commitment only)
}

// cvlKeyStat tracks, for one key in one step, how many times it was appended to
// the value-file and how many of those were genuine value changes vs identical.
type cvlKeyStat struct {
	appends     uint64
	transitions uint64 // value differed from the immediately previous append (incl. first)
	lastHash    uint64
	bytes       uint64 // total value bytes appended for this key (dead + live)
	lastLen     uint32 // size of the latest value (the live, MDBX-referenced one)
}

func newDomainValFiles(name kv.Domain, dir, base, verStr string, threshold, minKeyLen int, noFsync bool, stepSize, stepsInFrozenFile uint64, logger log.Logger) *domainValFiles {
	return &domainValFiles{
		name:              name,
		dir:               dir,
		base:              base,
		verStr:            verStr,
		threshold:         threshold,
		minKeyLen:         minKeyLen,
		noFsync:           noFsync,
		stepSize:          stepSize,
		stepsInFrozenFile: stepsInFrozenFile,
		logger:            logger,
		writers:           map[kv.Step]*valfile.Writer{},
		items:             map[kv.Step]*FilesItem{},
		stats:             map[kv.Step]map[string]*cvlKeyStat{},
	}
}

// fnv1a is a fast non-crypto hash used only to compare appended value bytes for
// the .cvl repetition stats.
func fnv1a(b []byte) uint64 {
	h := uint64(14695981039346656037)
	for _, c := range b {
		h ^= uint64(c)
		h *= 1099511628211
	}
	return h
}

func (m *domainValFiles) recordStat(step kv.Step, key, v []byte) {
	h := fnv1a(v)
	m.statsMu.Lock()
	defer m.statsMu.Unlock()
	byKey := m.stats[step]
	if byKey == nil {
		byKey = map[string]*cvlKeyStat{}
		m.stats[step] = byKey
	}
	s := byKey[string(key)]
	if s == nil {
		s = &cvlKeyStat{}
		byKey[string(key)] = s
	}
	if s.appends == 0 || s.lastHash != h {
		s.transitions++
	}
	s.appends++
	s.lastHash = h
	s.bytes += uint64(len(v))
	s.lastLen = uint32(len(v))
}

func (m *domainValFiles) reportStats(step kv.Step) {
	m.statsMu.Lock()
	byKey := m.stats[step]
	delete(m.stats, step)
	m.statsMu.Unlock()
	if byKey == nil || m.logger == nil {
		return
	}
	type lenAgg struct{ keys, appends, bytes, liveBytes uint64 }
	byLen := map[int]*lenAgg{}
	var appends, transitions, bytes, liveBytes uint64
	var cold, warm, hot, scalding uint64 // keys appended 1x / 2-9x / 10-99x / 100x+
	var topAppends uint64
	topKeyLen := 0
	for k, s := range byKey {
		appends += s.appends
		transitions += s.transitions
		bytes += s.bytes
		liveBytes += uint64(s.lastLen)
		la := byLen[len(k)]
		if la == nil {
			la = &lenAgg{}
			byLen[len(k)] = la
		}
		la.keys++
		la.appends += s.appends
		la.bytes += s.bytes
		la.liveBytes += uint64(s.lastLen)
		switch {
		case s.appends == 1:
			cold++
		case s.appends < 10:
			warm++
		case s.appends < 100:
			hot++
		default:
			scalding++
		}
		if s.appends > topAppends {
			topAppends = s.appends
			topKeyLen = len(k)
		}
	}
	keysByLen := map[int]uint64{}
	appendsByLen := map[int]uint64{}
	churnByLen := map[int]uint64{}  // avg appends per key at this depth (per-key churn)
	liveKBByLen := map[int]uint64{} // live (MDBX-referenced) bytes per depth, KB
	for l, la := range byLen {
		keysByLen[l] = la.keys
		appendsByLen[l] = la.appends
		if la.keys > 0 {
			churnByLen[l] = la.appends / la.keys
		}
		liveKBByLen[l] = la.liveBytes / 1024
	}
	var amplification uint64
	if liveBytes > 0 {
		amplification = bytes / liveBytes
	}
	m.logger.Warn("[dbg] .cvl repetition", "step", step,
		"appends", appends, "keys", len(byKey), "valueChanges", transitions, "identicalReappends", appends-transitions,
		"bytes", datasize.ByteSize(bytes).HR(), "liveBytes", datasize.ByteSize(liveBytes).HR(), "amplification", amplification,
		"churnHist(1x/2-9/10-99/100+)", fmt.Sprintf("%d/%d/%d/%d", cold, warm, hot, scalding),
		"keysByLen", fmt.Sprintf("%v", keysByLen), "appendsByKeyLen", fmt.Sprintf("%v", appendsByLen),
		"churnByLen", fmt.Sprintf("%v", churnByLen), "liveKBByLen", fmt.Sprintf("%v", liveKBByLen),
		"topKeyLen", topKeyLen, "topKeyAppends", topAppends)
}

func (m *domainValFiles) filePath(step kv.Step) string {
	return filepath.Join(m.dir, fmt.Sprintf("%s-%s.%d-%d.cvl", m.verStr, m.base, step, step+1))
}

// encode returns the MDBX payload for value v: inline when small or when the key
// is short (hot top-of-trie), otherwise appended to the step's value-file and
// replaced by a handle. Write path only.
func (m *domainValFiles) encode(step kv.Step, key, v []byte) ([]byte, error) {
	smallValue := len(v) < m.threshold
	hotKey := len(key) <= m.minKeyLen // || (len(key) >= 33 && len(key) < 34)
	if smallValue || hotKey {
		return valfile.EncodeInline(nil, v), nil
	}
	w, err := m.writer(step)
	if err != nil {
		return nil, err
	}
	h, err := w.Append(v)
	if err != nil {
		return nil, err
	}
	if dbgValFileStats && m.name == kv.CommitmentDomain {
		m.recordStat(step, key, v)
	}
	return valfile.EncodeExternal(nil, h), nil
}

func (m *domainValFiles) writer(step kv.Step) (*valfile.Writer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if w := m.writers[step]; w != nil {
		return w, nil
	}
	path := m.filePath(step)
	exists, err := dir.FileExist(path)
	if err != nil {
		return nil, err
	}
	var w *valfile.Writer
	if exists {
		w, err = valfile.OpenWriter(path) // resume after restart, appending at EOF
	} else {
		w, err = valfile.NewWriter(path)
	}
	if err != nil {
		return nil, err
	}
	if m.noFsync {
		w.DisableFsync()
	}
	m.writers[step] = w
	return w, nil
}

// sync fdatasyncs all open writers. MUST run before committing the MDBX handles
// that reference the appended bytes.
func (m *domainValFiles) sync() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, w := range m.writers {
		if err := w.Sync(); err != nil {
			return err
		}
	}
	return nil
}

// ensureFilesItems opens a read-only FilesItem for every step that has a writer
// but no item yet. Call after sync, once the file header+values are durable.
func (m *domainValFiles) ensureFilesItems() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for step := range m.writers {
		if m.items[step] != nil {
			continue
		}
		r, err := valfile.OpenReader(m.filePath(step))
		if err != nil {
			return err
		}
		fi := newFilesItem(uint64(step)*m.stepSize, uint64(step+1)*m.stepSize, m.stepSize, m.stepsInFrozenFile)
		fi.valReader = r
		m.items[step] = fi
	}
	return nil
}

// visibleValFiles returns a snapshot of the per-step items for domainVisible. Not
// bounded by the frozen toTxNum: these files exist for the un-collated steps.
func (m *domainValFiles) visibleValFiles() map[kv.Step]*FilesItem {
	m.mu.Lock()
	defer m.mu.Unlock()
	res := make(map[kv.Step]*FilesItem, len(m.items))
	for step, it := range m.items {
		if it.valReader != nil {
			res[step] = it
		}
	}
	return res
}

// decode resolves a payload using the step's value-file from the write-side map.
// For write-side callers (collate) where the file is not being deleted.
func (m *domainValFiles) decode(step kv.Step, payload, buf []byte) ([]byte, error) {
	return valfile.DecodePayload(payload, buf, func(h valfile.Handle, dst []byte) ([]byte, error) {
		return m.getHandle(step, h, dst)
	})
}

// getHandle reads a value by handle from the step's write-side item. Fallback for
// reads of a step not yet in the caller's visible bundle.
func (m *domainValFiles) getHandle(step kv.Step, h valfile.Handle, dst []byte) ([]byte, error) {
	m.mu.Lock()
	it := m.items[step]
	m.mu.Unlock()
	if it == nil || it.valReader == nil {
		return nil, fmt.Errorf("valfile: no reader for step %d", step)
	}
	return it.valReader.Get(h, dst)
}

// retire drops a step's writer and removes its item, returning the FilesItem so
// the caller routes it through recalcVisibleFiles(retired) for reader-safe delete.
func (m *domainValFiles) retire(step kv.Step) *FilesItem {
	m.mu.Lock()
	defer m.mu.Unlock()
	if dbgValFileStats && m.name == kv.CommitmentDomain && m.logger != nil {
		m.reportStats(step)
	}
	if w := m.writers[step]; w != nil {
		w.Close()
		delete(m.writers, step)
	}
	it := m.items[step]
	if it == nil {
		return nil
	}
	delete(m.items, step)
	return it
}

// openFolder, on startup, deletes orphan .cvl (steps already collated into a
// frozen .kv, per firstStepNotInFiles) and reopens the rest as readable items.
func (m *domainValFiles) openFolder(firstStepNotInFiles kv.Step, logger log.Logger) error {
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return err
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".cvl") {
			names = append(names, e.Name())
		}
	}
	l := filterDirtyFiles(names, m.stepSize, m.stepsInFrozenFile, m.base, "cvl", logger)

	m.mu.Lock()
	defer m.mu.Unlock()
	for _, fi := range l {
		step := fi.StartStep(m.stepSize)
		path := m.filePath(step)
		if step < firstStepNotInFiles {
			dir.RemoveFile(path)
			continue
		}
		if m.items[step] != nil {
			continue
		}
		r, err := valfile.OpenReader(path)
		if err != nil {
			return err
		}
		fi.valReader = r
		m.items[step] = fi
	}
	return nil
}

func (m *domainValFiles) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, w := range m.writers {
		w.Close()
	}
	for _, it := range m.items {
		if it.valReader != nil {
			it.valReader.Close()
		}
	}
	m.writers = map[kv.Step]*valfile.Writer{}
	m.items = map[kv.Step]*FilesItem{}
}
