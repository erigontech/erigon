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
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
)

var PBTProgressInterval = 30 * time.Second

type PBTProgressConfig struct {
	Logger   log.Logger
	Message  string
	Phase    string
	Fields   []any
	Total    uint64
	TempDir  string
	Interval time.Duration
}

type PBTProgress struct {
	logger   log.Logger
	message  string
	phase    string
	fields   []any
	total    uint64
	tempDir  string
	interval time.Duration
	started  time.Time
	next     time.Time
	done     atomic.Uint64
	mu       sync.Mutex
}

type pbtProgressBatch struct {
	progress  *PBTProgress
	threshold uint64
	done      uint64
	reported  uint64
}

type pbtRangeProgress struct {
	tempDir string
	prefix  string
	hook    func(string) error
}

func newPbtRangeProgress(tempDir, prefix string) *pbtRangeProgress {
	return &pbtRangeProgress{tempDir: tempDir, prefix: prefix}
}

func (p *pbtRangeProgress) stage(suffix string, total uint64) *PBTProgress {
	return NewPBTProgress(PBTProgressConfig{
		Message: "PBT range writer progress",
		Phase:   p.prefix + " " + suffix,
		Total:   total,
		TempDir: p.tempDir,
	})
}

func (p *pbtRangeProgress) batch(suffix string, total uint64) *pbtProgressBatch {
	return &pbtProgressBatch{progress: p.stage(suffix, total), threshold: pbtProgressThreshold(total)}
}

func pbtProgressThreshold(total uint64) uint64 {
	if total > 1 && total < 8192 {
		return max(uint64(1), total/2)
	}
	return 4096
}

func (p *pbtProgressBatch) Add(n uint64) {
	if p == nil {
		return
	}
	p.done += n
	if p.done-p.reported >= p.threshold {
		p.progress.Add(p.done - p.reported)
		p.reported = p.done
	}
}

func (p *pbtProgressBatch) Reset() {
	if p == nil {
		return
	}
	p.done = 0
	p.reported = 0
	p.progress.done.Store(0)
	p.progress.mu.Lock()
	p.progress.next = time.Now().Add(p.progress.interval)
	p.progress.mu.Unlock()
}

func (p *pbtProgressBatch) ProgressBatchSize() uint64 {
	return p.threshold
}

func (p *pbtProgressBatch) Done() {
	if p == nil {
		return
	}
	if p.done != p.reported {
		p.progress.Add(p.done - p.reported)
	}
	p.progress.Done()
}

func NewPBTProgress(config PBTProgressConfig) *PBTProgress {
	if config.Logger == nil {
		config.Logger = log.Root()
	}
	if config.Message == "" {
		config.Message = "PBT progress"
	}
	if config.Interval <= 0 {
		config.Interval = PBTProgressInterval
	}
	if config.Interval <= 0 {
		config.Interval = 30 * time.Second
	}
	now := time.Now()
	progress := &PBTProgress{
		logger:   config.Logger,
		message:  config.Message,
		phase:    config.Phase,
		fields:   config.Fields,
		total:    config.Total,
		tempDir:  config.TempDir,
		interval: config.Interval,
		started:  now,
		next:     now.Add(config.Interval),
	}
	progress.log(0)
	return progress
}

func (p *PBTProgress) Add(n uint64) {
	done := p.done.Add(n)
	now := time.Now()
	p.mu.Lock()
	if now.Before(p.next) {
		p.mu.Unlock()
		return
	}
	p.next = now.Add(p.interval)
	p.mu.Unlock()
	p.log(done)
}

func (p *PBTProgress) Done() {
	p.log(p.done.Load())
}

func (p *PBTProgress) SetFields(fields ...any) {
	p.mu.Lock()
	p.fields = append([]any(nil), fields...)
	p.mu.Unlock()
}

func (p *PBTProgress) log(done uint64) {
	elapsed := time.Since(p.started).Seconds()
	if elapsed <= 0 {
		elapsed = 1e-9
	}
	ctx := []any{
		"phase", p.phase,
		"done", done,
		"rate", float64(done) / elapsed,
		"rss_anon", pbtRSSAnon(),
		"go_heap", pbtHeapBytes(),
	}
	if p.total > 0 {
		eta := float64(0)
		if done < p.total && done > 0 {
			eta = elapsed * float64(p.total-done) / float64(done)
		}
		ctx = append(ctx, "total", p.total, "eta", eta)
	}
	if p.tempDir != "" {
		ctx = append(ctx, "temp_bytes", pbtDirSize(p.tempDir))
	}
	p.mu.Lock()
	fields := append([]any(nil), p.fields...)
	p.mu.Unlock()
	ctx = append(ctx, fields...)
	p.logger.Info(p.message, ctx...)
}

func pbtHeapBytes() uint64 {
	alloc, _ := dbg.MemUsage()
	return alloc
}

func pbtDirSize(root string) uint64 {
	var total uint64
	_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if entry.IsDir() {
			return nil
		}
		info, statErr := entry.Info()
		if statErr == nil {
			total += uint64(info.Size())
		}
		return nil
	})
	return total
}
