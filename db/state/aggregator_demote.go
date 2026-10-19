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
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

// DemoteFile copies the domain's [from, to) .kv file to coldDir, replaces it with an absolute symlink to the
// copy and reopens it in place. Readers of older visible generations keep the old inode until they close.
func (a *Aggregator) DemoteFile(ctx context.Context, domain kv.Domain, from, to kv.Step, coldDir string) error {
	d := a.d[domain]
	old, ssdPath, wordCount, err := a.dirtyDomainFile(d, from, to)
	if err != nil {
		return err
	}
	coldDir, err = filepath.Abs(coldDir)
	if err != nil {
		return err
	}
	name, size, start := filepath.Base(ssdPath), old.decompressor.Size(), time.Now()
	mxDemotionInFlight.Set(1)
	mxDemotionFile.WithLabelValues(name).Set(1)
	defer func() {
		mxDemotionInFlight.Set(0)
		mxDemotionFile.Reset()
	}()
	a.logger.Info("[tiering] demotion started", "file", name, "size", common.ByteCount(uint64(size)))

	coldPath := filepath.Join(coldDir, name)
	if a.onDemoteCopy != nil {
		a.onDemoteCopy(ctx)
	}
	if err = copyToCold(ctx, ssdPath, coldPath, a.coldIO); err != nil {
		return fmt.Errorf("DemoteFile: copy %s: %w", ssdPath, err)
	}
	copyBytesPerSec := float64(size) / time.Since(start).Seconds()
	if err = replaceWithSymlink(ssdPath, coldPath); err != nil {
		return fmt.Errorf("DemoteFile: symlink %s: %w", ssdPath, err)
	}

	dirEntries, err := filesFromDir(a.dirs.SnapDomain)
	if err != nil {
		return err
	}
	nu := newFilesItem(old.startTxNum, old.endTxNum)
	if !d.openDirtyItem(nu, dirEntries) {
		nu.closeFiles()
		return fmt.Errorf("DemoteFile: reopen %s failed", ssdPath)
	}
	if got := nu.decompressor.Count(); got != wordCount {
		nu.closeFiles()
		return fmt.Errorf("DemoteFile: %s has %d words, want %d", coldPath, got, wordCount)
	}

	a.dirtyFilesLock.Lock()
	defer a.dirtyFilesLock.Unlock()
	retired, err := d.replaceDirtyFile(old, nu)
	if err != nil {
		nu.closeFiles()
		return err
	}
	a.recalcVisibleFiles(retired)

	seconds := time.Since(start).Seconds()
	mxDemotions.WithLabelValues(domain.String()).Inc()
	mxDemotedBytes.WithLabelValues(domain.String()).Add(float64(size))
	mxDemotionSeconds.Set(seconds)
	mxDemotionCopyBytesPerSec.Set(copyBytesPerSec)
	a.logger.Info("[tiering] demotion done", "file", name, "size", common.ByteCount(uint64(size)), "seconds", fmt.Sprintf("%.1f", seconds), "MB/s", fmt.Sprintf("%.0f", copyBytesPerSec/1e6))
	return nil
}

func (a *Aggregator) dirtyDomainFile(d *Domain, from, to kv.Step) (item *FilesItem, path string, wordCount int, err error) {
	a.dirtyFilesLock.Lock()
	defer a.dirtyFilesLock.Unlock()
	item, ok := d.dirtyFiles.Get(newFilesItem(uint64(from)*d.stepSize, uint64(to)*d.stepSize))
	if !ok || item.decompressor == nil {
		return nil, "", 0, fmt.Errorf("DemoteFile: %s %d-%d is not an open file", d.FilenameBase, from, to)
	}
	return item, item.decompressor.FilePath(), item.decompressor.Count(), nil
}

// copyToCold skips the copy when dst already has the size of src (a demotion interrupted after the copy).
func copyToCold(ctx context.Context, src, dst string, gate *coldIOGate) error {
	srcInfo, err := os.Stat(src)
	if err != nil {
		return err
	}
	if dstInfo, statErr := os.Stat(dst); statErr == nil && dstInfo.Size() == srcInfo.Size() {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp := dst + ".tmp"
	out, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if err := copyWithCancel(ctx, out, in, gate); err != nil {
		_ = out.Close()
		_ = dir.RemoveFile(tmp)
		return err
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, dst); err != nil {
		return err
	}
	return dir.FsyncDir(filepath.Dir(dst))
}

// copyWithCancel copies in chunks so cancel and pause act between them; io.CopyN keeps the copy_file_range fast
// path. Each chunk's writeback is started at once, so a pause also stops the disk writes, not only the copy.
func copyWithCancel(ctx context.Context, dst *os.File, src *os.File, gate *coldIOGate) error {
	const chunk = 1 << 20
	var off int64
	for {
		waitStart := time.Now()
		if err := gate.Wait(ctx); err != nil {
			return err
		}
		mxDemotionPausedSeconds.Add(time.Since(waitStart).Seconds())
		n, err := io.CopyN(dst, src, chunk)
		if n > 0 {
			if werr := writeBack(dst, off, n, chunk); werr != nil {
				return werr
			}
			off += n
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func replaceWithSymlink(path, target string) error {
	tmp := path + ".demote.tmp"
	if err := dir.RemoveFile(tmp); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if err := os.Symlink(target, tmp); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return dir.FsyncDir(filepath.Dir(path))
}

// DemoteInBackground starts demoting the oldest commitment .kv on SSD whose whole range ends at or before
// boundaryTxNum. It returns false when a demotion is already in flight or there is nothing to demote.
func (a *Aggregator) DemoteInBackground(boundaryTxNum uint64, coldDir string) bool {
	if dbg.NoBackgroundMaintenance() {
		return false
	}
	if !a.demoting.CompareAndSwap(false, true) {
		return false
	}
	from, to, ok := a.demotionCandidate(boundaryTxNum)
	if !ok {
		a.demoting.Store(false)
		return false
	}
	started := a.background.TryGo(func() {
		defer a.demoting.Store(false)
		if err := a.DemoteFile(a.ctx, kv.CommitmentDomain, from, to, coldDir); err != nil && !errors.Is(err, context.Canceled) {
			a.logger.Warn("[tiering] demotion failed", "from", from, "to", to, "err", err)
		}
	})
	if !started {
		a.demoting.Store(false)
	}
	return started
}

func (a *Aggregator) demotionCandidate(boundaryTxNum uint64) (from, to kv.Step, ok bool) {
	at := a.BeginFilesRo()
	defer at.Close()
	stepSize := a.StepSize()
	for _, f := range at.d[kv.CommitmentDomain].files {
		if f.endTxNum > boundaryTxNum {
			break
		}
		fi, err := os.Lstat(f.Fullpath())
		if err != nil || fi.Mode()&os.ModeSymlink != 0 {
			continue
		}
		return kv.Step(f.startTxNum / stepSize), kv.Step(f.endTxNum / stepSize), true
	}
	return 0, 0, false
}

// recoverDemotion removes the leftovers of an interrupted demotion. The cold dir can be shared by several datadirs,
// so only partial copies of this datadir's commitment files are removed there.
func recoverDemotion(snapDomain, coldDir string, logger log.Logger) error {
	entries, err := os.ReadDir(snapDomain)
	if err != nil {
		return err
	}
	commitment := "-" + kv.CommitmentDomain.String() + "."
	for _, e := range entries {
		name := e.Name()
		switch {
		case strings.HasSuffix(name, ".demote.tmp"):
			if err := dir.RemoveFile(filepath.Join(snapDomain, name)); err != nil {
				return err
			}
		case strings.Contains(name, commitment) && strings.HasSuffix(name, ".kv"):
			if err := dir.RemoveFile(filepath.Join(coldDir, name+".tmp")); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return err
			}
			if _, err := os.Stat(filepath.Join(coldDir, name)); err == nil && e.Type().IsRegular() {
				logger.Info("[tiering] demotion swap pending; the next trigger finishes it", "file", name)
			}
		}
	}
	return nil
}
