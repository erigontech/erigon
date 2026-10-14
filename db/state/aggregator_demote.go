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

	"github.com/erigontech/erigon/common/dir"
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
	coldPath := filepath.Join(coldDir, filepath.Base(ssdPath))
	if err = copyToCold(ctx, ssdPath, coldPath); err != nil {
		return fmt.Errorf("DemoteFile: copy %s: %w", ssdPath, err)
	}
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
func copyToCold(ctx context.Context, src, dst string) error {
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
	if _, err := io.Copy(out, in); err != nil {
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
