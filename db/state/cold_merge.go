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
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
)

const coldMergeLinkSuffix = ".coldmerge.tmp"

// createColdMergeLink records an in-flight cold merge output before it is written, so recovery can find and
// remove it in the shared cold dir.
func createColdMergeLink(path, target string) (string, error) {
	link := path + coldMergeLinkSuffix
	removeColdMergeLink(link)
	if err := os.Symlink(target, link); err != nil {
		return "", err
	}
	return link, dir.FsyncDir(filepath.Dir(link))
}

func commitColdMergeLink(link, path string) error {
	if err := os.Rename(link, path); err != nil {
		return err
	}
	return dir.FsyncDir(filepath.Dir(path))
}

// removeColdMergeLink removes the link, its target and the compressor's temp files next to the target.
func removeColdMergeLink(link string) {
	if target, err := os.Readlink(link); err == nil {
		tmps, _ := filepath.Glob(target + ".*.tmp")
		for _, p := range append(tmps, target) {
			_ = dir.RemoveFile(p)
		}
	}
	_ = dir.RemoveFile(link)
}

func (d *Domain) waitColdMerge(ctx context.Context) error {
	start := time.Now()
	mxColdMergePaused.Set(1)
	err := d.coldIO.Wait(ctx)
	mxColdMergePaused.Set(0)
	mxColdMergePausedSeconds.Add(time.Since(start).Seconds())
	return err
}

func (d *Domain) logColdMerge(r MergeRange, inputs []*FilesItem, out *FilesItem, start time.Time) {
	var inBytes int64
	for _, f := range inputs {
		inBytes += f.decompressor.Size()
	}
	outBytes, seconds := out.decompressor.Size(), time.Since(start).Seconds()
	steps := (r.to - r.from) / d.stepSize
	mxColdMergeSeconds.Observe(seconds)
	mxColdMergeSteps.Set(float64(steps))
	mxColdMergeInBytes.Add(float64(inBytes))
	mxColdMergeOutBytes.Add(float64(outBytes))
	d.logger.Info("[tiering] cold merge done", "file", out.decompressor.FileName(), "steps", steps,
		"in", common.ByteCount(uint64(inBytes)), "out", common.ByteCount(uint64(outBytes)), "seconds", fmt.Sprintf("%.1f", seconds))
}
