// Copyright 2021 The Erigon Authors
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

package dir

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
)

var (
	removedFilesChan chan string
	removedFiles     []string
)

func init() {
	if dbg.AssertEnabled {
		removedFilesChan = make(chan string, 100)
		go trackRemovedFiles()
	}
}

func trackRemovedFiles() {
	// Use a single ticker to avoid leaking timers created by time.Tick
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case path := <-removedFilesChan:
			if len(removedFiles) > 10_000 {
				removedFiles = make([]string, 0)
			}
			removedFiles = append(removedFiles, path)
		case <-ticker.C:
			for _, path := range removedFiles {
				if exists, _ := FileExist(path); exists {
					log.Warn("Removed file unexpectedly exists", "path", path)
				}
			}
		}
	}
}

// user rwx, group rwx, other rx
// x is required to navigate through directories. umask 0o022 is the default and will mask final
// permissions to 0o755 for newly created files (and directories).
const DirPerm = 0o775

func MustExist(path ...string) {
	for _, p := range path {
		exist, err := Exist(p)
		if err != nil {
			panic(err)
		}
		if exist {
			continue
		}
		if err := os.MkdirAll(p, DirPerm); err != nil {
			panic(err)
		}
	}
}

func Exist(path string) (exists bool, err error) {
	_, err = os.Stat(path)
	switch {
	case err == nil:
		return true, nil
	case os.IsNotExist(err):
		return false, nil
	default:
		return false, err
	}
}

func FileExist(path string) (exists bool, err error) {
	fi, err := os.Stat(path)
	switch {
	case os.IsNotExist(err):
		return false, nil
	case err != nil:
		return false, err
	default:
	}
	if fi == nil {
		return false, nil
	}
	if !fi.Mode().IsRegular() {
		return false, nil
	}
	return true, nil
}

func FileNonZero(path string) bool {
	fi, err := os.Stat(path)
	if err != nil && os.IsNotExist(err) {
		return false
	}
	if fi == nil {
		return false
	}
	if !fi.Mode().IsRegular() {
		return false
	}
	return fi.Size() > 0
}

// Writes an entire file from data. Extra flags can be provided.
func writeFileWithFsyncAndFlags(name string, data []byte, perm os.FileMode, flags int) error {
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|flags, perm)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(data)
	if err != nil {
		return err
	}
	err = f.Sync()
	if err != nil {
		return err
	}
	return err
}

// nolint
func WriteFileWithFsync(name string, data []byte, perm os.FileMode) error {
	return writeFileWithFsyncAndFlags(name, data, perm, 0)
}

func WriteExclusiveFileWithFsync(name string, data []byte, perm os.FileMode) error {
	return writeFileWithFsyncAndFlags(name, data, perm, os.O_EXCL)
}

// nolint
func DeleteFiles(dirs ...string) error {
	g := errgroup.Group{}
	for _, dir := range dirs {
		files, err := ListFiles(dir)
		if errors.Is(err, os.ErrNotExist) {
			log.Debug("directory does not exist, skipping deletion", "dir", dir)
			continue
		}
		if err != nil {
			return err
		}
		for _, fPath := range files {
			g.Go(func() error { return RemoveFile(fPath) })
		}
	}
	return g.Wait()
}

func ListFiles(dir string, extensions ...string) (paths []string, err error) {
	files, err := ReadDir(dir)
	if err != nil {
		return nil, err
	}

	paths = make([]string, 0, len(files))
	for _, f := range files {
		if f.IsDir() && !f.Type().IsRegular() {
			continue
		}
		if strings.HasPrefix(f.Name(), ".") {
			continue
		}
		match := false
		if len(extensions) == 0 {
			match = true
		}
		for _, ext := range extensions {
			if filepath.Ext(f.Name()) == ext { // filter out only compressed files
				match = true
			}
		}
		if !match {
			continue
		}
		paths = append(paths, filepath.Join(dir, f.Name()))
	}
	return paths, nil
}

func RemoveFilesByMask(path string) error {
	matches, err := filepath.Glob(path)
	if err != nil {
		return fmt.Errorf("invalid pattern: %w", err)
	}
	for _, match := range matches {
		if err := RemoveFile(match); err != nil {
			return err
		}
	}
	return nil
}

func RemoveFile(path string) error {
	if dbg.TraceDeletion {
		log.Debug("[removing] removing file", "path", path, "stack", dbg.Stack())
	}

	if err := os.Remove(path); err != nil { //nolint
		return err
	}
	if dbg.AssertEnabled {
		select {
		case removedFilesChan <- path:
		default:
		}
	}
	return nil
}

func RemoveAll(path string) error {
	if dbg.TraceDeletion {
		log.Debug("[removing] removing dir", "path", path, "stack", dbg.Stack())
	}
	return os.RemoveAll(path) //nolint
}

// CreateTemp creates a temporary file using `file` as base
func CreateTemp(file string) (*os.File, error) {
	return CreateTempWithExtension(file, "tmp")
}

func CreateTempWithExtension(file string, extension string) (*os.File, error) {
	directory := filepath.Dir(file)
	filename := filepath.Base(file)
	pattern := fmt.Sprintf("%s.*.%s", filename, extension)
	if !strings.HasSuffix(pattern, ".tmp") {
		return nil, fmt.Errorf("extension must end with .tmp, erigon cleans these up at restart. pattern: %s", pattern)
	}
	return os.CreateTemp(directory, pattern)
}

// LogOnENOSPC checks whether err wraps syscall.ENOSPC and, if so, logs
// diagnostic information about disk usage. Summaries are logged first (most
// valuable), individual file listings last (can be truncated if disk is full).
// datadir is the erigon data directory (parent of temp/, snapshots/, chaindata/).
// After logging, it panics to stop the process immediately.
func LogOnENOSPC(err error, datadir string) {
	if !errors.Is(err, syscall.ENOSPC) {
		return
	}

	// Log summaries first — these are the most important and least likely
	// to be truncated by a full disk.
	logFilesystemStats(datadir)
	logDeletedOpenFiles()
	logChaindataSize(filepath.Join(datadir, "chaindata"))
	logSubdirSummary(filepath.Join(datadir, "snapshots"))
	logDirSummary(filepath.Join(datadir, "temp"))

	// Now log individual file listings (may be truncated on very full disks).
	logDirFiles(filepath.Join(datadir, "temp"))

	panic(fmt.Sprintf("[ENOSPC] fatal: no space left on device (datadir=%s)", datadir))
}

func logDirSummary(dirPath string) {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return
	}
	var totalSize uint64
	var fileCount int
	for _, e := range entries {
		info, infoErr := e.Info()
		if infoErr != nil {
			continue
		}
		fileCount++
		totalSize += uint64(info.Size())
	}
	log.Warn("[ENOSPC] dir summary", "dir", dirPath, "files", fileCount, "totalSize", byteCount(int64(totalSize)))
}

func logDirFiles(dirPath string) {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		log.Warn("[ENOSPC] failed to read dir", "dir", dirPath, "err", err)
		return
	}
	for _, e := range entries {
		info, infoErr := e.Info()
		if infoErr != nil {
			continue
		}
		log.Warn("[ENOSPC] file", "dir", dirPath, "name", e.Name(), "size", byteCount(info.Size()), "isDir", e.IsDir())
	}
}

// logSubdirSummary logs a per-subdirectory summary (file count + total size)
// for the given directory. Useful for large directories where logging every
// file would be too noisy.
func logSubdirSummary(dirPath string) {
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return
	}
	var grandTotal uint64
	for _, e := range entries {
		childPath := filepath.Join(dirPath, e.Name())
		if !e.IsDir() {
			info, infoErr := e.Info()
			if infoErr != nil {
				continue
			}
			grandTotal += uint64(info.Size())
			log.Warn("[ENOSPC] snapshot file", "name", e.Name(), "size", byteCount(info.Size()))
			continue
		}
		children, readErr := os.ReadDir(childPath)
		if readErr != nil {
			continue
		}
		var dirSize uint64
		var fileCount int
		for _, c := range children {
			info, infoErr := c.Info()
			if infoErr != nil {
				continue
			}
			fileCount++
			dirSize += uint64(info.Size())
		}
		grandTotal += dirSize
		log.Warn("[ENOSPC] snapshot subdir", "name", e.Name(), "files", fileCount, "totalSize", byteCount(int64(dirSize)))
	}
	log.Warn("[ENOSPC] snapshots summary", "dir", dirPath, "totalSize", byteCount(int64(grandTotal)))
}

func logChaindataSize(chaindataDir string) {
	entries, err := os.ReadDir(chaindataDir)
	if err != nil {
		return
	}
	var totalSize uint64
	for _, e := range entries {
		info, infoErr := e.Info()
		if infoErr != nil {
			continue
		}
		totalSize += uint64(info.Size())
	}
	log.Warn("[ENOSPC] chaindata", "dir", chaindataDir, "totalSize", byteCount(int64(totalSize)))
}

// logDeletedOpenFiles reads /proc/self/fd to find deleted files still held open
// by this process. These consume disk space that du won't report but df will.
func logDeletedOpenFiles() {
	fdDir := "/proc/self/fd"
	entries, err := os.ReadDir(fdDir)
	if err != nil {
		return // not on Linux or no procfs
	}
	var deletedCount int
	var deletedSize uint64
	for _, e := range entries {
		link, err := os.Readlink(filepath.Join(fdDir, e.Name()))
		if err != nil {
			continue
		}
		if !strings.HasSuffix(link, " (deleted)") {
			continue
		}
		info, err := os.Stat(filepath.Join(fdDir, e.Name()))
		if err != nil {
			continue
		}
		size := uint64(info.Size())
		deletedCount++
		deletedSize += size
		if size > 100*1024*1024 { // only log files > 100MB
			log.Warn("[ENOSPC] deleted-but-open", "fd", e.Name(), "path", link, "size", byteCount(int64(size)))
		}
	}
	log.Warn("[ENOSPC] deleted-but-open summary", "files", deletedCount, "totalSize", byteCount(int64(deletedSize)))
}

func byteCount(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%dB", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(b)/float64(div), "KMGTPE"[exp])
}
