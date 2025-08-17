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
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/log/v3"
)

func MustExist(path ...string) {
	// user rwx, group rwx, other rx
	// x is required to navigate through directories. umask 0o022 is the default and will mask final
	// permissions to 0o755 for newly created files (and directories).
	const perm = 0o775
	for _, p := range path {
		exist, err := Exist(p)
		if err != nil {
			panic(err)
		}
		if exist {
			continue
		}
		if err := os.MkdirAll(p, perm); err != nil {
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
		if err != nil {
			return err
		}
		for _, fPath := range files {
			fPath := fPath
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

func RemoveFile(path string) error {
	if dbg.TraceDeletion {
		log.Debug("[removing] removing file", "path", path, "stack", dbg.Stack())
	}
	return os.Remove(path)
}

func RemoveAll(path string) error {
	if dbg.TraceDeletion {
		log.Debug("[removing] removing dir", "path", path, "stack", dbg.Stack())
	}
	return os.RemoveAll(path)
}
