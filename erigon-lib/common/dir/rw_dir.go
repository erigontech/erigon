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
	"github.com/erigontech/erigon-lib/common/customfs"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sync/errgroup"
)

func MustExist(path ...string) {
	const perm = 0764 // user rwx, group rw, other r
	for _, p := range path {
		exist, err := Exist(p)
		if err != nil {
			panic(err)
		}
		if exist {
			continue
		}
		if err := customfs.CFS.MkdirAll(p, perm); err != nil {
			panic(err)
		}
	}
}

func Exist(path string) (exists bool, err error) {
	_, err = customfs.CFS.Stat(path)
	switch {
	case err == nil:
		return true, nil
	case customfs.CFS.IsNotExist(err):
		return false, nil
	default:
		return false, err
	}
}

func FileExist(path string) (exists bool, err error) {
	fi, err := customfs.CFS.Stat(path)
	switch {
	case customfs.CFS.IsNotExist(err):
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
	fi, err := customfs.CFS.Stat(path)
	if err != nil && customfs.CFS.IsNotExist(err) {
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

// nolint
func WriteFileWithFsync(name string, data []byte, perm os.FileMode) error {
	f, err := customfs.CFS.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
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

func Recreate(dir string) {
	exist, err := Exist(dir)
	if err != nil {
		panic(err)
	}
	if exist {
		_ = customfs.CFS.RemoveAll(dir)
	}
	MustExist(dir)
}

func HasFileOfType(dir, ext string) bool {
	files, err := ReadDir(dir)
	if err != nil {
		return false
	}
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		if filepath.Ext(f.Name()) == ext {
			return true
		}
	}
	return false
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
			g.Go(func() error { return customfs.CFS.Remove(fPath) })
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
