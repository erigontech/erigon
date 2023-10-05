/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package dir

import (
	"os"
	"path/filepath"
)

func MustExist(path ...string) {
	const perm = 0764 // user rwx, group rw, other r
	for _, p := range path {
		if err := os.MkdirAll(p, perm); err != nil {
			panic(err)
		}
	}
}

func Exist(path string) bool {
	_, err := os.Stat(path)
	if err != nil && os.IsNotExist(err) {
		return false
	}
	return true
}

func FileExist(path string) bool {
	fi, err := os.Stat(path)
	if err != nil && os.IsNotExist(err) {
		return false
	}
	if !fi.Mode().IsRegular() {
		return false
	}
	return true
}

// nolint
func WriteFileWithFsync(name string, data []byte, perm os.FileMode) error {
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
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
	if Exist(dir) {
		_ = os.RemoveAll(dir)
	}
	MustExist(dir)
}

func HasFileOfType(dir, ext string) bool {
	files, err := os.ReadDir(dir)
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
	for _, dir := range dirs {
		files, err := ListFiles(dir)
		if err != nil {
			return err
		}
		for _, fPath := range files {
			if err := os.Remove(fPath); err != nil {
				return err
			}
		}
	}
	return nil
}

func ListFiles(dir string, extensions ...string) ([]string, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	res := make([]string, 0, len(files))
	for _, f := range files {
		if f.IsDir() && !f.Type().IsRegular() {
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
		res = append(res, filepath.Join(dir, f.Name()))
	}
	return res, nil
}
