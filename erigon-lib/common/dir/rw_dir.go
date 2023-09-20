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

func MustExist(path string) {
	const perm = 0764 // user rwx, group rw, other r
	if err := os.MkdirAll(path, perm); err != nil {
		panic(err)
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

func DeleteFilesOfType(dir string, exts ...string) {
	d, err := os.Open(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		panic(err)
	}
	defer d.Close()

	files, err := d.Readdir(-1)
	if err != nil {
		panic(err)
	}

	for _, file := range files {
		if !file.Mode().IsRegular() {
			continue
		}

		for _, ext := range exts {
			if filepath.Ext(file.Name()) == ext {
				_ = os.Remove(filepath.Join(dir, file.Name()))
			}
		}
	}
}
