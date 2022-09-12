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
				_ = os.Remove(file.Name())
			}
		}
	}
}
