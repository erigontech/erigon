//go:build !windows

package dir

import "os"

func ReadDir(name string) ([]os.DirEntry, error) {
	return os.ReadDir(name)
}
