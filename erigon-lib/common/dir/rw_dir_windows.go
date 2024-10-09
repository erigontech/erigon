//go:build windows

package dir

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

func ReadDir(name string) ([]os.DirEntry, error) {
	files, err := os.ReadDir(name)
	if err != nil {
		// some windows remote drived return this error
		// when they are empty - should really be handled
		// in os.ReadDir but is not
		// - looks likey fixed in go 1.22
		if errors.Is(err, windows.ERROR_NO_MORE_FILES) {
			return nil, nil
		}
	}
	return files, err
}
