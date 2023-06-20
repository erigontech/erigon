package filecache

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
)

var errNotInBase = errors.New("not in base")

func joinValidate(paths ...string) (string, error) {
	if len(paths) == 0 {
		return "", nil
	}
	jp := filepath.Join(paths...)
	if !strings.HasPrefix(jp, paths[0]) {
		return "", &os.PathError{
			Op:   "join",
			Path: jp,
			Err:  errNotInBase,
		}
	}
	return jp, nil
}
