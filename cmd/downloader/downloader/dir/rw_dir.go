package dir

import (
	"errors"
	"fmt"
	"path/filepath"
	"syscall"

	"github.com/gofrs/flock"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/log/v3"
)

// Rw - type to represent Read-Write access to directory
// if some code accept this typ - it can be sure: dir exists, no other process now write there
// We have no specific Ro type, just use `string` for Ro directory path
type Rw struct {
	dirLock *flock.Flock // prevents concurrent use of instance directory
	Path    string
}

func convertFileLockError(err error, dir string) error {
	if errno, ok := err.(syscall.Errno); ok && dirInUseErrnos[uint(errno)] {
		return fmt.Errorf("%w: %s\n", ErrDirUsed, dir)
	}
	return err
}

var dirInUseErrnos = map[uint]bool{11: true, 32: true, 35: true}

var (
	ErrDirUsed = errors.New("datadir already used by another process")
)

func OpenRw(dir string) (*Rw, error) {
	common.MustExist(dir)

	// Lock the instance directory to prevent concurrent use by another instance as well as
	// accidental use of the instance directory as a database.
	l := flock.New(filepath.Join(dir, "LOCK"))
	locked, err := l.TryLock()
	if err != nil {
		return nil, convertFileLockError(err, dir)
	}
	if !locked {
		return nil, fmt.Errorf("%w: %s\n", ErrDirUsed, dir)
	}
	return &Rw{dirLock: l, Path: dir}, nil
}
func (t *Rw) Close() {
	// Release instance directory lock.
	if t.dirLock != nil {
		if err := t.dirLock.Unlock(); err != nil {
			log.Error("Can't release snapshot dir lock", "err", err)
		}
		t.dirLock = nil
	}
}
