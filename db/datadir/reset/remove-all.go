package reset

import (
	"errors"
	"fmt"
	"io/fs"
	"os"

	"github.com/erigontech/erigon/common/log/v3"
)

// Removes the contents of directories, and symlinks to *non-directories*. Non-directory symlink
// targets will be cleaned up as appropriate if the target is found. We also remove anything else.
type removeAll struct {
	logger     log.Logger
	root       OsFilePath
	removeFunc removeAllRemoveFunc
	warnNoRoot bool
}

// Calls the provided removeFunc which does filtering, stat collection and permission checks
// determined by the caller.
func (me *removeAll) remove(name OsFilePath, info os.FileInfo) error {
	return me.removeFunc(name, info)
}

// Removes the contents of a directory. Does not remove the directory.
func (me *removeAll) dir(name OsFilePath) error {
	entries, err := os.ReadDir(string(name))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			err = nil
		}
		return err
	}
	for _, de := range entries {
		info, err := de.Info()
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		fullName := name.Join(OsFilePath(de.Name()))
		err = me.inner(fullName, info)
		if err != nil {
			return err
		}
	}
	return nil
}

// Remove name if appropriate.
func (me *removeAll) inner(name OsFilePath, fi fs.FileInfo) error {
	switch modeType := fi.Mode().Type(); modeType {
	case fs.ModeDir:
		err := me.dir(name)
		if err != nil {
			return err
		}
		// We don't super care if directories fail to get removed.
		if err := me.remove(name, fi); err != nil {
			// Should handle the case where it's a mount point.
			me.logger.Debug("Error removing directory", "name", name, "err", err)
		}
		return nil
	case fs.ModeSymlink:
		targetInfo, err := os.Stat(string(name))
		if err != nil {
			// Dangling symlinks are bad because we can't decide if we should remove them because we
			// want to preserve links to directories.
			return fmt.Errorf("statting symlink target: %w", err)
		}
		if targetInfo.IsDir() {
			// Remove the contents only
			return me.dir(name)
		} else {
			// Remove the link itself
			return me.remove(name, fi)
		}
	default:
		return me.remove(name, fi)
	}
}

func (me *removeAll) do() error {
	println(me.root)
	info, err := os.Lstat(string(me.root))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			if me.warnNoRoot {
				me.logger.Warn("Error removing top-level", "root", me.root, "err", err)
			}
			return nil
		}
		return err
	}
	return me.inner(me.root, info)
}
