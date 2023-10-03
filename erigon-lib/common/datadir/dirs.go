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

package datadir

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/gofrs/flock"
	"github.com/ledgerwatch/erigon-lib/common/dir"
)

// Dirs is the file system folder the node should use for any data storage
// requirements. The configured data directory will not be directly shared with
// registered services, instead those can use utility methods to create/access
// databases or flat files
type Dirs struct {
	DataDir         string
	RelativeDataDir string // like dataDir, but without filepath.Abs() resolution
	Chaindata       string
	Tmp             string
	Snap            string
	SnapIdx         string
	SnapHistory     string
	SnapDomain      string
	SnapAccessors   string
	Downloader      string
	TxPool          string
	Nodes           string
}

func New(datadir string) Dirs {
	relativeDataDir := datadir
	if datadir != "" {
		var err error
		absdatadir, err := filepath.Abs(datadir)
		if err != nil {
			panic(err)
		}
		datadir = absdatadir
	}

	dirs := Dirs{
		RelativeDataDir: relativeDataDir,
		DataDir:         datadir,
		Chaindata:       filepath.Join(datadir, "chaindata"),
		Tmp:             filepath.Join(datadir, "temp"),
		Snap:            filepath.Join(datadir, "snapshots"),
		SnapIdx:         filepath.Join(datadir, "snapshots", "idx"),
		SnapHistory:     filepath.Join(datadir, "snapshots", "history"),
		SnapDomain:      filepath.Join(datadir, "snapshots", "domain"),
		SnapAccessors:   filepath.Join(datadir, "snapshots", "accessor"),
		Downloader:      filepath.Join(datadir, "snapshots", "db"),
		TxPool:          filepath.Join(datadir, "txpool"),
		Nodes:           filepath.Join(datadir, "nodes"),
	}
	dir.MustExist(dirs.Chaindata, dirs.Tmp,
		dirs.SnapIdx, dirs.SnapHistory, dirs.SnapDomain, dirs.SnapAccessors,
		dirs.Downloader, dirs.TxPool, dirs.Nodes)
	return dirs
}

var (
	ErrDataDirLocked = errors.New("datadir already used by another process")

	datadirInUseErrNos = map[uint]bool{11: true, 32: true, 35: true}
)

func convertFileLockError(err error) error {
	//nolint
	if errno, ok := err.(syscall.Errno); ok && datadirInUseErrNos[uint(errno)] {
		return ErrDataDirLocked
	}
	return err
}

func Flock(dirs Dirs) (*flock.Flock, bool, error) {
	// Lock the instance directory to prevent concurrent use by another instance as well as
	// accidental use of the instance directory as a database.
	l := flock.New(filepath.Join(dirs.DataDir, "LOCK"))
	locked, err := l.TryLock()
	if err != nil {
		return nil, false, convertFileLockError(err)
	}
	return l, locked, nil
}

// ApplyMigrations - if can get flock.
func ApplyMigrations(dirs Dirs) error {
	lock, locked, err := Flock(dirs)
	if err != nil {
		return err
	}
	if !locked {
		return nil
	}
	defer lock.Unlock()

	if err := downloaderV2Migration(dirs); err != nil {
		return err
	}
	//if err := erigonV3foldersV31Migration(dirs); err != nil {
	//	return err
	//}
	return nil
}

func downloaderV2Migration(dirs Dirs) error {
	// move db from `datadir/snapshot/db` to `datadir/downloader`
	if dir.Exist(filepath.Join(dirs.Snap, "db", "mdbx.dat")) { // migration from prev versions
		from, to := filepath.Join(dirs.Snap, "db", "mdbx.dat"), filepath.Join(dirs.Downloader, "mdbx.dat")
		if err := os.Rename(from, to); err != nil {
			//fall back to copy-file if folders are on different disks
			if err := copyFile(from, to); err != nil {
				return err
			}
		}
	}
	return nil
}

// nolint
func erigonV3foldersV31Migration(dirs Dirs) error {
	// migrate files db from `datadir/snapshot/warm` to `datadir/snapshots/domain`
	if dir.Exist(filepath.Join(dirs.Snap, "warm")) {
		warmDir := filepath.Join(dirs.Snap, "warm")
		moveFiles(warmDir, dirs.SnapDomain, ".kv")
		os.Rename(filepath.Join(dirs.SnapHistory, "salt.txt"), filepath.Join(dirs.Snap, "salt.txt"))
		moveFiles(warmDir, dirs.SnapDomain, ".kv")
		moveFiles(warmDir, dirs.SnapDomain, ".kvei")
		moveFiles(warmDir, dirs.SnapDomain, ".bt")
		moveFiles(dirs.SnapHistory, dirs.SnapAccessors, ".vi")
		moveFiles(dirs.SnapHistory, dirs.SnapAccessors, ".efi")
		moveFiles(dirs.SnapHistory, dirs.SnapAccessors, ".efei")
		moveFiles(dirs.SnapHistory, dirs.SnapIdx, ".ef")
	}
	return nil
}

// nolint
func moveFiles(from, to string, ext string) error {
	files, err := os.ReadDir(from)
	if err != nil {
		return fmt.Errorf("ReadDir: %w, %s", err, from)
	}
	for _, f := range files {
		if f.Type().IsDir() || !f.Type().IsRegular() {
			continue
		}
		if filepath.Ext(f.Name()) != ext {
			continue
		}
		_ = os.Rename(filepath.Join(from, f.Name()), filepath.Join(to, f.Name()))
	}
	return nil
}

func copyFile(from, to string) error {
	r, err := os.Open(from)
	if err != nil {
		return fmt.Errorf("please manually move file: from %s to %s. error: %w", from, to, err)
	}
	defer r.Close()
	w, err := os.Create(to)
	if err != nil {
		return fmt.Errorf("please manually move file: from %s to %s. error: %w", from, to, err)
	}
	defer w.Close()
	if _, err = w.ReadFrom(r); err != nil {
		w.Close()
		os.Remove(to)
		return fmt.Errorf("please manually move file: from %s to %s. error: %w", from, to, err)
	}
	if err = w.Sync(); err != nil {
		w.Close()
		os.Remove(to)
		return fmt.Errorf("please manually move file: from %s to %s. error: %w", from, to, err)
	}
	return nil
}
