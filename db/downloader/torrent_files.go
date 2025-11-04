// Copyright 2024 The Erigon Authors
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

package downloader

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/erigontech/erigon-lib/common/dir"
)

// AtomicTorrentFS - does provide thread-safe CRUD operations on .torrent files. TODO: Is this
// needed? Callers should be relying on inherently atomic FS operations anyway. Also we need this
// treatment for many files not just .torrent files.
type AtomicTorrentFS struct {
	lock sync.Mutex
	dir  string
}

func NewAtomicTorrentFS(dir string) *AtomicTorrentFS {
	return &AtomicTorrentFS{dir: dir}
}

func (tf *AtomicTorrentFS) Exists(name string) (bool, error) {
	tf.lock.Lock()
	defer tf.lock.Unlock()
	return tf.exists(name)
}

func (tf *AtomicTorrentFS) exists(name string) (bool, error) {
	if !strings.HasSuffix(name, ".torrent") {
		name += ".torrent"
	}
	return dir.FileExist(filepath.Join(tf.dir, filepath.FromSlash(name)))
}

func (tf *AtomicTorrentFS) Delete(name string) error {
	tf.lock.Lock()
	defer tf.lock.Unlock()
	return tf.delete(name)
}

func (tf *AtomicTorrentFS) delete(name string) error {
	if !strings.HasSuffix(name, ".torrent") {
		name += ".torrent"
	}
	return dir.RemoveFile(filepath.Join(tf.dir, name))
}

func (tf *AtomicTorrentFS) writeFile(name string, r io.Reader) (err error) {
	fPath := tf.nameToPath(name)
	f, err := os.CreateTemp(filepath.Dir(fPath), filepath.Base(fPath))
	if err != nil {
		return
	}
	// Defer this first so Close occurs before RemoveFile (Windows).
	removed := false
	defer func() {
		if removed {
			return
		}
		// I wonder if in some circumstances os.Rename can fail but the source file is gone. I doubt
		// it.
		err = errors.Join(dir.RemoveFile(f.Name()))
	}()
	closed := false
	defer func() {
		if closed {
			return
		}
		err = errors.Join(err, f.Close())
	}()
	_, err = io.Copy(f, r)
	if err != nil {
		return
	}
	err = f.Sync()
	if err != nil {
		return
	}
	// Checking Close error is required because on many systems Write errors are delayed.
	err = f.Close()
	closed = true
	if err != nil {
		return
	}
	err = os.Rename(f.Name(), fPath)
	if err != nil {
		return
	}
	removed = true
	return
}

func (tf *AtomicTorrentFS) createFromMetaInfo(name string, mi *metainfo.MetaInfo) error {
	r, w := io.Pipe()
	go func() {
		w.CloseWithError(mi.Write(w))
	}()
	return tf.writeFile(name, r)
}

// TODO: Refactor this to not return created? At this point all callers (could) assume the file does
// not exist.
func (tf *AtomicTorrentFS) CreateWithMetaInfo(info *metainfo.Info, additionalMetaInfo *metainfo.MetaInfo) (created bool, err error) {
	name := info.Name
	if !strings.HasSuffix(name, ".torrent") {
		name += ".torrent"
	}
	mi, err := CreateMetaInfo(info, additionalMetaInfo)
	if err != nil {
		return false, err
	}

	tf.lock.Lock()
	defer tf.lock.Unlock()

	exists, err := tf.exists(name)
	if err != nil {
		return false, err
	}
	if exists {
		return false, nil
	}
	if err = tf.createFromMetaInfo(name, mi); err != nil {
		return false, err
	}
	return true, nil
}

func (tf *AtomicTorrentFS) LoadByName(name string) (*torrent.TorrentSpec, error) {
	tf.lock.Lock()
	defer tf.lock.Unlock()
	return tf.load(filepath.Join(tf.dir, name))
}

func (tf *AtomicTorrentFS) LoadByPath(fPath string) (*torrent.TorrentSpec, error) {
	tf.lock.Lock()
	defer tf.lock.Unlock()
	return tf.load(fPath)
}

func (tf *AtomicTorrentFS) load(fPath string) (*torrent.TorrentSpec, error) {
	if !strings.HasSuffix(fPath, ".torrent") {
		fPath += ".torrent"
	}
	mi, err := metainfo.LoadFromFile(fPath)
	if err != nil {
		return nil, fmt.Errorf("LoadFromFile: %w, file=%s", err, fPath)
	}
	mi.AnnounceList = Trackers
	return torrent.TorrentSpecFromMetaInfoErr(mi)
}

func (tf *AtomicTorrentFS) nameToPath(name string) string {
	// Names are unix-style paths, and we need to convert them to the local path format.
	return filepath.Join(tf.dir, filepath.FromSlash(name))
}
