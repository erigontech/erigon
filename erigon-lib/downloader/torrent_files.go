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
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"

	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/fastjson"
)

// AtomicTorrentFS - does provide thread-safe CRUD operations on .torrent files
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
	return dir.FileExist(filepath.Join(tf.dir, name))
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
	return os.Remove(filepath.Join(tf.dir, name))
}

func (tf *AtomicTorrentFS) Create(name string, res []byte) (ts *torrent.TorrentSpec, created bool, err error) {
	tf.lock.Lock()
	defer tf.lock.Unlock()

	exists, err := tf.exists(name)
	if err != nil {
		return nil, false, err
	}
	if !exists {
		err = tf.create(name, res)
		if err != nil {
			return nil, false, err
		}
	}

	ts, err = tf.load(filepath.Join(tf.dir, name))
	if err != nil {
		return nil, false, err
	}
	return ts, false, nil
}

func (tf *AtomicTorrentFS) create(name string, res []byte) error {
	if !strings.HasSuffix(name, ".torrent") {
		name += ".torrent"
	}
	if len(res) == 0 {
		return fmt.Errorf("try to write 0 bytes to file: %s", name)
	}

	fPath := filepath.Join(tf.dir, name)
	f, err := os.Create(fPath + ".tmp")
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err = f.Write(res); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(fPath+".tmp", fPath); err != nil {
		return err
	}

	return nil
}

func (tf *AtomicTorrentFS) createFromMetaInfo(fPath string, mi *metainfo.MetaInfo) error {
	file, err := os.Create(fPath + ".tmp")
	if err != nil {
		return err
	}
	defer file.Close()
	if err := mi.Write(file); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(fPath+".tmp", fPath); err != nil {
		return err
	}
	return nil
}

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
	if err = tf.createFromMetaInfo(filepath.Join(tf.dir, name), mi); err != nil {
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

const ProhibitNewDownloadsFileName = "prohibit_new_downloads.lock"

// Erigon "download once" - means restart/upgrade/downgrade will not download files (and will be fast)
// After "download once" - Erigon will produce and seed new files
// Downloader will able: seed new files (already existing on FS), download uncomplete parts of existing files (if Verify found some bad parts)
func (tf *AtomicTorrentFS) ProhibitNewDownloads(t string) error {
	tf.lock.Lock()
	defer tf.lock.Unlock()
	return tf.prohibitNewDownloads(t)
}

func (tf *AtomicTorrentFS) prohibitNewDownloads(t string) error {
	// open or create file ProhibitNewDownloadsFileName
	f, err := os.OpenFile(filepath.Join(tf.dir, ProhibitNewDownloadsFileName), os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer f.Close()
	var prohibitedList []string
	torrentListJsonBytes, err := io.ReadAll(f)
	if err != nil {
		return fmt.Errorf("read file: %w", err)
	}
	if len(torrentListJsonBytes) > 0 {
		if err := json.Unmarshal(torrentListJsonBytes, &prohibitedList); err != nil {
			return fmt.Errorf("unmarshal: %w", err)
		}
	}
	if slices.Contains(prohibitedList, t) {
		return nil
	}
	prohibitedList = append(prohibitedList, t)
	f.Close()

	// write new prohibited list by opening the file in truncate mode
	f, err = os.OpenFile(filepath.Join(tf.dir, ProhibitNewDownloadsFileName), os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open file for writing: %w", err)
	}
	defer f.Close()
	prohibitedListJsonBytes, err := fastjson.Marshal(prohibitedList)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	if _, err := f.Write(prohibitedListJsonBytes); err != nil {
		return fmt.Errorf("write: %w", err)
	}

	return f.Sync()
}

func (tf *AtomicTorrentFS) NewDownloadsAreProhibited(name string) (bool, error) {
	tf.lock.Lock()
	defer tf.lock.Unlock()
	return tf.newDownloadsAreProhibited(name)
}

func (tf *AtomicTorrentFS) newDownloadsAreProhibited(name string) (bool, error) {
	f, err := os.OpenFile(filepath.Join(tf.dir, ProhibitNewDownloadsFileName), os.O_CREATE|os.O_RDONLY, 0644)
	if err != nil {
		return false, err
	}
	defer f.Close()
	var prohibitedList []string
	torrentListJsonBytes, err := io.ReadAll(f)
	if err != nil {
		return false, fmt.Errorf("NewDownloadsAreProhibited: read file: %w", err)
	}
	if len(torrentListJsonBytes) > 0 {
		if err := json.Unmarshal(torrentListJsonBytes, &prohibitedList); err != nil {
			return false, fmt.Errorf("NewDownloadsAreProhibited: unmarshal: %w", err)
		}
	}
	for _, p := range prohibitedList {
		if strings.Contains(name, p) {
			return true, nil
		}
	}
	return false, nil
}
