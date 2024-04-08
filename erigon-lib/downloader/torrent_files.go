package downloader

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"golang.org/x/exp/slices"
)

// TorrentFiles - does provide thread-safe CRUD operations on .torrent files
type TorrentFiles struct {
	lock sync.Mutex
	dir  string
}

func NewAtomicTorrentFiles(dir string) *TorrentFiles {
	return &TorrentFiles{dir: dir}
}

func (tf *TorrentFiles) Exists(name string) bool {
	tf.lock.Lock()
	defer tf.lock.Unlock()
	return tf.exists(name)
}

func (tf *TorrentFiles) exists(name string) bool {
	if !strings.HasSuffix(name, ".torrent") {
		name += ".torrent"
	}
	return dir.FileExist(filepath.Join(tf.dir, name))
}
func (tf *TorrentFiles) Delete(name string) error {
	tf.lock.Lock()
	defer tf.lock.Unlock()
	return tf.delete(name)
}

func (tf *TorrentFiles) delete(name string) error {
	if !strings.HasSuffix(name, ".torrent") {
		name += ".torrent"
	}
	return os.Remove(filepath.Join(tf.dir, name))
}

func (tf *TorrentFiles) Create(name string, res []byte) error {
	if !strings.HasSuffix(name, ".torrent") {
		name += ".torrent"
	}

	tf.lock.Lock()
	defer tf.lock.Unlock()
	return tf.create(filepath.Join(tf.dir, name), res)
}
func (tf *TorrentFiles) create(torrentFilePath string, res []byte) error {
	if len(res) == 0 {
		return fmt.Errorf("try to write 0 bytes to file: %s", torrentFilePath)
	}
	f, err := os.Create(torrentFilePath)
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
	return nil
}

func (tf *TorrentFiles) CreateTorrentFromMetaInfo(fPath string, mi *metainfo.MetaInfo) error {
	tf.lock.Lock()
	defer tf.lock.Unlock()
	return tf.createTorrentFromMetaInfo(fPath, mi)
}
func (tf *TorrentFiles) createTorrentFromMetaInfo(fPath string, mi *metainfo.MetaInfo) error {
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

func (tf *TorrentFiles) LoadByName(name string) (*torrent.TorrentSpec, error) {
	tf.lock.Lock()
	defer tf.lock.Unlock()
	return tf.load(filepath.Join(tf.dir, name))
}

func (tf *TorrentFiles) LoadByPath(fPath string) (*torrent.TorrentSpec, error) {
	tf.lock.Lock()
	defer tf.lock.Unlock()
	return tf.load(fPath)
}

func (tf *TorrentFiles) load(fPath string) (*torrent.TorrentSpec, error) {
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
func (tf *TorrentFiles) prohibitNewDownloads(t string) error {
	tf.lock.Lock()
	defer tf.lock.Unlock()
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
	prohibitedListJsonBytes, err := json.Marshal(prohibitedList)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	if _, err := f.Write(prohibitedListJsonBytes); err != nil {
		return fmt.Errorf("write: %w", err)
	}

	return f.Sync()
}

func (tf *TorrentFiles) newDownloadsAreProhibited(name string) (bool, error) {
	tf.lock.Lock()
	defer tf.lock.Unlock()
	f, err := os.OpenFile(filepath.Join(tf.dir, ProhibitNewDownloadsFileName), os.O_CREATE|os.O_APPEND|os.O_RDONLY, 0644)
	if err != nil {
		return false, err
	}
	defer f.Close()
	var prohibitedList []string
	torrentListJsonBytes, err := io.ReadAll(f)
	if err != nil {
		return false, fmt.Errorf("newDownloadsAreProhibited: read file: %w", err)
	}
	if len(torrentListJsonBytes) > 0 {
		if err := json.Unmarshal(torrentListJsonBytes, &prohibitedList); err != nil {
			return false, fmt.Errorf("newDownloadsAreProhibited: unmarshal: %w", err)
		}
	}
	for _, p := range prohibitedList {
		if strings.Contains(name, p) {
			return true, nil
		}
	}
	return false, nil
}
