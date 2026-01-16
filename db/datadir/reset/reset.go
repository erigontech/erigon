package reset

import (
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/preverified"

	g "github.com/anacrolix/generics"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv/dbcfg"
)

type Stats struct {
	TorrentFiles int
	DataFiles    int
	UnknownFiles int
}

// Configuration struct to perform datadir resets.
type Reset struct {
	Logger               log.Logger
	RemoveFunc           func(name OsFilePath) error
	PreverifiedSnapshots preverified.SortedItems
	RemoveUnknown        bool
	RemoveLocal          bool
	Dirs                 *datadir.Dirs

	stats struct {
		removed  Stats
		retained Stats
	}
}

func (reset *Reset) Run() (err error) {
	logger := reset.Logger
	logger.Info("Resetting snapshots directory", "path", reset.pathForLog(datadir.SnapDir))
	err = reset.doSnapshots()
	if err != nil {
		err = fmt.Errorf("resetting snapshots: %w", err)
		return
	}
	logger.Info("Files NOT removed from snapshots directory",
		"torrents", reset.stats.retained.TorrentFiles,
		"data", reset.stats.retained.DataFiles)
	logger.Info("Files removed from snapshots directory",
		"torrents", reset.stats.removed.TorrentFiles,
		"data", reset.stats.removed.DataFiles)
	// Remove chaindata last, so that the config is available if there's an error.
	if reset.RemoveLocal {
		for _, extraDir := range []slashName{
			dbcfg.HeimdallDB,
			dbcfg.PolygonBridgeDB,
		} {
			// Probably shouldn't log these unless they existed, it would confuse the user for
			// unrelated chains.
			ra := reset.makeRemoveAll(reset.dataDirOsPath().Join(extraDir.MustLocalize()))
			ra.warnNoRoot = false
			err = ra.do()
			if err != nil {
				return fmt.Errorf("removing extra dir %q: %w", extraDir, err)
			}
		}
		logger.Info("Removing chaindata dir", "path", reset.pathForLog(dbcfg.ChainDB))
		ra := reset.makeRemoveAll(slashName(dbcfg.ChainDB).MustLocalize())
		err = ra.do()
		if err != nil {
			err = fmt.Errorf("removing chaindata dir: %w", err)
			return
		}
	}
	err = reset.remove(OsFilePath(reset.Dirs.PreverifiedPath()))
	if err == nil {
		logger.Info("Removed snapshots lock file", "path", datadir.PreverifiedFileName)
	} else {
		if !errors.Is(err, fs.ErrNotExist) {
			err = fmt.Errorf("removing snapshot lock file: %w", err)
			return
		}
	}
	return nil
}

func (reset *Reset) dataDirOsPath() OsFilePath {
	return OsFilePath(reset.Dirs.DataDir)
}

func (reset *Reset) remove(name OsFilePath) error {
	return reset.RemoveFunc(reset.dataDirOsPath().JoinClobbering(name))
}

func (reset *Reset) makeRemoveAll(root OsFilePath) removeAll {
	return removeAll{
		logger: reset.Logger,
		removeFunc: func(name OsFilePath, info fs.FileInfo) error {
			return reset.RemoveFunc(name)
		},
		root:       reset.dataDirOsPath().JoinClobbering(root),
		warnNoRoot: true,
	}
}

type removeAllRemoveFunc func(name OsFilePath, info os.FileInfo) error

func (me *removeAll) wrapRemove(wrapper func(inner removeAllRemoveFunc, name OsFilePath, info os.FileInfo) error) {
	inner := me.removeFunc
	me.removeFunc = func(name OsFilePath, info os.FileInfo) error {
		return wrapper(inner, name, info)
	}
}

// Probably want to render full/real name rather than rooted inside fs.
func (reset *Reset) pathForLog(path string) string {
	return path
}

type resetItemInfo struct {
	filePath OsFilePath
	// For logging, decision is already made based on this.
	snapName      string
	hash          g.Option[string]
	isTorrent     bool
	inPreverified bool
}

func (me *Reset) doSnapshots() (err error) {
	snapDir := me.dataDirOsPath().Join(datadir.SnapDir)
	ra := me.makeRemoveAll(snapDir)
	ra.wrapRemove(func(inner removeAllRemoveFunc, filePath OsFilePath, info fs.FileInfo) error {
		itemName := string(filePath.mustLocalRelSlash(snapDir))
		itemName, _ = strings.CutSuffix(itemName, ".part")
		itemName, isTorrent := strings.CutSuffix(itemName, ".torrent")
		item, ok := me.PreverifiedSnapshots.Get(itemName)
		doRemove := me.decideRemove(resetItemInfo{
			filePath:      filePath,
			snapName:      itemName,
			hash:          func() g.Option[string] { return g.OptionFromTuple(item.Hash, ok) }(),
			isTorrent:     isTorrent,
			inPreverified: ok,
		})
		stats := &me.stats.retained
		if doRemove {
			stats = &me.stats.removed
			err = inner(filePath, info)
			if err != nil {
				return fmt.Errorf("removing file %v: %w", filePath, err)
			}
		}
		if isTorrent {
			stats.TorrentFiles++
		} else {
			stats.DataFiles++
		}
		return nil
	})
	// Skip to the contents of the snapshots dir, we don't expect this directory to be empty after
	// traversal.
	return ra.dir(snapDir)
}

// Decides whether to remove a file, and logs the reasoning.
func (me *Reset) decideRemove(file resetItemInfo) bool {
	logger := me.Logger
	name := file.snapName
	if !file.inPreverified {
		if !me.RemoveUnknown {
			logger.Debug("skipping unknown file", "name", name)
		}
		return me.RemoveUnknown
	}
	// TODO: missing or incorrect torrent delete data file?
	if file.isTorrent {
		mi, err := me.loadMetainfoFromFile(file.filePath)
		if err != nil {
			logger.Error("error loading metainfo file", "name", name, "err", err)
			return true
		}
		expectedHash := file.hash.Unwrap()
		if mi.HashInfoBytes().String() == expectedHash {
			logger.Debug("torrent file matches preverified hash", "name", name)
			return false
		} else {
			logger.Debug("removing metainfo file with incorrect infohash",
				"name", name,
				"expected", expectedHash,
				"actual", mi.HashInfoBytes())
			return true
		}
	} else {
		// No checks required. Downloader will clobber it into shape after reset on next run.
		logger.Debug("skipping expected snapshot", "name", name)
		return false
	}
}

func (me *Reset) loadMetainfoFromFile(path OsFilePath) (mi *metainfo.MetaInfo, err error) {
	f, err := os.Open(string(path))
	if err != nil {
		return
	}
	defer f.Close()
	var buf bufio.Reader
	buf.Reset(f)
	return metainfo.Load(&buf)
}
