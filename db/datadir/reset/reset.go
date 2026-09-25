package reset

import (
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"regexp"
	"slices"
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
	// AllowMixedStateBuilds skips the commitment/state build check.
	AllowMixedStateBuilds bool
	Dirs                  *datadir.Dirs

	stats struct {
		removed  Stats
		retained Stats
	}
}

// stateKVName matches a domain data file, capturing the domain and its step range.
var stateKVName = regexp.MustCompile(`^v[0-9]+\.[0-9]+-(accounts|storage|commitment)\.([0-9]+-[0-9]+)\.kv$`)

// checkStateBuilds reports step ranges whose commitment file would end up from a different build
// than its accounts/storage files.
//
// Commitment values address accounts and storage records by byte offset into the .kv of the same
// step range, so the two only agree when they come from the same build. Reset normalises files the
// preverified set knows about and leaves the rest untouched, so a range holding one of each ends up
// with offsets pointing into bytes that moved.
func (reset *Reset) checkStateBuilds() error {
	entries, err := os.ReadDir(reset.Dirs.SnapDomain)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	canonical := map[string]map[string]bool{}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		m := stateKVName.FindStringSubmatch(entry.Name())
		if m == nil {
			continue
		}
		domain, stepRange := m[1], m[2]
		_, known := reset.PreverifiedSnapshots.Get("domain/" + entry.Name())
		if canonical[stepRange] == nil {
			canonical[stepRange] = map[string]bool{}
		}
		canonical[stepRange][domain] = known
	}

	var mixed []string
	for stepRange, domains := range canonical {
		commitment, ok := domains["commitment"]
		if !ok {
			continue
		}
		for _, domain := range []string{"accounts", "storage"} {
			if state, ok := domains[domain]; ok && state != commitment {
				mixed = append(mixed, stepRange)
				break
			}
		}
	}
	if len(mixed) == 0 {
		return nil
	}
	slices.Sort(mixed)
	return fmt.Errorf("reset would leave commitment and its accounts/storage files from different "+
		"builds for step ranges %s, which breaks the byte offsets commitment holds into them. "+
		"Remove those ranges first with 'erigon snapshots rm-state-snapshots --step=<range>', or "+
		"re-run with --allow-mixed-state to proceed anyway", strings.Join(mixed, ", "))
}

func (reset *Reset) Run() (err error) {
	logger := reset.Logger
	if !reset.AllowMixedStateBuilds {
		if mixed := reset.checkStateBuilds(); mixed != nil {
			return mixed
		}
	}
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
		// Left over from Polygon support; harmless on chains that never had them.
		for _, extraDir := range []slashName{
			"heimdall",
			"polygon-bridge",
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
	} else if !errors.Is(err, fs.ErrNotExist) {
		err = fmt.Errorf("removing snapshot lock file: %w", err)
		return
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
	// Data files retained this pass, keyed by their preverified name. A data file is walked before
	// its sibling ".torrent" (os.ReadDir sorts, and "x.ef" < "x.ef.torrent"), so when an incorrect
	// torrent is later removed we can retract the stale data file it vouched for.
	retainedDataFiles := map[string]OsFilePath{}
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
			// An incorrect torrent for a preverified file means the data it describes is unverifiable
			// (stale local build): drop it too so the downloader re-fetches the canonical copy.
			if isTorrent && ok {
				if dataPath, retained := retainedDataFiles[itemName]; retained {
					if err = inner(dataPath, nil); err != nil {
						return fmt.Errorf("removing stale data file %v: %w", dataPath, err)
					}
					delete(retainedDataFiles, itemName)
					me.stats.retained.DataFiles--
					me.stats.removed.DataFiles++
				}
			}
		} else if !isTorrent {
			retainedDataFiles[itemName] = filePath
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
