package reset

import (
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/preverified"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/version"

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
	// AllowMixedStateBuilds skips the commitment/state build check.
	AllowMixedStateBuilds bool
	Dirs                  *datadir.Dirs

	stats struct {
		removed  Stats
		retained Stats
	}
}

var stateFileName = regexp.MustCompile(`^(v[0-9]+(?:\.[0-9]+)?)-([a-z]+)\.([0-9]+)-([0-9]+)\.(kv|v|ef)$`)

type stateFile struct {
	ver              string
	entity           string
	ext              string
	stepRange        string
	fromStep, toStep uint64
}

func parseStateFile(name string) (stateFile, bool) {
	m := stateFileName.FindStringSubmatch(name)
	if m == nil {
		return stateFile{}, false
	}
	fromStep, err := strconv.ParseUint(m[3], 10, 64)
	if err != nil {
		return stateFile{}, false
	}
	toStep, err := strconv.ParseUint(m[4], 10, 64)
	if err != nil || fromStep > toStep {
		return stateFile{}, false
	}
	return stateFile{ver: m[1], entity: m[2], ext: m[5], stepRange: m[3] + "-" + m[4], fromStep: fromStep, toStep: toStep}, true
}

// domainBuild is the file a step range will hold for one entity once reset is done and the
// downloader has fetched what the manifest describes. Only the highest version of a logical file is
// opened, so that is the one recorded.
type domainBuild struct {
	name          string
	path          string
	retainedLocal bool
	ver           version.Version
	fromStep      uint64
	toStep        uint64
}

// referenced reports whether a commitment file of this version and span stores shortened keys,
// which are the byte offsets that a differing build invalidates. Steps are passed with a step size
// of one, since the file name already counts in steps.
func (b *domainBuild) referenced() bool {
	return state.CommitmentBranchReferenced(b.ver, 1, b.fromStep, b.toStep)
}

func (b *domainBuild) keeps(ver version.Version, name string) bool {
	return b != nil && (b.ver.Cmp(ver) > 0 || (b.ver.Cmp(ver) == 0 && b.name <= name))
}

// checkStateBuilds reports step ranges whose commitment file would end up from a different build
// than its accounts/storage files.
//
// Commitment values address accounts and storage records by byte offset into the .kv of the same
// step range, so the two only agree when they come from the same build. Reset normalises whatever
// the manifest describes and leaves the rest untouched, so a range holding one of each ends up with
// offsets pointing into bytes that moved.
func (reset *Reset) checkStateBuilds() error {
	// Everything the manifest does not describe is about to be removed, so no local build survives
	// to be paired with a canonical one.
	if reset.RemoveUnknown {
		return nil
	}

	builds := map[string]map[string]*domainBuild{}
	record := func(fileName, path string) {
		f, ok := parseStateFile(fileName)
		if !ok {
			return
		}
		ver, err := version.ParseVersion(f.ver)
		if err != nil {
			return
		}
		stepRange, entity := f.stepRange, f.entity+"."+f.ext
		if builds[stepRange] == nil {
			builds[stepRange] = map[string]*domainBuild{}
		}
		if builds[stepRange][entity].keeps(ver, fileName) {
			return
		}
		builds[stepRange][entity] = &domainBuild{
			name:          fileName,
			path:          path,
			retainedLocal: path != "",
			ver:           ver,
			fromStep:      f.fromStep,
			toStep:        f.toStep,
		}
	}

	accessors := map[string]*domainBuild{}
	recordAccessor := func(fileName, path string) {
		v, unversioned, ok := strings.Cut(fileName, "-")
		if !ok {
			return
		}
		ver, err := version.ParseVersion(v)
		if err != nil || accessors[unversioned].keeps(ver, fileName) {
			return
		}
		accessors[unversioned] = &domainBuild{name: fileName, path: path, ver: ver}
	}

	// A file the manifest describes ends up canonical whether or not it is on disk now: reset drops
	// the lock file, and the next sync fetches or repairs it.
	for _, item := range reset.PreverifiedSnapshots {
		dir, name, _ := strings.Cut(item.Name, "/")
		switch dir {
		case "domain":
			record(name, "")
			recordAccessor(name, "")
		case "history", "idx":
			record(name, "")
		}
	}

	for _, dir := range []string{reset.Dirs.SnapDomain, reset.Dirs.SnapHistory, reset.Dirs.SnapIdx} {
		entries, err := os.ReadDir(dir)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		info, err := os.Lstat(dir)
		walkedAtStartup := err == nil && info.IsDir()
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			name, kept := entry.Name(), true
			if walkedAtStartup {
				name, kept = reset.Dirs.RenamedOldVersion(dir, entry.Name())
			}
			if !kept {
				continue
			}
			path := filepath.Join(dir, entry.Name())
			if _, known := reset.PreverifiedSnapshots.Get(filepath.Base(dir) + "/" + name); known {
				path = ""
			}
			record(name, path)
			if dir == reset.Dirs.SnapDomain {
				recordAccessor(name, path)
			}
		}
	}

	entityEnds := map[string]uint64{}
	for _, files := range builds {
		for entity, f := range files {
			if f.path == "" || opens(f.path) {
				entityEnds[entity] = max(entityEnds[entity], f.toStep)
			}
		}
	}
	visibleEnd := uint64(math.MaxUint64)
	for _, end := range entityEnds {
		visibleEnd = min(visibleEnd, end)
	}
	usable := func(commitment *domainBuild) bool {
		span := fmt.Sprintf("commitment.%d-%d.", commitment.fromStep, commitment.toStep)
		for _, accessor := range []struct {
			kind statecfg.Accessors
			ext  string
			open func(path string) error
		}{
			{statecfg.AccessorHashMap, "kvi", openHashMap},
			{statecfg.AccessorBTree, "bt", nil},
			{statecfg.AccessorExistence, "kvei", nil},
		} {
			if !statecfg.Schema.CommitmentDomain.Accessors.Has(accessor.kind) {
				continue
			}
			a, ok := accessors[span+accessor.ext]
			if !ok || (a.path != "" && accessor.open != nil && accessor.open(a.path) != nil) {
				return false
			}
		}
		return commitment.path == "" || opens(commitment.path)
	}
	hidden := func(commitment *domainBuild) bool {
		for _, domains := range builds {
			merged := domains["commitment.kv"]
			if merged != nil && merged != commitment && merged.toStep <= visibleEnd &&
				merged.fromStep <= commitment.fromStep && commitment.toStep <= merged.toStep && usable(merged) {
				return true
			}
		}
		return false
	}

	var mixed []string
	for stepRange, domains := range builds {
		commitment := domains["commitment.kv"]
		if commitment == nil || !commitment.referenced() || hidden(commitment) {
			continue
		}
		for _, domain := range []string{"accounts", "storage"} {
			st := domains[domain+".kv"]
			if st == nil {
				continue
			}
			if st.retainedLocal != commitment.retainedLocal {
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

func opens(path string) bool {
	d, err := seg.NewDecompressor(path)
	if err != nil {
		return false
	}
	d.Close()
	return true
}

func openHashMap(path string) error {
	idx, err := recsplit.OpenIndex(path)
	if err != nil {
		return err
	}
	idx.Close()
	return nil
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

func (reset *Reset) doSnapshots() (err error) {
	snapDir := reset.dataDirOsPath().Join(datadir.SnapDir)
	ra := reset.makeRemoveAll(snapDir)
	ra.wrapRemove(func(inner removeAllRemoveFunc, filePath OsFilePath, info fs.FileInfo) error {
		itemName := string(filePath.mustLocalRelSlash(snapDir))
		itemName, _ = strings.CutSuffix(itemName, ".part")
		itemName, isTorrent := strings.CutSuffix(itemName, ".torrent")
		item, ok := reset.PreverifiedSnapshots.Get(itemName)
		doRemove := reset.decideRemove(resetItemInfo{
			filePath:      filePath,
			snapName:      itemName,
			hash:          func() g.Option[string] { return g.OptionFromTuple(item.Hash, ok) }(),
			isTorrent:     isTorrent,
			inPreverified: ok,
		})
		stats := &reset.stats.retained
		if doRemove {
			stats = &reset.stats.removed
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
func (reset *Reset) decideRemove(file resetItemInfo) bool {
	logger := reset.Logger
	name := file.snapName
	if !file.inPreverified {
		if !reset.RemoveUnknown {
			logger.Debug("skipping unknown file", "name", name)
		}
		return reset.RemoveUnknown
	}
	if file.isTorrent {
		mi, err := reset.loadMetainfoFromFile(file.filePath)
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

func (reset *Reset) loadMetainfoFromFile(path OsFilePath) (mi *metainfo.MetaInfo, err error) {
	f, err := os.Open(string(path))
	if err != nil {
		return
	}
	defer f.Close()
	var buf bufio.Reader
	buf.Reset(f)
	return metainfo.Load(&buf)
}
