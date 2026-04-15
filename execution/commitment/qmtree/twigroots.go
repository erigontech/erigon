package qmtree

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

// Twig roots file format:
//
//   [8 bytes]  prevLeaf — the previousLeafHash at the END of this step range
//   [32 bytes] twigRoot[0]
//   [32 bytes] twigRoot[1]
//   ...
//   [32 bytes] twigRoot[N-1]
//
// File size = 8 + N*32 bytes, where N = number of twigs in the step range.
// The prevLeaf is stored so that AppendLeaf can resume chaining after loading
// from snapshots.

const (
	twigRootsVersion = "v1.0"
	twigRootsName    = "qmtree-roots"
	twigRootsExt     = ".v"
)

func twigRootsPath(dir string, fromStep, toStep uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%s-%s.%d-%d%s", twigRootsVersion, twigRootsName, fromStep, toStep, twigRootsExt))
}

// writeTwigRoots computes twig roots from entry data for the given step range
// and writes them to a .twigs file. Also records the prevLeaf at the end of
// the range for hash chain resumption.
//
// The entries must be readable (from MDBX or snapshot .kv file) and the
// hasher is used to build the intra-twig Merkle trees.
func writeTwigRoots(dir string, fromStep, toStep, stepSize uint64, hasher Hasher,
	prevLeafStart common.Hash,
	readEntry func(txNum uint64) (pre, sc, trans common.Hash, err error)) error {

	fromTxNum := fromStep * stepSize
	toTxNum := toStep * stepSize

	firstTwigId := fromTxNum / LEAF_COUNT_IN_TWIG
	lastTxNum := toTxNum - 1
	lastTwigId := lastTxNum / LEAF_COUNT_IN_TWIG

	path := twigRootsPath(dir, fromStep, toStep)
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create twig roots file: %w", err)
	}
	defer f.Close()

	prevLeaf := prevLeafStart
	nullMt := hasher.nullMtForTwig()

	// Write placeholder for prevLeaf — we'll seek back and fill it at the end.
	var prevLeafBuf [32]byte
	if _, err := f.Write(prevLeafBuf[:]); err != nil {
		return err
	}

	for twigId := firstTwigId; twigId <= lastTwigId; twigId++ {
		twigBase := twigId * LEAF_COUNT_IN_TWIG
		twigEnd := twigBase + LEAF_COUNT_IN_TWIG
		if twigEnd > toTxNum {
			twigEnd = toTxNum
		}

		// Build the twig Merkle tree from leaf hashes.
		mt := nullMt.Clone()
		for txNum := twigBase; txNum < twigEnd; txNum++ {
			pre, sc, trans, err := readEntry(txNum)
			if err != nil {
				return fmt.Errorf("read entry txNum=%d for twig root: %w", txNum, err)
			}
			leafHash := ComputeLeafHash(pre, sc, trans, prevLeaf)
			mt[LEAF_COUNT_IN_TWIG+int(txNum-twigBase)] = leafHash
			prevLeaf = leafHash
		}
		mt.Sync(hasher, 0, int32(LEAF_COUNT_IN_TWIG-1))
		twigRoot := mt[1] // root of the intra-twig Merkle tree

		if _, err := f.Write(twigRoot[:]); err != nil {
			return fmt.Errorf("write twig root %d: %w", twigId, err)
		}
	}

	// Write prevLeaf at the start of the file.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err := f.Write(prevLeaf[:]); err != nil {
		return err
	}

	if err := f.Sync(); err != nil {
		return err
	}

	log.Info("qmtree: wrote twig roots",
		"fromStep", fromStep, "toStep", toStep,
		"twigs", lastTwigId-firstTwigId+1,
	)
	return nil
}

// loadTwigRoots reads twig roots from a .twigs file and returns
// (prevLeaf, twigRoots map[twigId]→root).
func loadTwigRoots(dir string, fromStep, toStep, stepSize uint64) (common.Hash, map[uint64]common.Hash, error) {
	path := twigRootsPath(dir, fromStep, toStep)
	f, err := os.Open(path)
	if err != nil {
		return common.Hash{}, nil, err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return common.Hash{}, nil, err
	}

	// Read prevLeaf.
	var prevLeaf common.Hash
	if _, err := io.ReadFull(f, prevLeaf[:]); err != nil {
		return common.Hash{}, nil, fmt.Errorf("read prevLeaf: %w", err)
	}

	// Read twig roots.
	dataSize := fi.Size() - 32 // subtract prevLeaf
	if dataSize%32 != 0 {
		return common.Hash{}, nil, fmt.Errorf("twig roots file size not aligned: %d", fi.Size())
	}
	numTwigs := int(dataSize / 32)
	firstTwigId := (fromStep * stepSize) / LEAF_COUNT_IN_TWIG

	roots := make(map[uint64]common.Hash, numTwigs)
	var buf [32]byte
	for i := 0; i < numTwigs; i++ {
		if _, err := io.ReadFull(f, buf[:]); err != nil {
			return common.Hash{}, nil, fmt.Errorf("read twig root %d: %w", i, err)
		}
		roots[firstTwigId+uint64(i)] = buf
	}

	return prevLeaf, roots, nil
}

// loadAllTwigRoots scans the domain directory for .twigs files and loads
// all twig roots. Returns (prevLeaf from highest step, all twig roots, highestStep).
func loadAllTwigRoots(dir string, stepSize uint64) (common.Hash, map[uint64]common.Hash, uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return common.Hash{}, nil, 0, nil
		}
		return common.Hash{}, nil, 0, err
	}

	allRoots := make(map[uint64]common.Hash)
	var highestStep uint64
	var highestPrevLeaf common.Hash

	prefix := twigRootsVersion + "-" + twigRootsName + "."
	for _, e := range entries {
		if e.IsDir() || !hasExtension(e.Name(), twigRootsExt) || !hasPrefix(e.Name(), prefix) {
			continue
		}
		var fromStep, toStep uint64
		pattern := prefix + "%d-%d" + twigRootsExt
		n, _ := fmt.Sscanf(e.Name(), pattern, &fromStep, &toStep)
		if n != 2 {
			continue
		}

		prevLeaf, roots, err := loadTwigRoots(dir, fromStep, toStep, stepSize)
		if err != nil {
			log.Warn("qmtree: failed to load twig roots", "file", e.Name(), "err", err)
			continue
		}
		for k, v := range roots {
			allRoots[k] = v
		}
		if toStep > highestStep {
			highestStep = toStep
			highestPrevLeaf = prevLeaf
		}
	}

	return highestPrevLeaf, allRoots, highestStep, nil
}

func hasExtension(name, ext string) bool {
	return len(name) > len(ext) && name[len(name)-len(ext):] == ext
}

func hasPrefix(name, prefix string) bool {
	return len(name) > len(prefix) && name[:len(prefix)] == prefix
}

// rebuildUpperTreeFromTwigRoots creates a Tree with the upper tree populated
// from persisted twig roots. No entry data is needed.
func rebuildUpperTreeFromTwigRoots(hasher Hasher, twigRoots map[uint64]common.Hash, nextTxNum uint64) *Tree {
	tree := NewTree(hasher, 0, nil, nil)

	if len(twigRoots) == 0 || nextTxNum == 0 {
		return tree
	}

	// After a clean resume from fully-frozen state, nextTxNum is aligned to
	// LEAF_COUNT_IN_TWIG (e.g. 1778125000 = 868225 * 2048). The youngest twig
	// is then the NEXT (currently empty) one at nextTxNum / LEAF_COUNT_IN_TWIG.
	// When nextTxNum is not aligned, the youngest twig is the partially-filled
	// one at (nextTxNum - 1) / LEAF_COUNT_IN_TWIG.
	var youngestTwigId uint64
	if nextTxNum%LEAF_COUNT_IN_TWIG == 0 {
		youngestTwigId = nextTxNum / LEAF_COUNT_IN_TWIG
	} else {
		youngestTwigId = (nextTxNum - 1) / LEAF_COUNT_IN_TWIG
	}

	// Set twig roots in the upper tree at twigRoot_LEVEL.
	for twigId, root := range twigRoots {
		pos := nodePos(twigRoot_LEVEL, twigId)
		tree.upperTree.SetNode(pos, root)
	}
	tree.youngestTwigId = youngestTwigId

	// Build a null twig for the youngest (possibly incomplete) twig and register
	// it in both the upper tree's active shards AND newTwigMap. The latter is
	// required so that SyncAndRoot / syncMtForYoungestTwig can find the live
	// youngest twig after a clean resume (NewTree leaves only newTwigMap[0]).
	nullMt := hasher.nullMtForTwig()
	nullTwigVal := nullTwig(nullMt[1])
	shardIdx, key := GetShardIdxAndKey(youngestTwigId)
	tree.upperTree.activeTwigShards[shardIdx][key] = &nullTwigVal

	// Replace the default newTwigMap[0] with an entry for the actual youngest twig.
	// Without this, syncMtForYoungestTwig dereferences a nil *Twig (panic).
	delete(tree.newTwigMap, 0)
	liveYoungestTwig := hasher.nullTwig().Clone()
	tree.newTwigMap[youngestTwigId] = liveYoungestTwig

	// Sync upper nodes to compute the root.
	var nList []uint64
	for twigId := uint64(0); twigId <= youngestTwigId; twigId++ {
		nList = append(nList, twigId/2)
	}
	// Deduplicate nList.
	seen := make(map[uint64]bool)
	var deduped []uint64
	for _, n := range nList {
		if !seen[n] {
			seen[n] = true
			deduped = append(deduped, n)
		}
	}

	tree.upperTree.SyncUpperNodes(hasher, deduped, youngestTwigId)

	return tree
}

// writeTwigRootsForStep computes and writes twig roots for a collated step.
// Reads entries from the snapshot .kv file (streaming, O(1) memory per entry).
func writeTwigRootsForStep(dir string, step, stepSize uint64, hasher Hasher, prevLeafAtStepStart common.Hash) error {
	kvPath := entryKVPath(dir, step, step+1)

	f, err := os.Open(kvPath)
	if err != nil {
		return fmt.Errorf("open .kv for twig roots: %w", err)
	}
	defer f.Close()

	readEntry := func(txNum uint64) (pre, sc, trans common.Hash, err error) {
		offset := int64(txNum-step*stepSize) * int64(snapshotEntrySize)
		var buf [snapshotEntrySize]byte
		if _, err := f.ReadAt(buf[:], offset); err != nil {
			return common.Hash{}, common.Hash{}, common.Hash{}, err
		}
		copy(pre[:], buf[8:40])
		copy(sc[:], buf[40:72])
		copy(trans[:], buf[72:104])
		return pre, sc, trans, nil
	}

	return writeTwigRoots(dir, step, step+1, stepSize, hasher, prevLeafAtStepStart, readEntry)
}


// mergeRootFiles concatenates individual per-step .v root files into a single
// merged .v file covering [fromStep, toStep). Old per-step files are deleted.
func mergeRootFiles(dir string, fromStep, toStep, stepSize uint64) {
	mergedPath := twigRootsPath(dir, fromStep, toStep)
	tmpPath := mergedPath + ".tmp"
	out, err := os.Create(tmpPath)
	if err != nil {
		log.Warn("qmtree: failed to create merged roots file", "err", err)
		return
	}

	// The merged file has one prevLeaf (from the last step) followed by all twig roots.
	// We stream-copy the twig roots from each step file, keeping only the last prevLeaf.
	// Collect all root files in the range, sorted by fromStep.
	type rootFile struct {
		from, to uint64
		path     string
	}
	var files []rootFile
	entries, _ := os.ReadDir(dir)
	prefix := twigRootsVersion + "-" + twigRootsName + "."
	for _, e := range entries {
		if !hasExtension(e.Name(), twigRootsExt) || !hasPrefix(e.Name(), prefix) {
			continue
		}
		var fs, ts uint64
		pattern := prefix + "%d-%d" + twigRootsExt
		if n, _ := fmt.Sscanf(e.Name(), pattern, &fs, &ts); n == 2 {
			if fs >= fromStep && ts <= toStep {
				files = append(files, rootFile{fs, ts, filepath.Join(dir, e.Name())})
			}
		}
	}
	sort.Slice(files, func(i, j int) bool { return files[i].from < files[j].from })

	if len(files) == 0 {
		out.Close()
		os.Remove(tmpPath)
		return
	}

	// Write placeholder prevLeaf.
	out.Write(make([]byte, 32))

	var lastPrevLeaf common.Hash
	for _, rf := range files {
		f, err := os.Open(rf.path)
		if err != nil {
			continue
		}
		// Read this file's prevLeaf (first 32 bytes).
		var prevLeaf [32]byte
		if _, err := io.ReadFull(f, prevLeaf[:]); err != nil {
			f.Close()
			continue
		}
		lastPrevLeaf = prevLeaf
		// Copy all twig roots (rest of file).
		io.Copy(out, f)
		f.Close()
	}

	// Write the last prevLeaf at the start.
	out.Seek(0, io.SeekStart)
	out.Write(lastPrevLeaf[:])
	out.Sync()
	out.Close()

	// Rename tmp to final path.
	os.Rename(tmpPath, mergedPath)

	// Delete all root files whose range falls within the merged range.
	entries, _ = os.ReadDir(dir)
	prefix = twigRootsVersion + "-" + twigRootsName + "."
	for _, e := range entries {
		if !hasExtension(e.Name(), twigRootsExt) || !hasPrefix(e.Name(), prefix) {
			continue
		}
		var fs, ts uint64
		pattern := prefix + "%d-%d" + twigRootsExt
		if n, _ := fmt.Sscanf(e.Name(), pattern, &fs, &ts); n == 2 {
			if fs >= fromStep && ts <= toStep && !(fs == fromStep && ts == toStep) {
				os.Remove(filepath.Join(dir, e.Name()))
			}
		}
	}

	log.Info("qmtree: merged root files",
		"fromStep", fromStep, "toStep", toStep,
	)
}

// nodePos and GetShardIdxAndKey are defined in tree.go.
