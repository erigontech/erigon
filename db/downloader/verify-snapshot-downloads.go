// Copyright 2025 The Erigon Authors
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
	"context"
	"crypto/sha1" //nolint:gosec
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
	"github.com/pelletier/go-toml/v2"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/preverified"
)

// Preverified status strings used in progress output.
const (
	pvMatches  = "matches"
	pvMismatch = "doesn't match"
	pvNotIn    = "not in preverified"
	pvNoFile   = "no preverified.toml"
)

// SnapshotVerifySummary is returned after all snapshots have been checked.
type SnapshotVerifySummary struct {
	Total    int
	Errors   int
	Warnings int
}

// snapshotState collects all on-disk information about one snapshot name.
type snapshotState struct {
	// Unix-slash path relative to snapDir, e.g. "v1.0-mainnet-headers-0-1000.seg"
	// or "history/v1.0-mainnet-accounts.kv".
	name string

	// preverified.toml entry for this name, if present.
	pvItem    preverified.Item
	hasPV     bool
	pvPresent bool // preverified.toml file itself was found

	hasTorrent bool
	hasPart    bool
	hasData    bool
}

// output writes human-readable lines to out and counts errors/warnings. Safe for concurrent use.
type output struct {
	w        io.Writer
	mu       sync.Mutex
	errors   atomic.Int64
	warnings atomic.Int64
}

func newOutput(w io.Writer) *output { return &output{w: w} }

func (o *output) issue(severity, snapshot, message string) {
	o.mu.Lock()
	fmt.Fprintf(o.w, "%-5s  %s: %s\n", strings.ToUpper(severity), snapshot, message)
	o.mu.Unlock()
	switch severity {
	case "error":
		o.errors.Add(1)
	case "warn":
		o.warnings.Add(1)
	}
}

func (o *output) progress(snapshot string, percent float64, pvStatus string) {
	o.mu.Lock()
	fmt.Fprintf(o.w, "[%5.1f%%]  %-60s  %s\n", percent, snapshot, pvStatus)
	o.mu.Unlock()
}

func (o *output) summary(total int) SnapshotVerifySummary {
	return SnapshotVerifySummary{
		Total:    total,
		Errors:   int(o.errors.Load()),
		Warnings: int(o.warnings.Load()),
	}
}

// VerifySnapshotDownloads checks snapshot downloads against .torrent files and preverified.toml.
//
// The set of snapshots checked is the union of:
//   - All names in preverified.toml
//   - All files in the snap dir with ".torrent" stripped
//   - All files in the snap dir with ".part" stripped
//
// Findings are written as human-readable lines to out as they are discovered. After each snapshot
// completes a progress line is written with name, % done, and preverified status.
func VerifySnapshotDownloads(ctx context.Context, dirs datadir.Dirs, workers int, out io.Writer) (summary SnapshotVerifySummary, err error) {
	if workers <= 0 {
		workers = max(1, runtime.GOMAXPROCS(0)/2)
	}
	snapDir := dirs.Snap
	o := newOutput(out)

	// Load preverified.toml if present.
	var pvItems preverified.SortedItems
	pvPresent := false
	pvData, pvErr := os.ReadFile(dirs.PreverifiedPath())
	if pvErr == nil {
		pvPresent = true
		var rawMap map[string]string
		if err = toml.Unmarshal(pvData, &rawMap); err != nil {
			err = fmt.Errorf("parsing %s: %w", datadir.PreverifiedFileName, err)
			return
		}
		pvItems = preverified.ItemsFromMap(rawMap)
	} else if !errors.Is(pvErr, fs.ErrNotExist) {
		err = fmt.Errorf("reading %s: %w", datadir.PreverifiedFileName, pvErr)
		return
	}

	// Build the complete set of snapshot names from all sources.
	nameSet := make(map[string]*snapshotState)

	ensure := func(name string) *snapshotState {
		if s, ok := nameSet[name]; ok {
			return s
		}
		s := &snapshotState{name: name, pvPresent: pvPresent}
		if pvItems != nil {
			if item, ok := pvItems.Get(name); ok {
				s.hasPV = true
				s.pvItem = item
			}
		}
		nameSet[name] = s
		return s
	}

	// Seed from preverified.toml.
	for _, item := range pvItems {
		ensure(item.Name)
	}

	// Seed from files on disk.
	walkErr := fs.WalkDir(os.DirFS(snapDir), ".", func(relPath string, de fs.DirEntry, walkFileErr error) error {
		if walkFileErr != nil {
			o.issue("warn", relPath, fmt.Sprintf("walk error: %v", walkFileErr))
			return nil
		}
		if de.IsDir() {
			return nil
		}
		if name, ok := strings.CutSuffix(relPath, ".torrent"); ok {
			ensure(name).hasTorrent = true
			return nil
		}
		if name, ok := strings.CutSuffix(relPath, ".part"); ok {
			ensure(name).hasPart = true
			return nil
		}
		// Mark data file presence for known names.
		if s, exists := nameSet[relPath]; exists {
			s.hasData = true
		}
		return nil
	})
	if walkErr != nil {
		err = fmt.Errorf("walking snap dir %q: %w", snapDir, walkErr)
		return
	}

	// Second pass: detect data files for names not yet marked (walk order may miss them).
	for name, state := range nameSet {
		if !state.hasData {
			if _, statErr := os.Stat(filepath.Join(snapDir, filepath.FromSlash(name))); statErr == nil {
				state.hasData = true
			}
		}
	}

	states := make([]*snapshotState, 0, len(nameSet))
	for _, s := range nameSet {
		states = append(states, s)
	}
	total := len(states)

	var done atomic.Int64

	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(workers)

	for _, state := range states {
		eg.Go(func() error {
			pvStatus := verifyOneSnapshot(egCtx, snapDir, state, o)
			n := done.Add(1)
			o.progress(state.name, float64(n)/float64(total)*100, pvStatus)
			return nil
		})
	}
	_ = eg.Wait()

	summary = o.summary(total)
	fmt.Fprintf(out, "\nverified %d snapshots: %d error(s), %d warning(s)\n",
		summary.Total, summary.Errors, summary.Warnings)

	if ctx.Err() != nil {
		err = ctx.Err()
	}
	return
}

// verifyOneSnapshot checks one snapshot's .torrent, data file, .part file, and piece hashes.
// Returns the preverified status string for the progress line.
func verifyOneSnapshot(
	ctx context.Context,
	snapDir string,
	state *snapshotState,
	o *output,
) (pvStatus string) {
	name := state.name
	torrentRel := name + ".torrent"

	switch {
	case !state.pvPresent:
		pvStatus = pvNoFile
	case state.hasPV:
		pvStatus = pvMatches // refined below if hash differs
	default:
		pvStatus = pvNotIn
	}

	if !state.hasTorrent {
		if state.hasPV || state.hasPart {
			o.issue("error", name, "missing .torrent file")
		}
		checkPartAndData(name, state, -1, o)
		return
	}

	mi, err := metainfo.LoadFromFile(filepath.Join(snapDir, filepath.FromSlash(torrentRel)))
	if err != nil {
		o.issue("error", name, fmt.Sprintf("failed to load .torrent file: %v", err))
		return
	}

	infoHash := mi.HashInfoBytes()
	infoHashHex := infoHash.HexString()

	if state.hasPV {
		if state.pvItem.Hash == infoHashHex {
			pvStatus = pvMatches
		} else {
			pvStatus = pvMismatch
			o.issue("error", name,
				fmt.Sprintf("infohash mismatch: preverified=%s torrent=%s", state.pvItem.Hash, infoHashHex))
		}
	}

	info, err := mi.UnmarshalInfo()
	if err != nil {
		o.issue("error", name, fmt.Sprintf("failed to unmarshal torrent info: %v", err))
		return
	}

	infoName := info.BestName()
	if infoName != metainfo.NoName {
		if _, pathErr := storage.ToSafeFilePath(infoName); pathErr != nil {
			o.issue("error", name, fmt.Sprintf("info.Name %q is unsafe: %v", infoName, pathErr))
			return
		}
		if baseName := path.Base(name); infoName != baseName {
			o.issue("warn", name,
				fmt.Sprintf("info.Name %q doesn't match torrent filename %q", infoName, baseName))
		}
	}

	var expectedSize int64
	for _, fi := range info.UpvertedFiles() {
		expectedSize += fi.Length
	}

	checkPartAndData(name, state, expectedSize, o)

	if state.hasData && !state.hasPart && ctx.Err() == nil {
		if fi, statErr := os.Stat(filepath.Join(snapDir, filepath.FromSlash(name))); statErr == nil && fi.Size() == expectedSize {
			checkDataFilePerms(snapDir, name, o)
			verifyPieceHashes(ctx, snapDir, name, &info, infoHash, o)
		}
	}

	return
}

// checkPartAndData reports issues about the presence/absence of the data and .part files.
// expectedSize is -1 when unknown (no .torrent available).
func checkPartAndData(name string, state *snapshotState, expectedSize int64, o *output) {
	if state.hasData && state.hasPart {
		o.issue("error", name, "both data file and .part file exist simultaneously")
		return
	}
	if state.hasPart {
		o.issue("warn", name, "only .part file present; download is incomplete")
		return
	}
	if !state.hasData {
		severity := "warn"
		if state.hasPV {
			severity = "error"
		}
		o.issue(severity, name, "data file absent (no .part either)")
		return
	}
	// Data file present; check size if known.
	if expectedSize >= 0 {
		if fi, statErr := os.Stat(filepath.Join("", name)); statErr == nil && fi.Size() != expectedSize {
			o.issue("error", name,
				fmt.Sprintf("size mismatch: on-disk=%d torrent=%d", fi.Size(), expectedSize))
		}
	}
}

// checkDataFilePerms warns if a snapshot data file has unexpected permissions.
func checkDataFilePerms(snapDir, name string, o *output) {
	fi, err := os.Stat(filepath.Join(snapDir, filepath.FromSlash(name)))
	if err != nil {
		return
	}
	perm := fi.Mode().Perm()
	if perm&0o400 == 0 {
		o.issue("error", name, fmt.Sprintf("not readable by owner (mode=%v)", perm))
	}
	if perm&0o222 != 0 {
		o.issue("warn", name, fmt.Sprintf("writable (mode=%v); completed snapshots are expected read-only", perm))
	}
	if perm&0o002 != 0 {
		o.issue("error", name, fmt.Sprintf("world-writable (mode=%v)", perm))
	}
}

// verifyPieceHashes hashes each piece via the mmap/sparse-read storage (SEEK_DATA/SEEK_HOLE)
// using the same configuration as the main downloader, and reports hash mismatches.
func verifyPieceHashes(
	ctx context.Context,
	snapDir string,
	snapshotName string,
	info *metainfo.Info,
	infoHash metainfo.Hash,
	o *output,
) {
	client := newSnapStorage(snapDir, nil)
	defer client.Close()

	t, err := client.OpenTorrent(ctx, info, infoHash)
	if err != nil {
		o.issue("error", snapshotName, fmt.Sprintf("failed to open torrent storage: %v", err))
		return
	}
	defer t.Close()

	for i := range info.NumPieces() {
		if ctx.Err() != nil {
			return
		}
		p := info.Piece(i)
		expectedHash := p.V1Hash()
		if !expectedHash.Ok {
			continue
		}

		//nolint:gosec
		h := sha1.New()
		piece := t.Piece(p)

		var written int64
		var readErr error
		if wt, ok := piece.(io.WriterTo); ok {
			written, readErr = wt.WriteTo(h)
		} else {
			written, readErr = io.Copy(h, io.NewSectionReader(piece, 0, p.Length()))
		}

		if readErr != nil {
			o.issue("error", snapshotName, fmt.Sprintf("piece %d: read error: %v", i, readErr))
			continue
		}
		if written < p.Length() {
			o.issue("error", snapshotName,
				fmt.Sprintf("piece %d: short read: got %d of %d bytes (sparse or incomplete)", i, written, p.Length()))
			continue
		}

		var actualHash metainfo.Hash
		h.Sum(actualHash[:0])
		if actualHash != expectedHash.Value {
			o.issue("error", snapshotName,
				fmt.Sprintf("piece %d: hash mismatch: expected %x got %x", i, expectedHash.Value, actualHash))
		}
	}
}
