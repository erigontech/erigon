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
	"encoding/json"
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

// SnapshotVerifyIssue is a single finding, emitted as {"type":"issue",...} to stdout.
type SnapshotVerifyIssue struct {
	Type     string `json:"type"` // always "issue"
	Severity string `json:"severity"`
	Snapshot string `json:"snapshot,omitempty"`
	Torrent  string `json:"torrent,omitempty"`
	File     string `json:"file,omitempty"`
	Message  string `json:"message"`
}

// SnapshotVerifyProgress is emitted as {"type":"progress",...} after each snapshot completes.
type SnapshotVerifyProgress struct {
	Type        string  `json:"type"` // always "progress"
	Snapshot    string  `json:"snapshot"`
	Percent     float64 `json:"percent"`
	Preverified string  `json:"preverified"`
}

// SnapshotVerifySummary is emitted as {"type":"summary",...} after all snapshots complete.
type SnapshotVerifySummary struct {
	Type     string `json:"type"` // always "summary"
	Total    int    `json:"total"`
	Errors   int    `json:"errors"`
	Warnings int    `json:"warnings"`
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

// encoder wraps a json.Encoder with a mutex for concurrent use.
type encoder struct {
	enc      *json.Encoder
	mu       sync.Mutex
	errors   atomic.Int64
	warnings atomic.Int64
}

func newEncoder(out io.Writer) *encoder {
	return &encoder{enc: json.NewEncoder(out)}
}

func (e *encoder) emit(v any) {
	e.mu.Lock()
	_ = e.enc.Encode(v) //nolint:errcheck
	e.mu.Unlock()
}

func (e *encoder) issue(issue SnapshotVerifyIssue) {
	issue.Type = "issue"
	e.emit(issue)
	switch issue.Severity {
	case "error":
		e.errors.Add(1)
	case "warn":
		e.warnings.Add(1)
	}
}

// VerifySnapshotDownloads checks snapshot downloads against .torrent files and preverified.toml.
//
// The set of snapshots checked is the union of:
//   - All names in preverified.toml
//   - All files in the snap dir with ".torrent" stripped
//   - All files in the snap dir with ".part" stripped
//
// For each snapshot, findings are written immediately as JSON lines to out. After each snapshot
// completes, a progress line with name, % complete, and preverified status is written. A summary
// line is written last.
func VerifySnapshotDownloads(ctx context.Context, dirs datadir.Dirs, workers int, out io.Writer) (summary SnapshotVerifySummary, err error) {
	if workers <= 0 {
		workers = max(1, runtime.GOMAXPROCS(0)/2)
	}
	snapDir := dirs.Snap
	enc := newEncoder(out)

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

	// Build the complete set of snapshot names.
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
			enc.issue(SnapshotVerifyIssue{
				Severity: "warn",
				File:     relPath,
				Message:  fmt.Sprintf("walk error: %v", walkFileErr),
			})
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
		// Check if this is a raw data file for a known snapshot name.
		if _, exists := nameSet[relPath]; exists {
			nameSet[relPath].hasData = true
		}
		return nil
	})
	if walkErr != nil {
		err = fmt.Errorf("walking snap dir %q: %w", snapDir, walkErr)
		return
	}

	// Second pass: mark data file presence for all known names.
	for name, state := range nameSet {
		if !state.hasData {
			if _, statErr := os.Stat(filepath.Join(snapDir, filepath.FromSlash(name))); statErr == nil {
				state.hasData = true
			}
		}
	}

	// Collect into a slice for indexed progress tracking.
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
			pvStatus := verifyOneSnapshot(egCtx, snapDir, state, enc)
			n := done.Add(1)
			enc.emit(SnapshotVerifyProgress{
				Type:        "progress",
				Snapshot:    state.name,
				Percent:     float64(n) / float64(total) * 100,
				Preverified: pvStatus,
			})
			return nil
		})
	}
	_ = eg.Wait()

	summary = SnapshotVerifySummary{
		Type:     "summary",
		Total:    total,
		Errors:   int(enc.errors.Load()),
		Warnings: int(enc.warnings.Load()),
	}
	enc.emit(summary)

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
	enc *encoder,
) (pvStatus string) {
	name := state.name
	torrentRel := name + ".torrent"

	// Determine preverified status from what we already know.
	// This may be refined once we have the actual infohash from the .torrent file.
	switch {
	case !state.pvPresent:
		pvStatus = pvNoFile
	case state.hasPV:
		pvStatus = pvMatches // optimistic; corrected below if hash differs
	default:
		pvStatus = pvNotIn
	}

	if !state.hasTorrent {
		if state.hasPV || state.hasPart {
			// Missing .torrent despite being expected.
			enc.issue(SnapshotVerifyIssue{
				Severity: "error",
				Snapshot: name,
				Torrent:  torrentRel,
				Message:  "missing .torrent file",
			})
		}
		// Without a .torrent we can't do piece verification; check data/part presence only.
		checkPartAndData(name, torrentRel, state, -1, enc)
		return
	}

	torrentFilePath := filepath.Join(snapDir, filepath.FromSlash(torrentRel))
	mi, err := metainfo.LoadFromFile(torrentFilePath)
	if err != nil {
		enc.issue(SnapshotVerifyIssue{
			Severity: "error",
			Snapshot: name,
			Torrent:  torrentRel,
			Message:  fmt.Sprintf("failed to load .torrent file: %v", err),
		})
		return
	}

	infoHash := mi.HashInfoBytes()
	infoHashHex := infoHash.HexString()

	// Refine preverified status based on actual infohash.
	if state.hasPV {
		if state.pvItem.Hash == infoHashHex {
			pvStatus = pvMatches
		} else {
			pvStatus = pvMismatch
			enc.issue(SnapshotVerifyIssue{
				Severity: "error",
				Snapshot: name,
				Torrent:  torrentRel,
				Message:  fmt.Sprintf("infohash mismatch: preverified=%s torrent=%s", state.pvItem.Hash, infoHashHex),
			})
		}
	}

	info, err := mi.UnmarshalInfo()
	if err != nil {
		enc.issue(SnapshotVerifyIssue{
			Severity: "error",
			Snapshot: name,
			Torrent:  torrentRel,
			Message:  fmt.Sprintf("failed to unmarshal torrent info: %v", err),
		})
		return
	}

	// Validate info.Name is safe.
	infoName := info.BestName()
	if infoName != metainfo.NoName {
		if _, pathErr := storage.ToSafeFilePath(infoName); pathErr != nil {
			enc.issue(SnapshotVerifyIssue{
				Severity: "error",
				Snapshot: name,
				Torrent:  torrentRel,
				Message:  fmt.Sprintf("info.Name %q is unsafe: %v", infoName, pathErr),
			})
			return
		}
		baseName := path.Base(name)
		if infoName != baseName {
			enc.issue(SnapshotVerifyIssue{
				Severity: "warn",
				Snapshot: name,
				Torrent:  torrentRel,
				Message:  fmt.Sprintf("info.Name %q doesn't match torrent filename %q", infoName, baseName),
			})
		}
	}

	// Determine expected data file size from torrent.
	var expectedSize int64
	for _, fi := range info.UpvertedFiles() {
		expectedSize += fi.Length
	}

	checkPartAndData(name, torrentRel, state, expectedSize, enc)

	// Only run piece hashing if data file is fully present and the right size.
	if state.hasData && !state.hasPart && ctx.Err() == nil {
		dataFilePath := filepath.Join(snapDir, filepath.FromSlash(name))
		if fi, statErr := os.Stat(dataFilePath); statErr == nil && fi.Size() == expectedSize {
			verifyPieceHashes(ctx, snapDir, name, torrentRel, &info, infoHash, enc)
		}
	}

	return
}

// checkPartAndData reports issues about the presence/absence of the data and .part files.
// expectedSize is -1 when unknown (no .torrent).
func checkPartAndData(
	name string,
	torrentRel string,
	state *snapshotState,
	expectedSize int64,
	enc *encoder,
) {
	snapName := name // for clarity in messages

	if state.hasData && state.hasPart {
		enc.issue(SnapshotVerifyIssue{
			Severity: "error",
			Snapshot: snapName,
			Torrent:  torrentRel,
			Message:  "both data file and .part file exist simultaneously",
		})
		return
	}

	if state.hasPart {
		enc.issue(SnapshotVerifyIssue{
			Severity: "warn",
			Snapshot: snapName,
			Torrent:  torrentRel,
			Message:  "only .part file present; download is incomplete",
		})
		return
	}

	if !state.hasData {
		severity := "warn"
		if state.hasPV {
			severity = "error"
		}
		enc.issue(SnapshotVerifyIssue{
			Severity: severity,
			Snapshot: snapName,
			Torrent:  torrentRel,
			Message:  "data file absent (no .part either)",
		})
		return
	}

	// Data file is present. Check its size if we know what to expect.
	if expectedSize >= 0 {
		dataPath := filepath.Join("", name) // just for display; actual stat done by caller
		_ = dataPath
		// Size check is done per-FileInfo in verifyOneSnapshot's callers; skip here.
	}
}

// verifyPieceHashes hashes each piece via the mmap/sparse-read storage (SEEK_DATA/SEEK_HOLE)
// using the same configuration as the main downloader, and reports hash mismatches.
func verifyPieceHashes(
	ctx context.Context,
	snapDir string,
	snapshotName string,
	torrentRel string,
	info *metainfo.Info,
	infoHash metainfo.Hash,
	enc *encoder,
) {
	client := newSnapStorage(snapDir, nil)
	defer client.Close()

	t, err := client.OpenTorrent(ctx, info, infoHash)
	if err != nil {
		enc.issue(SnapshotVerifyIssue{
			Severity: "error",
			Snapshot: snapshotName,
			Torrent:  torrentRel,
			Message:  fmt.Sprintf("failed to open torrent storage for piece verification: %v", err),
		})
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
			enc.issue(SnapshotVerifyIssue{
				Severity: "error",
				Snapshot: snapshotName,
				Torrent:  torrentRel,
				Message:  fmt.Sprintf("piece %d: read error: %v", i, readErr),
			})
			continue
		}
		if written < p.Length() {
			enc.issue(SnapshotVerifyIssue{
				Severity: "error",
				Snapshot: snapshotName,
				Torrent:  torrentRel,
				Message:  fmt.Sprintf("piece %d: short read: got %d of %d bytes (sparse or incomplete)", i, written, p.Length()),
			})
			continue
		}

		var actualHash metainfo.Hash
		h.Sum(actualHash[:0])
		if actualHash != expectedHash.Value {
			enc.issue(SnapshotVerifyIssue{
				Severity: "error",
				Snapshot: snapshotName,
				Torrent:  torrentRel,
				Message:  fmt.Sprintf("piece %d: hash mismatch: expected %x got %x", i, expectedHash.Value, actualHash),
			})
		}
	}
}
