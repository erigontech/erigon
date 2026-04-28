// Copyright 2026 The Erigon Authors
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

// Versioned V2 chain manifest publish. Each call to RollingV2Publisher.Publish
// creates a fresh chain.v2.<seq>.toml plus its .torrent sidecar, registers the
// new torrent with the underlying Downloader, and (optionally) trims the oldest
// retained generation. Old generations stay seedable for as long as they're in
// the rolling window — peers that captured a stale ENR snapshot can still fetch
// the infohash they advertised at handshake time.
//
// The rolling buffer is the bound on disk space; Cleanup() is the
// defence-in-depth for crashed-mid-publish or shrunk-cap cases.

package downloader

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync"

	"github.com/anacrolix/torrent/metainfo"

	"github.com/erigontech/erigon/common/dir"
	snapshotinv "github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/p2p/enr"
)

// ChainTomlV2BaseName is the prefix every V2 manifest filename starts
// with. Generations append `.<seq>.toml` so the .toml extension stays
// terminal for tool recognition (parsers, IDEs, etc).
const ChainTomlV2BaseName = "chain.v2"

// DefaultV2MaxRetained is the rolling buffer cap when no override is
// supplied. Bounds disk space deterministically even if Cleanup() is
// never called.
const DefaultV2MaxRetained = 64

// chainTomlV2NameRE matches chain.v2.<seq>.toml — the only filename
// shape RollingV2Publisher emits and recognises.
var chainTomlV2NameRE = regexp.MustCompile(`^chain\.v2\.(\d+)\.toml$`)

// ChainTomlV2FileNameForSeq formats a generational filename for the
// given sequence number.
func ChainTomlV2FileNameForSeq(seq uint64) string {
	return fmt.Sprintf("%s.%d.toml", ChainTomlV2BaseName, seq)
}

// ParseChainTomlV2FileName extracts the generation seq from a filename
// matching chain.v2.<seq>.toml. ok=false for any other shape.
func ParseChainTomlV2FileName(name string) (seq uint64, ok bool) {
	m := chainTomlV2NameRE.FindStringSubmatch(name)
	if m == nil {
		return 0, false
	}
	n, err := strconv.ParseUint(m[1], 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// RollingV2Publisher writes successive generations of the V2 chain
// manifest into snapDir, each under a numbered filename. Construction
// scans snapDir for existing generations and resumes numbering from
// max+1.
//
// Concurrency: every public method holds the publisher's lock; safe to
// call from multiple goroutines. Publish() is the only mutator; Cleanup
// and inspection helpers are read-mostly.
type RollingV2Publisher struct {
	snapDir     string
	torrentFS   *AtomicTorrentFS
	downloader  *Downloader
	maxRetained int

	mu      sync.Mutex
	history []uint64 // chronological, oldest first; capped at maxRetained
}

// NewRollingV2Publisher constructs a publisher for snapDir. Discovers
// existing chain.v2.<seq>.toml files and seeds the rolling history with
// their seqs (sorted oldest first). maxRetained=0 selects the default.
//
// Returns an error if snapDir can't be read. A snapDir with no existing
// V2 generations is fine — the first Publish() writes seq 0.
func NewRollingV2Publisher(snapDir string, torrentFS *AtomicTorrentFS, dl *Downloader, maxRetained int) (*RollingV2Publisher, error) {
	if maxRetained <= 0 {
		maxRetained = DefaultV2MaxRetained
	}
	if torrentFS == nil {
		return nil, fmt.Errorf("NewRollingV2Publisher: nil torrent fs")
	}

	entries, err := os.ReadDir(snapDir)
	if err != nil {
		return nil, fmt.Errorf("NewRollingV2Publisher: scanning %s: %w", snapDir, err)
	}
	var seqs []uint64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if seq, ok := ParseChainTomlV2FileName(e.Name()); ok {
			seqs = append(seqs, seq)
		}
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })

	return &RollingV2Publisher{
		snapDir:     snapDir,
		torrentFS:   torrentFS,
		downloader:  dl,
		maxRetained: maxRetained,
		history:     seqs,
	}, nil
}

// Publish generates a fresh V2 manifest from inv, writes it as
// chain.v2.<nextSeq>.toml in snapDir, builds its .torrent, registers
// the new generation with the underlying Downloader (if non-nil), and
// trims the oldest retained generation if history is over cap.
// enrUpdater receives the new generation's infohash.
//
// On error, partial state is best-effort cleaned up (file written may
// stay; .torrent may be missing) — the caller should inspect snapDir
// state if precision matters. The returned hash is non-zero on success.
func (r *RollingV2Publisher) Publish(
	ctx context.Context,
	inv *snapshotinv.Inventory,
	authoritativeBlocks uint64,
	enrUpdater func(enr.ChainToml),
) (metainfo.Hash, error) {
	if inv == nil {
		return metainfo.Hash{}, fmt.Errorf("RollingV2Publisher.Publish: nil inventory")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	var nextSeq uint64
	if n := len(r.history); n > 0 {
		nextSeq = r.history[n-1] + 1
	}
	name := ChainTomlV2FileNameForSeq(nextSeq)
	path := filepath.Join(r.snapDir, name)

	manifest := GenerateV2(inv)
	tomlBytes, err := MarshalV2(manifest)
	if err != nil {
		return metainfo.Hash{}, fmt.Errorf("marshal %s: %w", name, err)
	}
	if err := saveChainTomlFile(path, tomlBytes); err != nil {
		return metainfo.Hash{}, fmt.Errorf("save %s: %w", name, err)
	}

	if _, err := BuildTorrentIfNeed(ctx, name, r.snapDir, r.torrentFS); err != nil {
		return metainfo.Hash{}, fmt.Errorf("build %s.torrent: %w", name, err)
	}
	spec, err := r.torrentFS.LoadByName(name + ".torrent")
	if err != nil {
		return metainfo.Hash{}, fmt.Errorf("load %s.torrent: %w", name, err)
	}

	if r.downloader != nil {
		if err := r.downloader.AddNewSeedableFile(ctx, name); err != nil {
			return metainfo.Hash{}, fmt.Errorf("seed %s: %w", name, err)
		}
	}

	if enrUpdater != nil {
		domainSteps, mergeDepth := ComputeENRFields(manifest)
		enrUpdater(enr.ChainToml{
			AuthoritativeBlocks: authoritativeBlocks,
			KnownBlocks:         authoritativeBlocks,
			InfoHash:            spec.InfoHash,
			DomainSteps:         domainSteps,
			MergeDepth:          mergeDepth,
		})
	}

	r.history = append(r.history, nextSeq)
	r.evictOldestLocked()

	return spec.InfoHash, nil
}

// evictOldestLocked drops generations from the front of history until
// len(history) <= maxRetained. Each evicted generation's torrent client
// registration is dropped and its on-disk data + .torrent files are
// removed. Caller must hold r.mu.
func (r *RollingV2Publisher) evictOldestLocked() {
	for len(r.history) > r.maxRetained {
		oldest := r.history[0]
		r.history = r.history[1:]
		oldName := ChainTomlV2FileNameForSeq(oldest)
		if r.downloader != nil {
			r.downloader.DropTorrentByName(oldName)
		}
		_ = dir.RemoveFile(filepath.Join(r.snapDir, oldName))
		_ = dir.RemoveFile(filepath.Join(r.snapDir, oldName+".torrent"))
	}
}

// Cleanup removes any chain.v2.<seq>.toml file (and its .torrent) in
// snapDir whose seq is not in the current history. Useful after a
// crash mid-publish (file written but seq never reached history) or
// after maxRetained is reduced. Idempotent.
//
// The current generations stay registered in the torrent client; only
// orphans are removed.
func (r *RollingV2Publisher) Cleanup() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	keep := make(map[uint64]struct{}, len(r.history))
	for _, s := range r.history {
		keep[s] = struct{}{}
	}

	entries, err := os.ReadDir(r.snapDir)
	if err != nil {
		return fmt.Errorf("RollingV2Publisher.Cleanup: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		seq, ok := ParseChainTomlV2FileName(e.Name())
		if !ok {
			continue
		}
		if _, kept := keep[seq]; kept {
			continue
		}
		// Orphan — drop torrent registration just in case, then
		// remove files. DropTorrentByName is a no-op if not registered.
		if r.downloader != nil {
			r.downloader.DropTorrentByName(e.Name())
		}
		_ = dir.RemoveFile(filepath.Join(r.snapDir, e.Name()))
		_ = dir.RemoveFile(filepath.Join(r.snapDir, e.Name()+".torrent"))
	}
	return nil
}

// LatestSeq returns the most recent generation seq and ok=true. ok is
// false if no generation has ever been published.
func (r *RollingV2Publisher) LatestSeq() (seq uint64, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.history) == 0 {
		return 0, false
	}
	return r.history[len(r.history)-1], true
}

// History returns a copy of the current generation seqs in chronological
// order (oldest first). For tests + diagnostics.
func (r *RollingV2Publisher) History() []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]uint64, len(r.history))
	copy(out, r.history)
	return out
}

// MaxRetained returns the configured rolling-buffer cap.
func (r *RollingV2Publisher) MaxRetained() int {
	return r.maxRetained
}
