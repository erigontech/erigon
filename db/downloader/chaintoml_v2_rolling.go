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
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
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

// ChainUCANBaseName is the prefix every UCAN sidecar filename starts
// with. Pairs with ChainTomlV2BaseName at the same <seq> so a peer's
// (V2 manifest, UCAN attestation) is one logical generation.
const ChainUCANBaseName = "chain.ucan"

// chainTomlV2NameRE matches chain.v2.<seq>.toml — the only filename
// shape RollingV2Publisher emits and recognises.
var chainTomlV2NameRE = regexp.MustCompile(`^chain\.v2\.(\d+)\.toml$`)

// chainUCANNameRE matches chain.ucan.<seq>.bin — the UCAN sidecar
// shape paired with each V2 generation.
var chainUCANNameRE = regexp.MustCompile(`^chain\.ucan\.(\d+)\.bin$`)

// ChainTomlV2FileNameForSeq formats a generational filename for the
// given sequence number.
func ChainTomlV2FileNameForSeq(seq uint64) string {
	return fmt.Sprintf("%s.%d.toml", ChainTomlV2BaseName, seq)
}

// ChainUCANFileNameForSeq formats the UCAN sidecar filename paired
// with the V2 manifest at the same seq.
func ChainUCANFileNameForSeq(seq uint64) string {
	return fmt.Sprintf("%s.%d.bin", ChainUCANBaseName, seq)
}

// ChainV2SigFileNameForSeq formats the signature sidecar filename
// paired with chain.v2.<seq>.toml. The signature is over the bytes
// of the .toml file. Receivers fetch both files, verify the
// signature against the peer's ENR public key, then process the
// manifest. Per
// docs/plans/20260515-three-layer-snapshot-distribution.md the
// signature is the MITM-publisher defence — any redistributor that
// modifies the .toml content invalidates the .sig.
func ChainV2SigFileNameForSeq(seq uint64) string {
	return fmt.Sprintf("%s.%d.sig", ChainTomlV2BaseName, seq)
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

// ParseChainUCANFileName extracts the generation seq from a filename
// matching chain.ucan.<seq>.bin. ok=false for any other shape.
func ParseChainUCANFileName(name string) (seq uint64, ok bool) {
	m := chainUCANNameRE.FindStringSubmatch(name)
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

	mu               sync.Mutex
	history          []uint64 // chronological, oldest first; capped at maxRetained
	delegationSource DelegationSource

	// selfCheck is invoked on every Publish() after the manifest has
	// been generated but before any file is written. It receives the
	// just-generated ChainTomlV2 and returns an error if the manifest
	// fails the producer-side self-check against canonical
	// (per docs/plans/20260515-three-layer-snapshot-distribution.md).
	//
	// Wired via a callback rather than a direct snapshotsync dependency
	// to avoid an import cycle (db/snapshotsync already imports
	// db/downloader). Production wires this to a closure that flattens
	// ChainTomlV2 to PreverifiedItems and calls
	// snapshotsync.CheckOwnAdvertisement; tests/harness leave it nil.
	//
	// nil means the check is skipped — fine for tests, but production
	// MUST wire it via SetSelfCheck. The downloader's
	// PublishLocalChainTomlV2 caller logs a Warn on a non-nil return,
	// stops the publish for that generation, and lets the node continue
	// running (per direction: "node stops publishing, can still run").
	selfCheck ManifestSelfCheckFn

	// signer signs the bytes of each chain.v2.<seq>.toml file
	// immediately after they're written. The returned signature is
	// persisted as chain.v2.<seq>.sig in the same directory and
	// registered as a seedable file. Per
	// docs/plans/20260515-three-layer-snapshot-distribution.md, the
	// signature is the MITM-publisher defence: redistributor peers can
	// re-serve the .toml but cannot modify its bytes without
	// invalidating the .sig.
	//
	// Wired via a callback — production supplies a closure that calls
	// snapshotsync.SignAdvertisement with the node's ENR-matching
	// secp256k1 private key. nil means signing is skipped (fine for
	// tests; production wires this via SetSigner).
	signer ManifestSignerFn
}

// ManifestSelfCheckFn is the type of the producer-side self-check
// callback (see RollingV2Publisher.selfCheck). Receives the just-
// generated manifest; returns an error to abort the publish for
// this generation. The publisher does not write the file, does not
// build the .torrent, does not update the ENR — the failure is
// graceful: this generation is skipped, the node keeps running, the
// next inventory-change-triggered publish will retry.
type ManifestSelfCheckFn func(manifest *ChainTomlV2) error

// ManifestSignerFn is the type of the producer-side signing
// callback (see RollingV2Publisher.signer). Receives the bytes of
// the just-written chain.v2.<seq>.toml file; returns the
// signature bytes (64 bytes [R||S] from
// snapshotsync.SignAdvertisement). Persistence as
// chain.v2.<seq>.sig sidecar and torrent registration happen
// inside Publish — the signer is content-only.
//
// Returning an error aborts the publish for this generation: the
// .toml file is removed (don't seed unsigned content), no .sig is
// written, no .torrent is built, history is not advanced. The
// publish caller logs at Warn and the node continues running, same
// as for self-check failure.
type ManifestSignerFn func(data []byte) ([]byte, error)

// DelegationSource yields the snapshotauth UCAN attestation bytes (canonical
// CBOR) that should be paired with the next published V2 manifest. Returning
// (nil, nil) means "no delegation this generation" — Publish() writes the V2
// without a UCAN sidecar and leaves UCANHash empty. An error aborts Publish.
//
// The source is consulted on every Publish() call so operators can rotate
// delegations without restarting the publisher (e.g. when the operator's
// signing key gets rolled).
type DelegationSource func() ([]byte, error)

// SetDelegationSource configures the publisher to write a paired
// chain.ucan.<seq>.bin alongside each chain.v2.<seq>.toml. Pass nil to
// clear (V2-only publication).
func (r *RollingV2Publisher) SetDelegationSource(src DelegationSource) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.delegationSource = src
}

// SetSelfCheck configures the producer-side fail-loud check called
// on every Publish() before any file is written. Pass nil to clear
// (no check; default for tests/harness). Production callers wire a
// closure that flattens the manifest and runs
// snapshotsync.CheckOwnAdvertisement against the current canonical
// set.
func (r *RollingV2Publisher) SetSelfCheck(fn ManifestSelfCheckFn) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.selfCheck = fn
}

// SetSigner configures the producer-side signing callback called
// on every Publish() after the .toml file is written. Pass nil to
// clear (no signing; default for tests/harness). Production callers
// wire a closure that calls snapshotsync.SignAdvertisement with the
// node's ENR-matching secp256k1 private key.
func (r *RollingV2Publisher) SetSigner(fn ManifestSignerFn) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.signer = fn
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
// When a DelegationSource is configured, Publish ALSO writes
// chain.ucan.<seq>.bin (the snapshotauth attestation paired with this
// generation) and stamps the V2 manifest's UCANHash field with the
// UCAN torrent's infohash so consumers can fetch the sidecar by that
// hash. The pair is registered, evicted, and cleaned up together —
// they are one logical generation on disk.
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

	// Write the UCAN sidecar first if a delegation source is wired.
	// Its infohash is embedded in the V2 manifest, so the V2 must be
	// generated AFTER the UCAN torrent exists.
	var ucanHashHex string
	if r.delegationSource != nil {
		ucanBytes, err := r.delegationSource()
		if err != nil {
			return metainfo.Hash{}, fmt.Errorf("delegation source: %w", err)
		}
		if len(ucanBytes) > 0 {
			ucanName := ChainUCANFileNameForSeq(nextSeq)
			ucanPath := filepath.Join(r.snapDir, ucanName)
			if err := saveChainTomlFile(ucanPath, ucanBytes); err != nil {
				return metainfo.Hash{}, fmt.Errorf("save %s: %w", ucanName, err)
			}
			if _, err := BuildTorrentIfNeed(ctx, ucanName, r.snapDir, r.torrentFS); err != nil {
				return metainfo.Hash{}, fmt.Errorf("build %s.torrent: %w", ucanName, err)
			}
			ucanSpec, err := r.torrentFS.LoadByName(ucanName + ".torrent")
			if err != nil {
				return metainfo.Hash{}, fmt.Errorf("load %s.torrent: %w", ucanName, err)
			}
			if r.downloader != nil {
				if err := r.downloader.AddNewSeedableFile(ctx, ucanName); err != nil {
					return metainfo.Hash{}, fmt.Errorf("seed %s: %w", ucanName, err)
				}
			}
			ucanHashHex = hex.EncodeToString(ucanSpec.InfoHash[:])
		}
	}

	// The storage component doesn't yet feed torrent hashes back into
	// the inventory after Seed (no downloader→storage callback wired),
	// so a publisher whose files are all on disk still has inventory
	// entries with zero TorrentHash — and GenerateV2 skips those,
	// emitting an effectively empty manifest. Stamp hashes from the
	// .torrent files on disk first so the published V2 manifest
	// actually lists the node's files.
	populateInventoryTorrentHashes(inv, r.snapDir)

	manifest := GenerateV2(inv)
	manifest.UCANHash = ucanHashHex

	// Producer self-check: fail loud if this manifest disagrees with
	// canonical for any known-canonical name. The check happens BEFORE
	// any file is written / built / seeded / advertised — on failure
	// nothing about this generation makes it onto disk, into the
	// torrent client, or into the ENR. Per direction
	// (docs/plans/20260515-three-layer-snapshot-distribution.md): the
	// node stops publishing this generation but keeps running; the
	// caller logs at Warn and the next inventory-change-triggered
	// Publish retries (in case the underlying retire bug is transient).
	if r.selfCheck != nil {
		if err := r.selfCheck(manifest); err != nil {
			return metainfo.Hash{}, fmt.Errorf("publisher self-check: %w", err)
		}
	}

	tomlBytes, err := MarshalV2(manifest)
	if err != nil {
		return metainfo.Hash{}, fmt.Errorf("marshal %s: %w", name, err)
	}
	if err := saveChainTomlFile(path, tomlBytes); err != nil {
		return metainfo.Hash{}, fmt.Errorf("save %s: %w", name, err)
	}

	// Producer signing: sign the just-written .toml bytes and persist
	// chain.v2.<seq>.sig alongside. Per
	// docs/plans/20260515-three-layer-snapshot-distribution.md the
	// signature is the MITM-publisher defence — redistributors can
	// re-serve the .toml but cannot modify its bytes without
	// invalidating the .sig. Failure here removes the .toml that was
	// just written (don't seed unsigned content) and aborts the
	// publish for this generation; the caller logs at Warn and the
	// node continues running.
	if r.signer != nil {
		sigBytes, err := r.signer(tomlBytes)
		if err != nil {
			_ = os.Remove(path)
			return metainfo.Hash{}, fmt.Errorf("publisher signing %s: %w", name, err)
		}
		sigName := ChainV2SigFileNameForSeq(nextSeq)
		sigPath := filepath.Join(r.snapDir, sigName)
		if err := saveChainTomlFile(sigPath, sigBytes); err != nil {
			_ = os.Remove(path)
			return metainfo.Hash{}, fmt.Errorf("save %s: %w", sigName, err)
		}
		if _, err := BuildTorrentIfNeed(ctx, sigName, r.snapDir, r.torrentFS); err != nil {
			_ = os.Remove(path)
			_ = os.Remove(sigPath)
			return metainfo.Hash{}, fmt.Errorf("build %s.torrent: %w", sigName, err)
		}
		if r.downloader != nil {
			if err := r.downloader.AddNewSeedableFile(ctx, sigName); err != nil {
				_ = os.Remove(path)
				_ = os.Remove(sigPath)
				return metainfo.Hash{}, fmt.Errorf("seed %s: %w", sigName, err)
			}
		}
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
// removed — for both the V2 manifest and the paired UCAN sidecar (if
// one was written). Caller must hold r.mu.
func (r *RollingV2Publisher) evictOldestLocked() {
	for len(r.history) > r.maxRetained {
		oldest := r.history[0]
		r.history = r.history[1:]
		r.evictGenerationLocked(oldest)
	}
}

// evictGenerationLocked removes the V2 + paired UCAN + paired
// signature artefacts for a single seq. UCAN/.sig files may not
// exist (delegation source / signer unset for that generation);
// RemoveFile on a missing path is a silent no-op. Caller must hold
// r.mu.
func (r *RollingV2Publisher) evictGenerationLocked(seq uint64) {
	tomlName := ChainTomlV2FileNameForSeq(seq)
	ucanName := ChainUCANFileNameForSeq(seq)
	sigName := ChainV2SigFileNameForSeq(seq)
	if r.downloader != nil {
		r.downloader.DropTorrentByName(tomlName)
		r.downloader.DropTorrentByName(ucanName)
		r.downloader.DropTorrentByName(sigName)
	}
	_ = dir.RemoveFile(filepath.Join(r.snapDir, tomlName))
	_ = dir.RemoveFile(filepath.Join(r.snapDir, tomlName+".torrent"))
	_ = dir.RemoveFile(filepath.Join(r.snapDir, ucanName))
	_ = dir.RemoveFile(filepath.Join(r.snapDir, ucanName+".torrent"))
	_ = dir.RemoveFile(filepath.Join(r.snapDir, sigName))
	_ = dir.RemoveFile(filepath.Join(r.snapDir, sigName+".torrent"))
}

// Cleanup removes any chain.v2.<seq>.toml + chain.ucan.<seq>.bin file
// (and their .torrent sidecars) in snapDir whose seq is not in the
// current history. Useful after a crash mid-publish (file written but
// seq never reached history) or after maxRetained is reduced.
// Idempotent.
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
		var seq uint64
		var ok bool
		if seq, ok = ParseChainTomlV2FileName(e.Name()); !ok {
			seq, ok = ParseChainUCANFileName(e.Name())
		}
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

// populateInventoryTorrentHashes scans snapDir (and the known state
// subdirs) for .torrent files and stamps their infohashes onto matching
// inventory entries. It's the stop-gap until the downloader feeds hashes
// back into the inventory after Seed: without it, a node whose snapshot
// files are all on disk still has zero-hash inventory entries, and
// GenerateV2 skips those — yielding an essentially empty V2 manifest.
//
// SetTorrentHash is a no-op for names with no matching entry, so scanning
// every .torrent file (including chain.v2.*.toml.torrent) is safe.
func populateInventoryTorrentHashes(inv *snapshotinv.Inventory, snapDir string) {
	if inv == nil || snapDir == "" {
		return
	}
	// stamp tries the basename (matches inventory entries that store the
	// bare basename, the form discoverNewFiles produces for state
	// domains) and, when scanning a subdir, also the subdir-prefixed
	// form (matches entries like caplin's "caplin/foo.seg" that include
	// the subdir in their Name). SetTorrentHash returns on first match,
	// so the second call is a no-op when the first matched.
	stamp := func(d, sub string) {
		entries, err := os.ReadDir(d)
		if err != nil {
			return
		}
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".torrent") {
				continue
			}
			mi, err := metainfo.LoadFromFile(filepath.Join(d, e.Name()))
			if err != nil {
				continue
			}
			base := strings.TrimSuffix(e.Name(), ".torrent")
			hash := [20]byte(mi.HashInfoBytes())
			snapshotinv.SetTorrentHash(inv, base, hash)
			if sub != "" {
				snapshotinv.SetTorrentHash(inv, sub+"/"+base, hash)
			}
		}
	}
	stamp(snapDir, "")
	for _, sub := range chainTomlScanSubdirs {
		stamp(filepath.Join(snapDir, sub), sub)
	}
	// Caplin lives in caplin/ and its inventory entries carry the
	// "caplin/" prefix in Name — NOT in chainTomlScanSubdirs (that's V1's
	// state-only scan list); scan it explicitly so caplin hashes land.
	stamp(filepath.Join(snapDir, "caplin"), "caplin")
}
