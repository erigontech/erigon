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
// new torrent with the underlying Downloader, and evicts any retained
// generation that has gone out of scope.
//
// Validity rule
//
// A retained chain.v2.<seq>.toml is VALID iff every snapshot name it lists
// is present in the publisher's current inventory (equivalently: in the
// most recent generation written). The moment a merge retires a snapshot
// file from inventory, any older generation that listed that name becomes
// INVALID and is removed: the .toml manifest, its Content UCAN, the
// paired Authority UCAN, and their .torrent
// sidecars are deleted from disk and dropped from the torrent client, and
// the orphan snapshot names — names in the evicted generation that are
// not in the current inventory and are therefore no longer seedable —
// are also dropped from the torrent client.
//
// Older generations that remain a subset of the current inventory stay
// seedable, so a peer that captured a stale ENR snapshot can still fetch
// the infohash it handshaked on.
//
// Cleanup() is the defence-in-depth for crashed-mid-publish cases (file
// written but the in-memory seq never advanced).

package downloader

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"github.com/anacrolix/torrent/metainfo"

	"github.com/erigontech/erigon/common/dir"
	snapshotinv "github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/p2p/enr"
)

// ChainTomlV2BaseName is the prefix every V2 manifest filename starts
// with. Generations append `.<genID>.toml` so the .toml extension stays
// terminal for tool recognition (parsers, IDEs, etc).
const ChainTomlV2BaseName = "chain.v2"

// ChainUCANBaseName is the prefix every UCAN sidecar filename starts
// with. Pairs with ChainTomlV2BaseName at the same <genID> so a peer's
// (V2 manifest, UCAN attestation) is one logical generation.
const ChainUCANBaseName = "chain.ucan"

// GenIDLen is the byte length of a generation ID. The ID is derived
// from the artefact's own content (see genIDFromContent), NOT a
// counter — replacing a monotonic <seq> so a passive discv5 scraper
// cannot read a publisher's republish rate from its ENR, while still
// giving two publishers of byte-identical content the same ID. 8 bytes
// renders as 16 lowercase hex chars in filenames.
const GenIDLen = 8

// chainTomlV2NameRE matches chain.v2.<enr-fp>.<genID>.toml — the only
// filename shape RollingV2Publisher emits and recognises. Both
// <enr-fp> and <genID> are 16 lowercase hex chars (see ENRFingerprint,
// genIDFromContent).
var chainTomlV2NameRE = regexp.MustCompile(`^chain\.v2\.([0-9a-f]{16})\.([0-9a-f]{16})\.toml$`)

// chainUCANNameRE matches chain.ucan.<enr-fp>.<genID>.bin — the UCAN
// sidecar shape paired with each V2 generation.
var chainUCANNameRE = regexp.MustCompile(`^chain\.ucan\.([0-9a-f]{16})\.([0-9a-f]{16})\.bin$`)

// ENRFingerprint formats the 16-hex-char node fingerprint used in
// per-node advertisement filenames — the first 8 bytes of the node's
// discv5 ID. The node ID is keccak(pubkey), stable across ENR-record
// updates, so the fingerprint identifies the node, not a record
// version.
func ENRFingerprint(nodeID [32]byte) string {
	return hex.EncodeToString(nodeID[:8])
}

// genIDFromContent derives a generation ID deterministically from an
// artefact's own bytes — the first GenIDLen bytes of their SHA-256,
// lowercase hex. Two publishers of byte-identical content get the same
// ID, hence the same filename and the same torrent info-hash, so an
// honest swarm converges on one artefact per distinct manifest; any
// content change yields a fresh ID.
func genIDFromContent(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:GenIDLen])
}

// ChainTomlV2FileName formats the per-node advertisement filename for
// the given ENR fingerprint and generation ID:
// chain.v2.<enr-fp>.<genID>.toml.
func ChainTomlV2FileName(enrFP, genID string) string {
	return fmt.Sprintf("%s.%s.%s.toml", ChainTomlV2BaseName, enrFP, genID)
}

// ChainUCANFileName formats the UCAN sidecar filename paired with the
// V2 manifest at the same fingerprint + genID.
func ChainUCANFileName(enrFP, genID string) string {
	return fmt.Sprintf("%s.%s.%s.bin", ChainUCANBaseName, enrFP, genID)
}

// ChainV2ContentUCANFileName formats the Content UCAN sidecar filename
// paired with chain.v2.<enr-fp>.<genID>.toml. The Content UCAN is a
// self-issued snapshotauth delegation binding chain.v2:hash:<sha256>
// to the .toml bytes; per
// docs/plans/20260520-chaintoml-ucan-flow-spec.md it replaces the
// interim .sig sidecar. A consumer name-derives it from the publisher
// ENR fingerprint + the genID carried in the peer's ENR chain-toml
// entry.
func ChainV2ContentUCANFileName(enrFP, genID string) string {
	return fmt.Sprintf("%s.%s.%s.ucan", ChainTomlV2BaseName, enrFP, genID)
}

// ParseChainTomlV2FileName extracts the ENR fingerprint and generation
// ID from a filename matching chain.v2.<enr-fp>.<genID>.toml. ok=false
// for any other shape.
func ParseChainTomlV2FileName(name string) (enrFP, genID string, ok bool) {
	m := chainTomlV2NameRE.FindStringSubmatch(name)
	if m == nil {
		return "", "", false
	}
	return m[1], m[2], true
}

// ParseChainUCANFileName extracts the ENR fingerprint and generation
// ID from a filename matching chain.ucan.<enr-fp>.<genID>.bin. ok=false
// for any other shape.
func ParseChainUCANFileName(name string) (enrFP, genID string, ok bool) {
	m := chainUCANNameRE.FindStringSubmatch(name)
	if m == nil {
		return "", "", false
	}
	return m[1], m[2], true
}

// generationEntry caches the genID and the set of snapshot names a
// retained chain.v2.<genID>.toml lists. The name-set drives the
// validity check on each Publish — keeping it in memory avoids
// re-parsing each retained generation off disk every cycle.
type generationEntry struct {
	genID string
	names map[string]struct{}
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
	snapDir    string
	torrentFS  *AtomicTorrentFS
	downloader *Downloader

	mu               sync.Mutex
	history          []generationEntry // chronological, oldest first
	delegationSource DelegationSource

	// enrFP is an explicit ENR-fingerprint override. When empty,
	// resolveENRFP falls back to downloader.SelfENRFingerprint().
	// Production leaves this empty (the Downloader carries the
	// fingerprint); tests / the harness / the cold-start helper set it
	// directly via SetENRFingerprint.
	enrFP string

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

	// contentMinter mints a Content UCAN over the bytes of each
	// chain.v2.<seq>.toml file immediately after they're written. The
	// returned canonical-CBOR delegation is persisted as
	// chain.v2.<enr-fp>.<seq>.ucan in the same directory and registered
	// as a seedable file. Per
	// docs/plans/20260520-chaintoml-ucan-flow-spec.md the Content UCAN
	// replaces the interim .sig sidecar: it binds chain.v2:hash:<sha256>
	// to the .toml bytes so a redistributor cannot modify them without
	// invalidating the attestation.
	//
	// Wired via a callback — production supplies a closure that calls
	// snapshotauth.MintContentUCAN with the node's ENR-matching
	// secp256k1 key. nil means no Content UCAN is written (fine for
	// tests; production wires this via SetContentUCANMinter).
	contentMinter ContentUCANMinterFn
}

// ManifestSelfCheckFn is the type of the producer-side self-check
// callback (see RollingV2Publisher.selfCheck). Receives the just-
// generated manifest; returns an error to abort the publish for
// this generation. The publisher does not write the file, does not
// build the .torrent, does not update the ENR — the failure is
// graceful: this generation is skipped, the node keeps running, the
// next inventory-change-triggered publish will retry.
type ManifestSelfCheckFn func(manifest *ChainTomlV2) error

// ContentUCANMinterFn is the type of the producer-side Content UCAN
// minting callback (see RollingV2Publisher.contentMinter). Receives
// the bytes of the just-written chain.v2.<seq>.toml file; returns the
// canonical-CBOR Content UCAN (from snapshotauth.MintContentUCAN).
// Persistence as the chain.v2.<enr-fp>.<seq>.ucan sidecar and torrent
// registration happen inside Publish — the minter is content-only.
//
// Returning an error aborts the publish for this generation: the
// .toml file is removed (don't seed unattested content), no .ucan is
// written, no .torrent is built, history is not advanced. The publish
// caller logs at Warn and the node continues running, same as for
// self-check failure.
type ContentUCANMinterFn func(tomlBytes []byte) ([]byte, error)

// DelegationSource yields the snapshotauth UCAN attestation bytes (canonical
// CBOR) that should be paired with the next published V2 manifest. Returning
// (nil, nil) means "no delegation this generation" — Publish() writes the V2
// without a UCAN sidecar and leaves AuthorityUCANHash empty. An error aborts Publish.
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

// SetContentUCANMinter configures the producer-side Content UCAN
// minting callback called on every Publish() after the .toml file is
// written. Pass nil to clear (no Content UCAN; default for
// tests/harness). Production callers wire a closure that calls
// snapshotauth.MintContentUCAN with the node's ENR-matching secp256k1
// key.
func (r *RollingV2Publisher) SetContentUCANMinter(fn ContentUCANMinterFn) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.contentMinter = fn
}

// SetENRFingerprint sets an explicit ENR fingerprint, overriding the
// downloader-carried one. Used by the cold-start helper, the harness,
// and tests — production leaves it unset (the Downloader carries it).
func (r *RollingV2Publisher) SetENRFingerprint(fp string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.enrFP = fp
}

// resolveENRFP returns the ENR fingerprint for filename construction:
// the explicit override if set, else the downloader's. Caller must
// hold r.mu. Empty result means the fingerprint is not yet known —
// Publish treats that as a wiring error.
func (r *RollingV2Publisher) resolveENRFP() string {
	if r.enrFP != "" {
		return r.enrFP
	}
	if r.downloader != nil {
		return r.downloader.SelfENRFingerprint()
	}
	return ""
}

// NewRollingV2Publisher constructs a publisher for snapDir. Discovers
// existing chain.v2.<genID>.toml files and parses each to recover its
// name set. A generation whose .toml is unparseable is skipped —
// Cleanup() will sweep the orphan.
//
// Resumed generations have no recoverable order (genID is opaque, not a
// counter) — history is in directory-scan order. That is harmless: the
// first post-resume Publish appends the new canonical generation last,
// and eviction keys on subset-validity, not ordering.
//
// Returns an error if snapDir can't be read. A snapDir with no existing
// V2 generations is fine.
func NewRollingV2Publisher(snapDir string, torrentFS *AtomicTorrentFS, dl *Downloader) (*RollingV2Publisher, error) {
	if torrentFS == nil {
		return nil, fmt.Errorf("NewRollingV2Publisher: nil torrent fs")
	}

	entries, err := os.ReadDir(snapDir)
	if err != nil {
		return nil, fmt.Errorf("NewRollingV2Publisher: scanning %s: %w", snapDir, err)
	}
	var history []generationEntry
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		_, genID, ok := ParseChainTomlV2FileName(e.Name())
		if !ok {
			continue
		}
		names, err := loadManifestNames(filepath.Join(snapDir, e.Name()))
		if err != nil {
			// Unparseable — leave on disk, Cleanup() will sweep.
			continue
		}
		history = append(history, generationEntry{genID: genID, names: names})
	}

	return &RollingV2Publisher{
		snapDir:    snapDir,
		torrentFS:  torrentFS,
		downloader: dl,
		history:    history,
	}, nil
}

// loadManifestNames reads chain.v2.<seq>.toml at path and returns the
// set of snapshot names it references (blocks + meta + salt + domain
// kv/history/idx + caplin). Used during discovery and tests.
func loadManifestNames(path string) (map[string]struct{}, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	manifest, err := ParseV2(data)
	if err != nil {
		return nil, err
	}
	return manifestFileNames(manifest), nil
}

// manifestFileNames collects every snapshot file name a V2 manifest
// references. The result drives the validity check on each Publish: a
// retained generation is valid iff its set ⊆ the current generation's
// set.
//
// chain.v2.*.toml / .ucan + chain.ucan.* sidecars are NOT in the result
// — they are the publisher's own per-generation artefacts, not advertised
// snapshot files. Their lifecycle is handled by evictGenerationLocked.
func manifestFileNames(m *ChainTomlV2) map[string]struct{} {
	out := make(map[string]struct{})
	if m == nil {
		return out
	}
	for name := range m.Blocks {
		out[name] = struct{}{}
	}
	for name := range m.Meta {
		out[name] = struct{}{}
	}
	for name := range m.Salt {
		out[name] = struct{}{}
	}
	for _, dom := range m.Domains {
		if dom == nil {
			continue
		}
		for _, f := range dom.Files {
			out[f.Name] = struct{}{}
		}
	}
	for _, c := range m.Caplin {
		out[c.Name] = struct{}{}
	}
	return out
}

// Publish generates a fresh V2 manifest from inv, writes it as
// chain.v2.<nextSeq>.toml in snapDir, builds its .torrent, registers
// the new generation with the underlying Downloader (if non-nil), and
// trims the oldest retained generation if history is over cap.
// enrUpdater receives the new generation's infohash.
//
// When a DelegationSource is configured, Publish ALSO writes
// chain.ucan.<seq>.bin (the snapshotauth attestation paired with this
// generation) and stamps the V2 manifest's AuthorityUCANHash field with the
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

	enrFP := r.resolveENRFP()
	if enrFP == "" {
		return metainfo.Hash{}, fmt.Errorf("RollingV2Publisher.Publish: ENR fingerprint not set (P2P not up, or SetENRFingerprint not called)")
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

	// The generation ID is derived from this generation's content — the
	// inventory-derived manifest bytes plus the Authority UCAN bytes.
	// Two publishers of byte-identical inventory and UCAN get the same
	// genID (hence the same filenames and manifest torrent info-hash),
	// so an honest swarm converges on one artefact per generation; any
	// inventory or UCAN change yields a fresh one. It is derived from
	// the manifest BEFORE its AuthorityUCANHash is stamped, breaking the
	// circular dependency — AuthorityUCANHash names the UCAN torrent,
	// which is itself named by this genID.
	coreBytes, err := MarshalV2(manifest)
	if err != nil {
		return metainfo.Hash{}, fmt.Errorf("marshal chain.v2 manifest: %w", err)
	}
	var ucanBytes []byte
	if r.delegationSource != nil {
		ucanBytes, err = r.delegationSource()
		if err != nil {
			return metainfo.Hash{}, fmt.Errorf("delegation source: %w", err)
		}
	}
	genID := genIDFromContent(append(append([]byte(nil), coreBytes...), ucanBytes...))
	name := ChainTomlV2FileName(enrFP, genID)
	path := filepath.Join(r.snapDir, name)

	// Write the Authority UCAN sidecar first when one is configured: its
	// torrent info-hash is stamped into the manifest's AuthorityUCANHash,
	// so the final manifest bytes can only be produced once the UCAN
	// torrent exists. The sidecar shares the manifest's genID — they are
	// one logical generation on disk, registered/evicted/cleaned up
	// together.
	var ucanHashHex string
	if len(ucanBytes) > 0 {
		ucanName := ChainUCANFileName(enrFP, genID)
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

	tomlBytes := coreBytes
	if ucanHashHex != "" {
		manifest.AuthorityUCANHash = ucanHashHex
		tomlBytes, err = MarshalV2(manifest)
		if err != nil {
			return metainfo.Hash{}, fmt.Errorf("marshal chain.v2 manifest: %w", err)
		}
	}
	if err := saveChainTomlFile(path, tomlBytes); err != nil {
		return metainfo.Hash{}, fmt.Errorf("save %s: %w", name, err)
	}

	// Producer Content UCAN: mint a Content UCAN over the just-written
	// .toml bytes and persist chain.v2.<enr-fp>.<seq>.ucan alongside.
	// Per docs/plans/20260520-chaintoml-ucan-flow-spec.md the Content
	// UCAN replaces the interim .sig — it binds chain.v2:hash:<sha256>
	// to the .toml so a redistributor cannot modify the bytes without
	// invalidating the attestation. Failure here removes the .toml that
	// was just written (don't seed unattested content) and aborts the
	// publish for this generation; the caller logs at Warn and the node
	// continues running.
	// contentUCANHash is the info-hash of the Content UCAN torrent. It
	// is stamped into the ENR (ChainToml.ContentUCANHash) so a consumer
	// can fetch the Content UCAN by hash to run the verification chain.
	// Stays zero when no content minter is wired.
	var contentUCANHash metainfo.Hash
	if r.contentMinter != nil {
		ucanBytes, err := r.contentMinter(tomlBytes)
		if err != nil {
			_ = dir.RemoveFile(path)
			return metainfo.Hash{}, fmt.Errorf("publisher content UCAN %s: %w", name, err)
		}
		cucanName := ChainV2ContentUCANFileName(enrFP, genID)
		cucanPath := filepath.Join(r.snapDir, cucanName)
		if err := saveChainTomlFile(cucanPath, ucanBytes); err != nil {
			_ = dir.RemoveFile(path)
			return metainfo.Hash{}, fmt.Errorf("save %s: %w", cucanName, err)
		}
		if _, err := BuildTorrentIfNeed(ctx, cucanName, r.snapDir, r.torrentFS); err != nil {
			_ = dir.RemoveFile(path)
			_ = dir.RemoveFile(cucanPath)
			return metainfo.Hash{}, fmt.Errorf("build %s.torrent: %w", cucanName, err)
		}
		cucanSpec, err := r.torrentFS.LoadByName(cucanName + ".torrent")
		if err != nil {
			_ = dir.RemoveFile(path)
			_ = dir.RemoveFile(cucanPath)
			return metainfo.Hash{}, fmt.Errorf("load %s.torrent: %w", cucanName, err)
		}
		contentUCANHash = cucanSpec.InfoHash
		if r.downloader != nil {
			if err := r.downloader.AddNewSeedableFile(ctx, cucanName); err != nil {
				_ = dir.RemoveFile(path)
				_ = dir.RemoveFile(cucanPath)
				return metainfo.Hash{}, fmt.Errorf("seed %s: %w", cucanName, err)
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
			ContentUCANHash:     contentUCANHash,
		})
	}

	canonical := manifestFileNames(manifest)
	// A no-op republish (unchanged inventory) produces the same content
	// genID; don't record a duplicate generation.
	if n := len(r.history); n == 0 || r.history[n-1].genID != genID {
		r.history = append(r.history, generationEntry{genID: genID, names: canonical})
	}
	r.evictInvalidLocked(enrFP, canonical)

	return spec.InfoHash, nil
}

// evictInvalidLocked walks retained generations (excluding the latest,
// which IS canonical) and evicts any whose name-set isn't a subset of
// canonical. The latest is always kept. Caller must hold r.mu.
//
// After per-generation eviction the function unseeds orphan snapshot
// names — names that appeared in an evicted generation but are not in
// canonical and not in any surviving retained generation. The merger
// is expected to delete those files from disk; this step ensures the
// torrent client stops advertising them even if the merger didn't drop
// the torrent registration.
func (r *RollingV2Publisher) evictInvalidLocked(enrFP string, canonical map[string]struct{}) {
	if len(r.history) <= 1 {
		return
	}

	kept := r.history[:0:0]
	var evicted []generationEntry

	for i := 0; i < len(r.history)-1; i++ {
		gen := r.history[i]
		if isSubsetOf(gen.names, canonical) {
			kept = append(kept, gen)
			continue
		}
		evicted = append(evicted, gen)
	}

	if len(evicted) == 0 {
		return
	}

	// Survivors keep their insertion order; the canonical entry (newest)
	// is appended last so r.history's tail stays the most recent Publish.
	kept = append(kept, r.history[len(r.history)-1])
	r.history = kept

	// Build the union of names still referenced by any retained
	// generation. Anything in an evicted gen but NOT in this union is
	// an orphan — unseed it.
	stillReferenced := make(map[string]struct{})
	for _, gen := range r.history {
		for name := range gen.names {
			stillReferenced[name] = struct{}{}
		}
	}

	orphanNames := make(map[string]struct{})
	for _, gen := range evicted {
		r.evictGenerationLocked(enrFP, gen.genID)
		for name := range gen.names {
			if _, kept := stillReferenced[name]; kept {
				continue
			}
			orphanNames[name] = struct{}{}
		}
	}

	if r.downloader != nil {
		for name := range orphanNames {
			r.downloader.DropTorrentByName(name)
		}
	}
}

// isSubsetOf reports whether every key in a is also in b.
func isSubsetOf(a, b map[string]struct{}) bool {
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}

// evictGenerationLocked removes the V2 manifest + its paired Content
// UCAN + the paired Authority UCAN sidecar for a single genID. The
// UCAN files may not exist (delegation source / content minter unset
// for that generation); RemoveFile on a missing path is a silent
// no-op. Caller must hold r.mu.
//
// Only the per-generation artefacts (.toml + .ucan + .bin + their
// .torrent sidecars) are removed here. Unseeding orphan snapshot files
// is done by evictInvalidLocked once the full set of evictions for
// this Publish is known.
func (r *RollingV2Publisher) evictGenerationLocked(enrFP, genID string) {
	tomlName := ChainTomlV2FileName(enrFP, genID)
	authorityUCANName := ChainUCANFileName(enrFP, genID)
	contentUCANName := ChainV2ContentUCANFileName(enrFP, genID)
	if r.downloader != nil {
		r.downloader.DropTorrentByName(tomlName)
		r.downloader.DropTorrentByName(authorityUCANName)
		r.downloader.DropTorrentByName(contentUCANName)
	}
	_ = dir.RemoveFile(filepath.Join(r.snapDir, tomlName))
	_ = dir.RemoveFile(filepath.Join(r.snapDir, tomlName+".torrent"))
	_ = dir.RemoveFile(filepath.Join(r.snapDir, authorityUCANName))
	_ = dir.RemoveFile(filepath.Join(r.snapDir, authorityUCANName+".torrent"))
	_ = dir.RemoveFile(filepath.Join(r.snapDir, contentUCANName))
	_ = dir.RemoveFile(filepath.Join(r.snapDir, contentUCANName+".torrent"))
}

// Cleanup removes any chain.v2.<genID>.toml + chain.ucan.<genID>.bin
// file (and their .torrent sidecars) in snapDir whose genID is not in
// the current history. Useful after a crash mid-publish (file written
// but the genID never reached history). Idempotent.
//
// The current generations stay registered in the torrent client; only
// orphans are removed.
func (r *RollingV2Publisher) Cleanup() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	keep := make(map[string]struct{}, len(r.history))
	for _, gen := range r.history {
		keep[gen.genID] = struct{}{}
	}

	entries, err := os.ReadDir(r.snapDir)
	if err != nil {
		return fmt.Errorf("RollingV2Publisher.Cleanup: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		var genID string
		var ok bool
		if _, genID, ok = ParseChainTomlV2FileName(e.Name()); !ok {
			_, genID, ok = ParseChainUCANFileName(e.Name())
		}
		if !ok {
			continue
		}
		if _, kept := keep[genID]; kept {
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

// LatestGenID returns the most recent generation's genID and ok=true.
// ok is false if no generation has ever been published. "Most recent"
// is the last Publish in this process; after a resume with no Publish
// yet it is whichever generation the directory scan saw last.
func (r *RollingV2Publisher) LatestGenID() (genID string, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.history) == 0 {
		return "", false
	}
	return r.history[len(r.history)-1].genID, true
}

// History returns a copy of the current generation IDs in history
// order (most recent last). For tests + diagnostics.
func (r *RollingV2Publisher) History() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.history))
	for i, gen := range r.history {
		out[i] = gen.genID
	}
	return out
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
