package downloader

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/anacrolix/torrent/metainfo"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/p2p/enr"
)

const ChainTomlFileName = "chain.toml"

// ChainTomlPath returns the path to the chain.toml file in the given snap directory.
func ChainTomlPath(snapDir string) string {
	return filepath.Join(snapDir, ChainTomlFileName)
}

// chainTomlScanSubdirs lists the snap-dir subdirectories whose .torrent
// files belong in chain.toml alongside top-level (block) torrents.
// Mirrors the snapshot.SubdirForName layout — see node/components/
// storage/snapshot/metadata.go. Kept as a local list to avoid a
// downloader→snapshot import (db/downloader is below node/components
// in the layering).
var chainTomlScanSubdirs = []string{"domain", "history", "idx", "accessor"}

// GenerateChainToml scans .torrent files in snapDir AND its state
// subdirs (domain/, history/, idx/, accessor/), extracts their
// info-hashes, and produces a deterministic TOML representation
// (sorted name = "hash" pairs). The output is byte-identical across
// nodes with the same set of .torrent files.
//
// Names for state torrents are emitted with their subdir prefix
// (e.g. "domain/foo.kv") — matching what consumers already expect
// from the preverified-merge path and what the downloader's Seed /
// Delete calls already use.
//
// **Validation gate**: when servable is non-nil, only entries whose
// info-hash is in the set are included. The map should reflect the
// torrent client's actual seedable set (built by the caller from
// `client.Torrents()`). The disk walk gives us NAMES; servable gives
// us "we can actually serve this". Without the gate, chain.toml
// advertises files we have on disk but haven't yet loaded into the
// seeding client — peers connect, ask for pieces, and find nothing.
// The architectural rule per docs is "validate before advertise"; the
// gate is what enforces it at the chain.toml regen seam.
//
// servable=nil disables the gate (legacy behaviour), used by tests
// and pre-V2 callers. Production calls from PublishLocalChainToml
// always pass a non-nil set built from the live torrent client.
func GenerateChainToml(snapDir string, servable map[metainfo.Hash]struct{}) ([]byte, error) {
	type entry struct {
		name string
		hash string
	}
	var items []entry

	collect := func(dirPath, namePrefix string) error {
		entries, err := os.ReadDir(dirPath)
		if err != nil {
			if os.IsNotExist(err) {
				return nil // missing subdir is normal on a fresh datadir
			}
			return err
		}
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".torrent") {
				continue
			}
			// Exclude chain.toml's own torrent from the manifest —
			// otherwise the manifest becomes self-referential.
			if namePrefix == "" && e.Name() == ChainTomlFileName+".torrent" {
				continue
			}
			fPath := filepath.Join(dirPath, e.Name())
			mi, err := metainfo.LoadFromFile(fPath)
			if err != nil {
				continue // skip unreadable torrent files
			}
			infoHash := mi.HashInfoBytes()
			// Validation gate: skip entries the torrent client isn't
			// ready to serve yet. See doc on this function.
			if servable != nil {
				if _, ok := servable[infoHash]; !ok {
					continue
				}
			}
			dataName := strings.TrimSuffix(e.Name(), ".torrent")
			if namePrefix != "" {
				dataName = namePrefix + "/" + dataName
			}
			items = append(items, entry{
				name: dataName,
				hash: hex.EncodeToString(infoHash[:]),
			})
		}
		return nil
	}

	if err := collect(snapDir, ""); err != nil {
		return nil, fmt.Errorf("reading snap dir: %w", err)
	}
	for _, sub := range chainTomlScanSubdirs {
		if err := collect(filepath.Join(snapDir, sub), sub); err != nil {
			return nil, fmt.Errorf("reading %s: %w", sub, err)
		}
	}

	// Sort by name for deterministic output.
	sort.Slice(items, func(i, j int) bool {
		return items[i].name < items[j].name
	})

	// Build TOML output manually for exact control over formatting and determinism.
	var buf strings.Builder
	for _, item := range items {
		fmt.Fprintf(&buf, "%q = %q\n", item.name, item.hash)
	}

	return []byte(buf.String()), nil
}

// SaveChainToml atomically writes chain.toml to the snap directory.
// It can be called repeatedly as snapshots advance.
// The file is chmod'd before writing because the torrent client may have
// created it as read-only during download.
func SaveChainToml(snapDir string, tomlBytes []byte) error {
	return saveChainTomlFile(ChainTomlPath(snapDir), tomlBytes)
}

// saveChainTomlFile is the shared implementation of SaveChainToml and
// SaveChainTomlV2. The chmod avoids a write failure if the torrent client
// previously created the file read-only during a peer download.
func saveChainTomlFile(path string, tomlBytes []byte) error {
	_ = os.Chmod(path, 0o644)
	return dir.WriteFileWithFsync(path, tomlBytes, 0o644)
}

// LoadChainToml reads the local chain.toml file if it exists.
// Returns nil, nil if the file does not exist.
func LoadChainToml(snapDir string) ([]byte, error) {
	path := ChainTomlPath(snapDir)
	exists, err := dir.FileExist(path)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	return os.ReadFile(path)
}

// BuildChainTomlTorrent creates (or recreates) the .torrent file for chain.toml.
// If chain.toml has changed since the last torrent was built, the old .torrent is
// removed and a new one is created.
// Returns the info-hash of the chain.toml torrent.
func BuildChainTomlTorrent(snapDir string, torrentFS *AtomicTorrentFS) (metainfo.Hash, error) {
	return buildChainTomlTorrentByName(ChainTomlFileName, snapDir, torrentFS)
}

// buildChainTomlTorrentByName is the shared implementation of
// BuildChainTomlTorrent (V1) and BuildChainTomlV2Torrent. It always
// deletes the old .torrent first because the manifest content changes on
// every regenerate — reusing a stale torrent would advertise the wrong
// hash.
func buildChainTomlTorrentByName(fileName, snapDir string, torrentFS *AtomicTorrentFS) (metainfo.Hash, error) {
	_ = torrentFS.Delete(fileName)

	ok, err := BuildTorrentIfNeed(context.Background(), fileName, snapDir, torrentFS)
	if err != nil {
		return metainfo.Hash{}, fmt.Errorf("building %s torrent: %w", fileName, err)
	}
	if !ok {
		return metainfo.Hash{}, fmt.Errorf("%s torrent was not created", fileName)
	}

	spec, err := torrentFS.LoadByName(fileName + ".torrent")
	if err != nil {
		return metainfo.Hash{}, fmt.Errorf("loading %s torrent: %w", fileName, err)
	}
	return spec.InfoHash, nil
}

// PublishChainToml orchestrates the full chain.toml publish flow:
// 1. Generate chain.toml from local .torrent files + preverified registry
// 2. Save chain.toml to disk
// 3. Build chain.toml .torrent file
// 4. Call ENR updater with the new info-hash
//
// enrUpdater may be nil if P2P is not yet available.
// chainName is used to compute AuthoritativeBlocks from the snapcfg
// registry.
//
// bootstrapFromPreverified controls whether preverified entries are
// merged INTO the published chain.toml. When true (bootstrap publishers),
// the published manifest = local files ∪ preverified — propagating the
// chain rollout's seed to peers. When false (regular V2 nodes), the
// published manifest = local files only — preverified is invisible to
// the network. See completion plan §5b.
func PublishChainToml(snapDir string, torrentFS *AtomicTorrentFS, chainName string, bootstrapFromPreverified bool, servable map[metainfo.Hash]struct{}, enrUpdater func(enr.ChainToml)) error {
	// Start from local .torrent files. servable filters to only those
	// the torrent client is ready to seed (validation gate).
	tomlBytes, err := GenerateChainToml(snapDir, servable)
	if err != nil {
		return fmt.Errorf("generating chain.toml: %w", err)
	}

	// Read AuthoritativeBlocks from the registry regardless — it's chain
	// metadata, not preverified file content. Optionally merge in
	// preverified entries when bootstrapping.
	var authoritativeTx uint64
	if chainName != "" {
		if cfg, known := snapcfg.KnownCfg(chainName); known {
			authoritativeTx = cfg.ExpectBlocks
			if bootstrapFromPreverified {
				localMap, _ := ParseChainToml(tomlBytes)
				preverified := make(map[string]string, len(cfg.Preverified.Items))
				for _, item := range cfg.Preverified.Items {
					preverified[item.Name] = item.Hash
				}
				merged, _ := MergeChainToml(localMap, preverified)
				tomlBytes = BuildTomlFromMap(merged)
			}
		}
	}

	if err := SaveChainToml(snapDir, tomlBytes); err != nil {
		return fmt.Errorf("saving chain.toml: %w", err)
	}

	infoHash, err := BuildChainTomlTorrent(snapDir, torrentFS)
	if err != nil {
		return fmt.Errorf("building chain.toml torrent: %w", err)
	}

	if enrUpdater != nil {
		// For the initial implementation, AuthoritativeBlocks == KnownBlocks.
		// When a node re-publishes peer-discovered entries, KnownBlocks will
		// be derived from the peer's ENR values and may exceed AuthoritativeBlocks.
		enrUpdater(enr.ChainToml{
			AuthoritativeBlocks: authoritativeTx,
			KnownBlocks:         authoritativeTx,
			InfoHash:            infoHash,
		})
	}

	return nil
}
