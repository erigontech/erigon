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

// GenerateChainToml scans all .torrent files in snapDir, extracts their info-hashes,
// and produces a deterministic TOML representation (sorted name = "hash" pairs).
// The output is byte-identical across nodes with the same set of .torrent files.
func GenerateChainToml(snapDir string) ([]byte, error) {
	torrentDir := snapDir
	entries, err := os.ReadDir(torrentDir)
	if err != nil {
		return nil, fmt.Errorf("reading snap dir: %w", err)
	}

	type entry struct {
		name string
		hash string
	}
	var items []entry

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".torrent") {
			continue
		}

		// Exclude chain.toml's own torrent from the manifest — otherwise the
		// manifest becomes self-referential: regenerating chain.toml changes
		// the torrent hash, which changes chain.toml, which changes the hash…
		// Consumers should discover chain.toml's info-hash via the ENR entry,
		// not through the manifest itself.
		if e.Name() == ChainTomlFileName+".torrent" {
			continue
		}

		fPath := filepath.Join(torrentDir, e.Name())
		mi, err := metainfo.LoadFromFile(fPath)
		if err != nil {
			continue // skip unreadable torrent files
		}

		// The data file name is the torrent file name without .torrent suffix.
		dataName := strings.TrimSuffix(e.Name(), ".torrent")
		infoHash := mi.HashInfoBytes()
		items = append(items, entry{
			name: dataName,
			hash: hex.EncodeToString(infoHash[:]),
		})
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
	path := ChainTomlPath(snapDir)
	// Make writable if already exists (torrent client creates files read-only).
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
	// Remove existing chain.toml torrent if present — the content has changed.
	_ = torrentFS.Delete(ChainTomlFileName)

	// Build new torrent from the chain.toml file.
	ok, err := BuildTorrentIfNeed(context.Background(), ChainTomlFileName, snapDir, torrentFS)
	if err != nil {
		return metainfo.Hash{}, fmt.Errorf("building chain.toml torrent: %w", err)
	}
	if !ok {
		// Should not happen since we deleted the old one, but handle gracefully.
		return metainfo.Hash{}, fmt.Errorf("chain.toml torrent was not created")
	}

	// Load the torrent spec to get the info-hash.
	spec, err := torrentFS.LoadByName(ChainTomlFileName + ".torrent")
	if err != nil {
		return metainfo.Hash{}, fmt.Errorf("loading chain.toml torrent: %w", err)
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
// chainName is used to include preverified entries from the snapcfg registry.
func PublishChainToml(snapDir string, torrentFS *AtomicTorrentFS, chainName string, enrUpdater func(enr.ChainToml)) error {
	// Start from local .torrent files.
	tomlBytes, err := GenerateChainToml(snapDir)
	if err != nil {
		return fmt.Errorf("generating chain.toml: %w", err)
	}

	// Merge in preverified entries from the registry (covers files without .torrent files).
	// Also compute AuthoritativeBlocks from the registry's ExpectBlocks.
	var authoritativeTx uint64
	if chainName != "" {
		localMap, _ := ParseChainToml(tomlBytes)
		if cfg, known := snapcfg.KnownCfg(chainName); known {
			authoritativeTx = cfg.ExpectBlocks
			preverified := make(map[string]string, len(cfg.Preverified.Items))
			for _, item := range cfg.Preverified.Items {
				preverified[item.Name] = item.Hash
			}
			merged, _ := MergeChainToml(localMap, preverified)
			tomlBytes = BuildTomlFromMap(merged)
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
