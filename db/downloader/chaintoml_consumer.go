package downloader

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"maps"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
	"github.com/c2h5oh/datasize"
	"github.com/pelletier/go-toml/v2"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/p2p/enr"
)

// ParseChainToml parses chain.toml TOML bytes into a name -> hash map.
func ParseChainToml(tomlBytes []byte) (map[string]string, error) {
	var m map[string]string
	if err := toml.Unmarshal(tomlBytes, &m); err != nil {
		return nil, fmt.Errorf("parsing chain.toml: %w", err)
	}
	if m == nil {
		m = make(map[string]string)
	}
	return m, nil
}

// BuildTomlFromMap produces deterministic TOML bytes from a name -> hash map.
// Output is sorted by key, matching the format of GenerateChainToml.
func BuildTomlFromMap(m map[string]string) []byte {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var buf strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&buf, "%q = %q\n", k, m[k])
	}
	return []byte(buf.String())
}

// MergeChainToml merges discovered entries into existing. Existing entries win on
// conflict (same key). Returns the merged map and the count of genuinely new entries.
func MergeChainToml(existing, discovered map[string]string) (merged map[string]string, newCount int) {
	merged = make(map[string]string, len(existing)+len(discovered))
	maps.Copy(merged, existing)
	for k, v := range discovered {
		if _, exists := merged[k]; !exists {
			merged[k] = v
			newCount++
		}
	}
	return merged, newCount
}

// CompareChainToml compares local and discovered chain.toml maps.
// Returns counts of matching, new (in discovered but not local), and mismatched entries.
// mismatches contains filenames where the hash differs.
func CompareChainToml(local, discovered map[string]string) (matching, newEntries int, mismatches []string) {
	for k, dHash := range discovered {
		lHash, exists := local[k]
		if !exists {
			newEntries++
			continue
		}
		if lHash == dHash {
			matching++
		} else {
			mismatches = append(mismatches, k)
		}
	}
	sort.Strings(mismatches)
	return
}

const maxChainTomlSize = int64(16 * datasize.MB)

type cachedChainToml struct {
	infoHash metainfo.Hash
	data     []byte
}

type chainTomlStorage struct {
	storage.ClientImplCloser
	rejected chan error
}

// The library opens storage before closing GotInfo, so validate before opening any files.
func (s chainTomlStorage) OpenTorrent(ctx context.Context, info *metainfo.Info, hash metainfo.Hash) (storage.TorrentImpl, error) {
	if err := validateChainTomlInfo(info); err != nil {
		// Rejected metadata leaves GotInfo open, so notify the downloader directly.
		select {
		case s.rejected <- err:
		default:
		}
		return storage.TorrentImpl{}, err
	}
	return s.ClientImplCloser.OpenTorrent(ctx, info, hash)
}

func validateChainTomlInfo(info *metainfo.Info) error {
	if info == nil {
		return fmt.Errorf("chain.toml torrent info missing")
	}
	// Manifests use v1; v2 file lengths live in the file tree instead of info.Length.
	if info.HasV2() {
		return fmt.Errorf("chain.toml torrent uses v2 metadata, expected v1")
	}
	if len(info.Files) != 0 {
		return fmt.Errorf("chain.toml torrent is multi-file (%d files), expected single file", len(info.Files))
	}
	if name := info.BestName(); name != ChainTomlFileName {
		return fmt.Errorf("chain.toml torrent names %q, expected %q", name, ChainTomlFileName)
	}
	if info.Length < 0 {
		return fmt.Errorf("invalid chain.toml torrent length: %d", info.Length)
	}
	if info.Length > maxChainTomlSize {
		return fmt.Errorf("chain.toml exceeds size limit of %d bytes", maxChainTomlSize)
	}
	// GeneratePieces loops indefinitely for a zero or negative piece length.
	if info.PieceLength <= 0 {
		return fmt.Errorf("invalid chain.toml torrent piece length: %d", info.PieceLength)
	}
	return nil
}

func (d *Downloader) downloadChainToml(ctx context.Context, peer *ChainTomlPeer) ([]byte, error) {
	d.chainTomlTorrentLock.Lock()
	defer d.chainTomlTorrentLock.Unlock()
	infoHash := peer.ChainToml.InfoHash
	if cached := d.chainTomlCache; cached != nil && cached.infoHash == infoHash {
		return cached.data, nil
	}
	data, err := DownloadChainTomlByInfoHash(ctx, d.torrentClient, infoHash, d.snapDir(), d.cfg.Dirs.Tmp, peer)
	if err != nil {
		return nil, err
	}
	if _, err := ParseChainToml(data); err != nil {
		return nil, err
	}
	d.chainTomlCache = &cachedChainToml{infoHash: infoHash, data: data}
	return data, nil
}

// DownloadChainTomlByInfoHash downloads chain.toml into temporary storage or verifies a completed local copy.
// It drops only torrents it creates and rejects incomplete existing torrents.
// The caller must clear tmpDir on startup to remove downloads left by a crash.
// A non-nil peer is used only for new torrents, if its ENR advertises a BitTorrent endpoint.
func DownloadChainTomlByInfoHash(ctx context.Context, client *torrent.Client, infoHash metainfo.Hash, snapDir, tmpDir string, peer *ChainTomlPeer) ([]byte, error) {
	downloadDir, err := os.MkdirTemp(tmpDir, ".chaintoml-download-")
	if err != nil {
		return nil, fmt.Errorf("creating chain.toml download directory: %w", err)
	}
	defer func() {
		if cleanupErr := dir.RemoveAll(downloadDir); cleanupErr != nil {
			log.Warn("[chaintoml] failed to remove download directory", "path", downloadDir, "err", cleanupErr)
		}
	}()
	store := chainTomlStorage{
		ClientImplCloser: storage.NewFileOpts(storage.NewFileClientOpts{ClientBaseDir: downloadDir}),
		rejected:         make(chan error, 1),
	}
	defer store.Close()

	t, added := client.AddTorrentOpt(torrent.AddTorrentOpts{InfoHash: infoHash, Storage: store})
	if added {
		defer t.Drop()
	} else if !t.Complete().Bool() {
		// AddTorrentOpt keeps the original storage, so we cannot safely resume an existing torrent.
		return nil, fmt.Errorf("requested torrent already exists and is incomplete")
	}

	if added && peer != nil {
		addTorrentPeerFromENR(t, peer)
	}

	// Bound network waits so an unresponsive peer does not stall discovery.
	dlCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	select {
	case <-t.GotInfo():
	case metadataErr := <-store.rejected:
		return nil, metadataErr
	case <-dlCtx.Done():
		return nil, fmt.Errorf("waiting for chain.toml torrent info: %w", dlCtx.Err())
	}

	info := t.Info()
	if metadataErr := validateChainTomlInfo(info); metadataErr != nil {
		return nil, metadataErr
	}
	metadata := t.Metainfo()
	if metadata.HashInfoBytes() != infoHash {
		return nil, fmt.Errorf("chain.toml metadata does not match advertised info hash")
	}

	filePath := ChainTomlPath(snapDir)
	if added {
		filePath = ChainTomlPath(downloadDir)
		t.DownloadAll()
		select {
		case <-t.Complete().On():
		case <-dlCtx.Done():
			return nil, fmt.Errorf("downloading chain.toml torrent: %w", dlCtx.Err())
		}
	}

	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("opening downloaded chain.toml: %w", err)
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, info.Length+1))
	if err != nil {
		return nil, fmt.Errorf("reading downloaded chain.toml: %w", err)
	}
	if int64(len(data)) != info.Length {
		return nil, fmt.Errorf("chain.toml does not match torrent length")
	}
	if !added {
		// The local file may have changed since the torrent was marked complete.
		// Verify the bytes we return, since a separate file read could see different contents.
		pieces, hashErr := metainfo.GeneratePieces(bytes.NewReader(data), info.PieceLength, nil)
		if hashErr != nil {
			return nil, fmt.Errorf("hashing local chain.toml: %w", hashErr)
		}
		if !bytes.Equal(pieces, info.Pieces) {
			return nil, fmt.Errorf("chain.toml does not match torrent pieces")
		}
	}

	return data, nil
}

// addTorrentPeerFromENR adds the peer using the IP and BitTorrent port advertised in its ENR.
func addTorrentPeerFromENR(t *torrent.Torrent, peer *ChainTomlPeer) {
	var bt enr.BT
	if err := peer.Node.Record().Load(&bt); err != nil {
		return // peer doesn't advertise a BT port
	}
	if bt == 0 {
		return
	}

	ip := peer.Node.IP()
	if ip == nil {
		return
	}

	t.AddPeers([]torrent.PeerInfo{{
		Addr:    ipPortAddr{IP: ip, Port: int(bt)},
		Trusted: true,
	}})
}

// ipPortAddr implements net.Addr for use with torrent.PeerInfo.
type ipPortAddr struct {
	IP   net.IP
	Port int
}

func (a ipPortAddr) Network() string { return "tcp" }
func (a ipPortAddr) String() string {
	return net.JoinHostPort(a.IP.String(), strconv.Itoa(a.Port))
}

// ApplyDiscoveredChainToml merges discovered chain.toml entries into the preverified registry.
// Returns the count of new entries added.
func ApplyDiscoveredChainToml(networkName string, discoveredToml []byte, snapDir string) (int, error) {
	discovered, err := ParseChainToml(discoveredToml)
	if err != nil {
		return 0, err
	}

	// Build existing map from current preverified registry
	existing := make(map[string]string)
	if cfg, known := snapcfg.KnownCfg(networkName); known {
		for _, item := range cfg.Preverified.Items {
			existing[item.Name] = item.Hash
		}
	}

	merged, newCount := MergeChainToml(existing, discovered)
	if newCount == 0 {
		return 0, nil
	}

	// Update the global registry with merged entries
	mergedToml := BuildTomlFromMap(merged)
	if err := snapcfg.SetToml(networkName, mergedToml, false); err != nil {
		return 0, err
	}

	// Save merged chain.toml locally
	if err := SaveChainToml(snapDir, mergedToml); err != nil {
		return newCount, fmt.Errorf("saving merged chain.toml: %w", err)
	}

	return newCount, nil
}

// chainTomlDiscoveryLoop is the background loop that discovers chain.toml from P2P peers.
// It operates in two modes:
//   - Acquiring mode: merges discovered entries into the preverified registry (before initial sync)
//   - Verify mode: compares discovered entries against local manifest (after initial sync)
func (d *Downloader) chainTomlDiscoveryLoop(ctx context.Context, networkName string) {
	// Initial delay to let P2P establish connections
	select {
	case <-ctx.Done():
		return
	case <-time.After(30 * time.Second):
	}

	// Re-publish existing chain.toml ENR entry now that P2P is likely running.
	// This is needed because the ENR updater is set before P2P servers start,
	// so any early PublishLocalChainToml call would be a no-op.
	if pubErr := d.PublishLocalChainToml(); pubErr != nil {
		d.logger.Debug("[chaintoml] no existing chain.toml to re-publish", "err", pubErr)
	} else {
		d.logger.Info("[chaintoml] re-published existing chain.toml ENR entry")
	}

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		d.lock.RLock()
		fn := d.nodeSourceFn
		d.lock.RUnlock()

		var ns NodeSource
		if fn != nil {
			ns = fn()
		}
		if ns == nil {
			d.logger.Debug("[chaintoml] node source not yet available, waiting...")
		}
		if ns != nil {
			cfg, known := snapcfg.KnownCfg(networkName)
			if known && cfg.Local {
				// VERIFY MODE: initial sync done, just compare
				d.verifyChainToml(ctx, networkName, ns)
			} else {
				// ACQUIRING MODE: merge discovered entries into registry
				d.acquireChainToml(ctx, networkName, ns)
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// acquireChainToml discovers the best chain.toml from peers, downloads it, and merges
// new entries into the preverified registry.
func (d *Downloader) acquireChainToml(ctx context.Context, networkName string, ns NodeSource) {
	allNodes := ns.AllNodes()
	// Count chain-toml entries and log details for debugging
	var withChainToml int
	for _, node := range allNodes {
		var ct enr.ChainToml
		if err := node.Record().Load(&ct); err == nil {
			withChainToml++
			d.logger.Debug("[chaintoml] found chain-toml entry", "node", node.ID().TerminalString(), "authoritativeTx", ct.AuthoritativeBlocks, "knownTx", ct.KnownBlocks)
		}
	}
	d.logger.Info("[chaintoml] scanning peers for chain-toml ENR entry", "peers", len(allNodes), "withChainToml", withChainToml)

	best := DiscoverChainToml(ns)
	if best == nil {
		d.logger.Info("[chaintoml] no peers with chain-toml ENR entry")
		return
	}

	d.logger.Info("[chaintoml] discovered peer chain.toml",
		"authoritativeTx", best.ChainToml.AuthoritativeBlocks,
		"knownTx", best.ChainToml.KnownBlocks,
		"infoHash", hex.EncodeToString(best.ChainToml.InfoHash[:]))

	tomlBytes, err := d.downloadChainToml(ctx, best)
	if err != nil {
		d.logger.Warn("[chaintoml] failed to download chain.toml", "err", err)
		return
	}

	newCount, err := ApplyDiscoveredChainToml(networkName, tomlBytes, d.snapDir())
	if err != nil {
		d.logger.Warn("[chaintoml] failed to apply chain.toml", "err", err)
		return
	}

	if newCount > 0 {
		d.logger.Info("[chaintoml] applied discovered entries", "new", newCount)
	} else {
		d.logger.Debug("[chaintoml] no new entries from peer")
	}

	// Signal that the P2P manifest is ready so the snapshot stage can proceed.
	// Readiness depends on a successful discovery+download+apply cycle, even if it
	// added zero new entries (e.g. the manifest was empty or already fully merged).
	// Otherwise --snap.p2p-manifest nodes can hang forever when peers' manifests
	// don't add anything new.
	if d.manifestReady != nil {
		select {
		case <-d.manifestReady:
			// already closed
		default:
			close(d.manifestReady)
		}
	}
}

// verifyChainToml discovers chain.toml from peers and compares against the local manifest.
// Logs agreement, new entries, and hash mismatches without modifying the local state.
func (d *Downloader) verifyChainToml(ctx context.Context, networkName string, ns NodeSource) {
	best := DiscoverChainToml(ns)
	if best == nil {
		d.logger.Debug("[chaintoml] verify: no peers with chain-toml ENR entry")
		return
	}

	tomlBytes, err := d.downloadChainToml(ctx, best)
	if err != nil {
		d.logger.Warn("[chaintoml] verify: failed to download chain.toml", "err", err)
		return
	}

	discovered, err := ParseChainToml(tomlBytes)
	if err != nil {
		d.logger.Warn("[chaintoml] verify: failed to parse chain.toml", "err", err)
		return
	}

	// Build local map from current preverified registry
	local := make(map[string]string)
	if cfg, known := snapcfg.KnownCfg(networkName); known {
		for _, item := range cfg.Preverified.Items {
			local[item.Name] = item.Hash
		}
	}

	matching, newEntries, mismatches := CompareChainToml(local, discovered)

	if len(mismatches) > 0 {
		for _, name := range mismatches {
			d.logger.Warn("[chaintoml] hash mismatch",
				"file", name,
				"local", local[name],
				"peer", discovered[name])
		}
	}
	if newEntries > 0 {
		d.logger.Log(log.LvlInfo, "[chaintoml] peer has new entries not in local manifest", "count", newEntries)
	}
	if matching > 0 {
		d.logger.Debug("[chaintoml] peer agrees on entries", "count", matching)
	}
}
