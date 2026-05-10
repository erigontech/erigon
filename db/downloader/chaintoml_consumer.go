package downloader

import (
	"context"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/pelletier/go-toml/v2"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snaptype"
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

// parseChainTomlAuto detects V1 vs V2 format and returns a flat
// name -> hash map either way. V2 step-header anchors (ProofRoot,
// AtBlock, AtTxNum, IsPartialBlock) are NOT carried through this map;
// they require an inventory-aware application path that takes a
// BlockReader so ProofRoot can be cross-checked against the local
// block header's stateRoot at advertise time. That path is the
// follow-up to this format-detection landing.
func parseChainTomlAuto(tomlBytes []byte) (map[string]string, error) {
	if DetectVersion(tomlBytes) >= ChainTomlV2Version {
		manifest, err := ParseV2(tomlBytes)
		if err != nil {
			return nil, err
		}
		return flattenV2Manifest(manifest), nil
	}
	return ParseChainToml(tomlBytes)
}

// flattenV2Manifest collects every file in a V2 manifest into a single
// name -> hash map, the shape the legacy merge path consumes. Step
// anchors are dropped — see parseChainTomlAuto for the rationale.
func flattenV2Manifest(m *ChainTomlV2) map[string]string {
	out := make(map[string]string, len(m.Blocks)+len(m.Meta)+len(m.Salt)+len(m.Caplin))
	for k, v := range m.Blocks {
		out[k] = v
	}
	for k, v := range m.Meta {
		out[k] = v
	}
	for k, v := range m.Salt {
		out[k] = v
	}
	for _, dm := range m.Domains {
		for _, f := range dm.Files {
			out[f.Name] = f.Hash
		}
	}
	for _, c := range m.Caplin {
		out[c.Name] = c.Hash
	}
	return out
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
	for k, v := range existing {
		merged[k] = v
	}
	for k, v := range discovered {
		if _, exists := merged[k]; !exists {
			merged[k] = v
			newCount++
		}
	}
	return merged, newCount
}

// RetiredClassification reports the fate of a chain.toml entry that
// the publisher has dropped on republish.
//
// MergedOut entries had their step coverage absorbed into a larger
// file in the new manifest — same Type, range superset. The download
// manager should cancel the in-flight torrent-add for these and let
// the new merged file's torrent-add (which fires from the standard
// "applied new entries" path) take over.
//
// Removed entries have no range-superset in the new manifest. The
// publisher genuinely retracted them; the download manager must drop
// any in-flight torrent-add and not retry.
//
// Quarantine policy: validation failures on retired entries (either
// class) MUST NOT escalate the failure count — the file's chain.toml
// basis has been retracted. The caller is responsible for skipping
// quarantine on these names.
type RetiredClassification struct {
	MergedOut []string
	Removed   []string
}

// ClassifyRetiredEntries returns the classification of entries that
// appear in old but not in new (retired by the publisher's republish).
// Entries that exist in both, or only in new, are not part of the
// classification.
//
// Algorithm: for each retired entry, parse its name into (Type, From,
// To). Search new for an entry with the same Type whose range
// [from, to) is a superset (from <= retired.From AND to >= retired.To).
// If found: merged-out. If not: removed. Entries whose names don't
// parse cleanly (e.g. salt files, config files) are conservatively
// classified as Removed — without range info we can't claim a merge.
//
// Pure function; caller wires the classification into the download
// manager (cancel obsolete torrent-adds) and quarantine policy.
func ClassifyRetiredEntries(old, new map[string]string) RetiredClassification {
	type rangeKey struct {
		from uint64
		to   uint64
	}
	// Keyed on TypeString (e.g. "accounts", "commitment", "bodies") —
	// the parsed snaptype.Type is nil for E3 state files because their
	// type names aren't in the E2 ParseEnum registry, so it can't tell
	// "accounts" apart from "commitment" by Type alone.
	newByType := make(map[string][]rangeKey, len(new))
	for name := range new {
		typ, from, to, ok := snaptype.ParseRange(name)
		if !ok {
			continue
		}
		newByType[typ] = append(newByType[typ], rangeKey{from, to})
	}

	var result RetiredClassification
	for name := range old {
		if _, stillThere := new[name]; stillThere {
			continue
		}
		typ, from, to, ok := snaptype.ParseRange(name)
		if !ok {
			result.Removed = append(result.Removed, name)
			continue
		}
		merged := false
		for _, candidate := range newByType[typ] {
			if candidate.from <= from && candidate.to >= to {
				merged = true
				break
			}
		}
		if merged {
			result.MergedOut = append(result.MergedOut, name)
		} else {
			result.Removed = append(result.Removed, name)
		}
	}
	sort.Strings(result.MergedOut)
	sort.Strings(result.Removed)
	return result
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

// DownloadChainTomlByInfoHash downloads chain.toml from the BitTorrent network using its info-hash.
// The file is downloaded to snapDir, read, and the torrent is dropped afterward.
//
// If peer is non-nil, it is used to directly add the peer's torrent endpoint via AddPeers,
// bypassing the need for DHT or tracker discovery.
func DownloadChainTomlByInfoHash(ctx context.Context, client *torrent.Client, infoHash metainfo.Hash, snapDir string, peer *ChainTomlPeer) ([]byte, error) {
	t, _ := client.AddTorrentInfoHash(infoHash)
	defer t.Drop()

	// If the peer advertised a BT port in their ENR, add them directly as a torrent peer.
	if peer != nil {
		addTorrentPeerFromENR(t, peer)
	}

	// Use a timeout to avoid blocking the discovery loop indefinitely.
	dlCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	select {
	case <-t.GotInfo():
	case <-dlCtx.Done():
		return nil, fmt.Errorf("waiting for chain.toml torrent info: %w", dlCtx.Err())
	}

	// Validate the torrent's metainfo — a malicious peer could advertise an
	// info-hash whose torrent is a multi-file bundle or names a file other
	// than chain.toml. Reject both: consumers should only download manifests
	// that match the expected single-file shape.
	info := t.Info()
	if info == nil {
		return nil, fmt.Errorf("chain.toml torrent info missing")
	}
	if len(info.Files) != 0 {
		return nil, fmt.Errorf("chain.toml torrent is multi-file (%d files), expected single file", len(info.Files))
	}
	if info.Name != ChainTomlFileName {
		return nil, fmt.Errorf("chain.toml torrent names %q, expected %q", info.Name, ChainTomlFileName)
	}

	t.DownloadAll()

	select {
	case <-t.Complete().On():
	case <-dlCtx.Done():
		return nil, fmt.Errorf("downloading chain.toml torrent: %w", dlCtx.Err())
	}

	// Read the downloaded chain.toml file. The validation above guarantees
	// the torrent wrote to this path.
	filePath := ChainTomlPath(snapDir)
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("reading downloaded chain.toml: %w", err)
	}

	return data, nil
}

// addTorrentPeerFromENR extracts the BT port and IP from a peer's ENR record
// and adds them as a direct torrent peer. The IP defaults to the node's
// advertised IP if not separately specified.
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
	return net.JoinHostPort(a.IP.String(), fmt.Sprintf("%d", a.Port))
}

// ApplyDiscoveredChainToml integrates discovered chain.toml entries into
// the registry. Behaviour depends on bootstrapFromPreverified:
//
//   - bootstrap=true: merges discovered into existing-from-preverified
//     (existing wins on conflict). Used by bootstrap publishers — the
//     published manifest = preverified ∪ local files ∪ discovered.
//   - bootstrap=false: REPLACES existing with discovered. preverified
//     is ignored — V2 nodes use peer-discovered manifest as the sole
//     source of truth, per
//     docs/plans/20260502-app-integration-completion.md §5b.
//
// Returns the count of newly-added entries (relative to the previous
// registry state) plus a RetiredClassification of entries that
// disappeared between the previous registry and the new merged set
// (publisher-republish-after-merge case). Caller is responsible for
// dropping torrent-adds for the retired names.
func ApplyDiscoveredChainToml(networkName string, discoveredToml []byte, snapDir string, bootstrapFromPreverified bool) (int, RetiredClassification, error) {
	var retired RetiredClassification
	discovered, err := parseChainTomlAuto(discoveredToml)
	if err != nil {
		return 0, retired, err
	}

	current := make(map[string]string)
	if cfg, known := snapcfg.KnownCfg(networkName); known {
		for _, item := range cfg.Preverified.Items {
			current[item.Name] = item.Hash
		}
	}

	var merged map[string]string
	var newCount int

	if bootstrapFromPreverified {
		// Bootstrap mode: preverified is the merge base; discovered adds.
		merged, newCount = MergeChainToml(current, discovered)
	} else {
		// V2-only mode: discovered IS the registry. preverified ignored.
		merged = discovered
		for k, v := range discovered {
			if existing, ok := current[k]; !ok || existing != v {
				newCount++
			}
		}
	}

	retired = ClassifyRetiredEntries(current, merged)

	if newCount == 0 && bootstrapFromPreverified && len(retired.MergedOut) == 0 && len(retired.Removed) == 0 {
		// Bootstrap mode preserves the optimisation — nothing changed.
		return 0, retired, nil
	}

	mergedToml := BuildTomlFromMap(merged)
	snapcfg.SetToml(networkName, mergedToml, false)

	if err := SaveChainToml(snapDir, mergedToml); err != nil {
		return newCount, retired, fmt.Errorf("saving merged chain.toml: %w", err)
	}

	return newCount, retired, nil
}

// chainTomlDiscoveryLoop is the background loop that discovers chain.toml from P2P peers.
// It operates in two modes:
//   - Acquiring mode: merges discovered entries into the preverified registry (before initial sync)
//   - Verify mode: compares discovered entries against local manifest (after initial sync)
func (d *Downloader) chainTomlDiscoveryLoop(ctx context.Context, networkName string) {
	// Initial delay to let P2P establish connections.
	// Tightened from 30s to 5s after the forced discv5-ping-on-connect
	// fix made ENR delivery sub-second; a longer wait here was just
	// dominant-loop latency for the V2 startup case.
	select {
	case <-ctx.Done():
		return
	case <-time.After(5 * time.Second):
	}

	// Re-publish existing chain.toml ENR entry now that P2P is likely running.
	// This is needed because the ENR updater is set before P2P servers start,
	// so any early PublishLocalChainToml call would be a no-op.
	if pubErr := d.PublishLocalChainToml(); pubErr != nil {
		d.logger.Debug("[chaintoml] no existing chain.toml to re-publish", "err", pubErr)
	} else {
		d.logger.Info("[chaintoml] re-published existing chain.toml ENR entry")
	}

	// 10s ticker (was 30s) — chain.toml discovery is now the dominant
	// startup phase for V2 consumers, so freshness-checking cadence
	// should match the seconds-scale latency we now expect end-to-end.
	ticker := time.NewTicker(10 * time.Second)
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

	// Fire the production hook so manifest_exchange (subscribed to
	// sentry.PeerConnected on the shared bus) can fetch the peer's V2
	// manifest. The chain-toml ENR entry is now confirmed populated
	// for this peer — re-publishing PeerConnected here gives
	// manifest_exchange the up-to-date ENR view it didn't have when
	// the original PeerConnected event fired (peer's discv5 ENR
	// hadn't propagated chain-toml yet).
	d.lock.RLock()
	hook := d.onPeerWithChainTomlDiscovered
	d.lock.RUnlock()
	if hook != nil {
		hook(best)
	}

	tomlBytes, err := DownloadChainTomlByInfoHash(ctx, d.torrentClient, best.ChainToml.InfoHash, d.snapDir(), best)
	if err != nil {
		d.logger.Warn("[chaintoml] failed to download chain.toml", "err", err)
		return
	}

	newCount, retired, err := ApplyDiscoveredChainToml(networkName, tomlBytes, d.snapDir(), d.cfg.BootstrapFromPreverified)
	if err != nil {
		d.logger.Warn("[chaintoml] failed to apply chain.toml", "err", err)
		return
	}

	// Drop torrent-adds for entries the publisher retired since the
	// previous manifest. Without this the consumer keeps retrying
	// 404s on files that no longer exist on the publisher.
	dropped := len(retired.MergedOut) + len(retired.Removed)
	for _, names := range [][]string{retired.MergedOut, retired.Removed} {
		for _, name := range names {
			d.DropTorrentByName(name)
		}
	}
	if dropped > 0 {
		d.logger.Info("[chaintoml] retired entries dropped",
			"mergedOut", len(retired.MergedOut), "removed", len(retired.Removed))
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

	tomlBytes, err := DownloadChainTomlByInfoHash(ctx, d.torrentClient, best.ChainToml.InfoHash, d.snapDir(), best)
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
