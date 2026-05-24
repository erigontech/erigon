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

package downloader

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"strings"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/rpc"
)

// ParentCutSchemaVersion is the on-disk format version for parent-cut.json.
// Bump if the shape changes incompatibly.
const ParentCutSchemaVersion = 1

// ParentCutCaptureSource identifies how the parent-cut was obtained.
type ParentCutCaptureSource string

const (
	// CaptureLive means the cut was captured from a live JSON-RPC endpoint
	// at the recorded CapturedAt time. The result is reproducible only via
	// the captured file; re-running against the live endpoint may produce
	// a different block (e.g. if --cut-block was "latest").
	CaptureLive ParentCutCaptureSource = "live"
	// CaptureFile means the cut was loaded from a pre-existing parent-cut.json
	// file (forward-distributed via repo or out-of-band). The on-wire result
	// matches the original capture byte-for-byte.
	CaptureFile ParentCutCaptureSource = "file"
)

// ParentCut is the deterministic record of a fork's cut point: the
// captured EL block at the cut and the reference to the parent's V2
// manifest at that point. fork-from consumes a ParentCut to produce the
// derived chain config, the new V2 manifest's [parent] section, and the
// pre-cut file-copy plan.
//
// Distributed as JSON for human inspection (matches ethpandaops
// shadow-fork's `_snapshot_eth_getBlockByNumber.json` precedent).
// Producers MUST emit canonical key-sorted JSON so a content-hash of
// the file is stable across producers.
//
// Post-merge constraint (Erigon does not support PoW): the captured
// block must carry a non-zero ParentBeaconBlockRoot or be after the
// chain's merge block. fork-from enforces this at capture + load time
// (LoadParentCut returns an error on pre-merge captures).
type ParentCut struct {
	// Schema is the on-disk format version (currently 1). Distinct from
	// the V2 manifest's version — this artefact has its own evolution.
	Schema int `json:"schema"`

	// ParentChain is the parent chain name (e.g. "mainnet", "sepolia").
	// Matches ChainConfig.ChainName.
	ParentChain string `json:"parent_chain"`

	// ParentChainID is the EL chain id of the parent. Preserved into the
	// fork's chain.Config.ChainID (forks share parent's chainId for
	// replay protection; the fork's p2p network identity uses a separate
	// NetworkID).
	ParentChainID uint64 `json:"parent_chain_id"`

	// CutBlock is the EL block number at which the fork diverges. Every
	// block strictly before CutBlock is inherited from the parent's
	// snapshots; CutBlock itself is the first block executed under the
	// fork's rules.
	CutBlock uint64 `json:"cut_block"`

	// CutBlockHash is the parent block's hash AT CutBlock. The fork's
	// first block (CutBlock) has CutBlockHash as its ParentHash.
	CutBlockHash common.Hash `json:"cut_block_hash"`

	// CutBlockTimestamp is the parent block's Unix-seconds timestamp at
	// CutBlock. fork-from uses it to derive shadowfork_cutoff_time
	// (the timestamp from which the fork's new fork-activation schedule
	// applies).
	CutBlockTimestamp uint64 `json:"cut_block_timestamp"`

	// CutBlockParentHash is the parent block's ParentHash field at
	// CutBlock — i.e. the hash of block (CutBlock - 1). Recorded so a
	// fork-follower can independently verify the cut block's lineage
	// without consulting the parent chain.
	CutBlockParentHash common.Hash `json:"cut_block_parent_hash"`

	// ParentManifestName is the on-disk name of the parent's V2 manifest
	// at the time of capture (e.g. "chain.v2.<enr-fp>.<seq>.toml"). May
	// be empty when the cut was captured from a parent that doesn't yet
	// publish V2 manifests (root chains pre-Phase-1).
	ParentManifestName string `json:"parent_manifest_name,omitempty"`

	// ParentManifestHash is the BitTorrent info-hash (20 bytes, hex) of
	// the parent's V2 manifest at capture time. Populates
	// chain.Config.ParentManifestHash and the V2 [parent] section's
	// manifest_hash field.
	ParentManifestHash string `json:"parent_manifest_hash,omitempty"`

	// Source records how this ParentCut was obtained — "live" (captured
	// from a JSON-RPC endpoint at CapturedAt) or "file" (loaded from a
	// pre-existing parent-cut.json). Informational; consumers must
	// treat both as equally authoritative once the file exists.
	Source ParentCutCaptureSource `json:"source"`

	// SourceRef is the source descriptor — RPC URL for Source=="live"
	// (with credentials stripped), file path for Source=="file".
	// Informational; not used for verification.
	SourceRef string `json:"source_ref,omitempty"`

	// CapturedAt is the wall-clock time the cut was first captured from
	// its source (Unix seconds). Reproducibility-relevant when the
	// source was a live endpoint that may have advanced; the captured
	// file is the canonical artefact regardless.
	CapturedAt int64 `json:"captured_at"`
}

// MustValidate returns an error if the ParentCut is missing fields that
// fork-from requires. Called by LoadParentCut after JSON decode and by
// CaptureParentCut after live capture.
func (p *ParentCut) Validate() error {
	if p == nil {
		return fmt.Errorf("parent-cut: nil")
	}
	if p.Schema != ParentCutSchemaVersion {
		return fmt.Errorf("parent-cut: unsupported schema version %d (want %d)", p.Schema, ParentCutSchemaVersion)
	}
	if p.ParentChain == "" {
		return fmt.Errorf("parent-cut: parent_chain is empty")
	}
	if p.ParentChainID == 0 {
		return fmt.Errorf("parent-cut: parent_chain_id is zero")
	}
	if p.CutBlock == 0 {
		return fmt.Errorf("parent-cut: cut_block is zero — pre-merge cuts unsupported, and block 0 is genesis")
	}
	if (p.CutBlockHash == common.Hash{}) {
		return fmt.Errorf("parent-cut: cut_block_hash is zero")
	}
	if p.CutBlockTimestamp == 0 {
		return fmt.Errorf("parent-cut: cut_block_timestamp is zero")
	}
	if p.Source != CaptureLive && p.Source != CaptureFile {
		return fmt.Errorf("parent-cut: source %q is not %q or %q", p.Source, CaptureLive, CaptureFile)
	}
	if p.ParentManifestHash != "" {
		// 20-byte hex info-hash; tolerate omission for root chains
		// pre-Phase-1 but reject malformed when present.
		hb, err := hex.DecodeString(p.ParentManifestHash)
		if err != nil || len(hb) != 20 {
			return fmt.Errorf("parent-cut: parent_manifest_hash must be 40 hex chars (got %d, err %v)", len(p.ParentManifestHash), err)
		}
	}
	return nil
}

// MarshalCanonical writes the ParentCut as canonical (key-sorted,
// indented) JSON. Producers MUST use this for the on-disk file so the
// file's content hash is stable across producers — anyone with the
// same struct value emits identical bytes.
func (p *ParentCut) MarshalCanonical() ([]byte, error) {
	// encoding/json sorts struct fields by source-declaration order, not
	// alphabetically. Use a map to force alphabetical key ordering, the
	// universal "canonical JSON" convention.
	asMap := make(map[string]any, 11)
	data, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("parent-cut marshal stage 1: %w", err)
	}
	if err := json.Unmarshal(data, &asMap); err != nil {
		return nil, fmt.Errorf("parent-cut remap for canonical sort: %w", err)
	}
	// json.Marshal of a map already sorts keys alphabetically (per
	// encoding/json docs). Indent for human readability.
	out, err := json.MarshalIndent(asMap, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("parent-cut marshal canonical: %w", err)
	}
	return append(out, '\n'), nil
}

// SaveParentCut writes a canonical parent-cut.json to path. Truncates
// + replaces any existing file.
func SaveParentCut(path string, p *ParentCut) error {
	if err := p.Validate(); err != nil {
		return fmt.Errorf("save parent-cut: %w", err)
	}
	bytes, err := p.MarshalCanonical()
	if err != nil {
		return fmt.Errorf("save parent-cut: %w", err)
	}
	return os.WriteFile(path, bytes, 0o644)
}

// LoadParentCut reads + validates a parent-cut.json from path.
func LoadParentCut(path string) (*ParentCut, error) {
	bytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read parent-cut %s: %w", path, err)
	}
	var p ParentCut
	if err := json.Unmarshal(bytes, &p); err != nil {
		return nil, fmt.Errorf("parse parent-cut %s: %w", path, err)
	}
	if err := p.Validate(); err != nil {
		return nil, fmt.Errorf("parent-cut %s: %w", path, err)
	}
	return &p, nil
}

// CaptureParentCut connects to an EL JSON-RPC endpoint, fetches the
// block at cutBlock (use 0 to mean "latest"), and returns a ParentCut
// ready to save. parentChain is the operator-supplied parent chain name
// (e.g. "mainnet"); parentManifestName / parentManifestHash are the
// parent's V2 manifest reference at the time of capture (typically
// looked up out-of-band; empty when the parent doesn't yet publish V2).
//
// Live captures embed the timestamp in CapturedAt; rerunning against a
// moving tip may produce a different block — the resulting file is the
// canonical artefact, not the live call.
//
// logger may be nil; the RPC layer's diagnostic logs are silenced in
// that case (a quiet capture is fine — the validation errors here cover
// the meaningful failures).
func CaptureParentCut(
	ctx context.Context,
	rpcURL string,
	parentChain string,
	cutBlock uint64,
	parentManifestName, parentManifestHash string,
	logger log.Logger,
) (*ParentCut, error) {
	if logger == nil {
		logger = log.Root()
	}
	client, err := rpc.DialContext(ctx, rpcURL, logger)
	if err != nil {
		return nil, fmt.Errorf("dial parent RPC %s: %w", rpcURL, err)
	}
	defer client.Close()

	var chainIDHex hexutil.Big
	if err := client.CallContext(ctx, &chainIDHex, "eth_chainId"); err != nil {
		return nil, fmt.Errorf("eth_chainId on parent: %w", err)
	}
	chainID := (*big.Int)(&chainIDHex).Uint64()
	if chainID == 0 {
		return nil, fmt.Errorf("parent RPC returned chain id 0")
	}

	// Resolve cutBlock=0 → "latest" so callers can capture-at-head.
	tag := fmt.Sprintf("0x%x", cutBlock)
	if cutBlock == 0 {
		tag = "latest"
	}
	var block struct {
		Number        hexutil.Uint64 `json:"number"`
		Hash          common.Hash    `json:"hash"`
		ParentHash    common.Hash    `json:"parentHash"`
		Timestamp     hexutil.Uint64 `json:"timestamp"`
		// ParentBeaconBlockRoot is non-empty post-Dencun (EIP-4788);
		// not strictly required for our merge-check but useful diagnostic.
		ParentBeaconBlockRoot *common.Hash `json:"parentBeaconBlockRoot,omitempty"`
		Difficulty            *hexutil.Big `json:"difficulty,omitempty"`
	}
	if err := client.CallContext(ctx, &block, "eth_getBlockByNumber", tag, false); err != nil {
		return nil, fmt.Errorf("eth_getBlockByNumber(%s) on parent: %w", tag, err)
	}
	if (block.Hash == common.Hash{}) {
		return nil, fmt.Errorf("parent RPC returned empty block at %s", tag)
	}

	// Post-merge guard. Pre-Dencun blocks won't have ParentBeaconBlockRoot
	// but ALL post-merge blocks have difficulty == 0 (PoS sets difficulty
	// to zero per EIP-3675). A non-zero difficulty signals pre-merge.
	if block.Difficulty != nil {
		d := (*big.Int)(block.Difficulty)
		if d.Sign() > 0 {
			return nil, fmt.Errorf("parent block %d is pre-merge (difficulty %s != 0); Erigon does not support PoW processing — cuts must be at or after the parent's merge block",
				uint64(block.Number), d.String())
		}
	}

	p := &ParentCut{
		Schema:             ParentCutSchemaVersion,
		ParentChain:        parentChain,
		ParentChainID:      chainID,
		CutBlock:           uint64(block.Number),
		CutBlockHash:       block.Hash,
		CutBlockTimestamp:  uint64(block.Timestamp),
		CutBlockParentHash: block.ParentHash,
		ParentManifestName: parentManifestName,
		ParentManifestHash: parentManifestHash,
		Source:             CaptureLive,
		SourceRef:          stripURLCredentials(rpcURL),
		CapturedAt:         time.Now().Unix(),
	}
	if err := p.Validate(); err != nil {
		return nil, fmt.Errorf("captured parent-cut failed validation: %w", err)
	}
	return p, nil
}

// stripURLCredentials drops user:password@ from a URL for diagnostic
// recording. Best-effort; the result is informational, not parsed.
func stripURLCredentials(raw string) string {
	if i := strings.Index(raw, "://"); i >= 0 {
		scheme := raw[:i+3]
		rest := raw[i+3:]
		if at := strings.Index(rest, "@"); at >= 0 {
			return scheme + rest[at+1:]
		}
	}
	return raw
}
