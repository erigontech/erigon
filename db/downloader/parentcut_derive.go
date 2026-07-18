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
	"encoding/hex"
	"fmt"
	"math/big"
	"reflect"
	"strings"

	"github.com/jinzhu/copier"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

// DeriveForkChainConfig builds a fork's chain.Config from a parent's
// chain.Config + a captured ParentCut + the fork's identity.
//
// Follows the ethpandaops shadow-fork pattern:
//   - Parent's ChainID is preserved (replay protection — signed parent
//     txns remain bound to the parent's chainId; the fork inherits this
//     identity at the EL level. p2p network identity uses a separate
//     NetworkID outside chain.Config, in node/ethconfig).
//   - Fork activations that activated AT OR BEFORE the cut are
//     preserved (the fork inherits all activated rules so existing state
//     remains valid).
//   - Fork activations strictly AFTER the cut are dropped (the fork
//     operator adds their own post-cut activations to the returned
//     Config before writing it — exactly the shadow-fork
//     `shadowfork_cutoff_time` walk).
//   - chain.Config.{Parent, CutBlock, ParentManifestHash} fork-lineage
//     fields (added in Phase 2a, d0743a710d) are populated from the
//     ParentCut.
//   - ChainName is set to forkName.
//
// The returned config is a deep copy; safe to mutate without affecting
// the parent.
//
// v1 limitation: Bor (Polygon) and AuRa (Gnosis) consensus chains are
// rejected — their nested fork-activation tables need separate handling
// not yet implemented. Mainstream post-merge chains (mainnet, sepolia,
// holesky, hoodi, future testnets) are in scope.
func DeriveForkChainConfig(parent *chain.Config, cut *ParentCut, forkName string) (*chain.Config, error) {
	if parent == nil {
		return nil, fmt.Errorf("derive fork chain.Config: nil parent")
	}
	if cut == nil {
		return nil, fmt.Errorf("derive fork chain.Config: nil parent-cut")
	}
	if err := cut.Validate(); err != nil {
		return nil, fmt.Errorf("derive fork chain.Config: invalid parent-cut: %w", err)
	}
	if forkName == "" {
		return nil, fmt.Errorf("derive fork chain.Config: empty fork name")
	}
	if forkName == parent.ChainName {
		return nil, fmt.Errorf("derive fork chain.Config: fork name %q is identical to parent chain name", forkName)
	}
	if parent.Bor != nil {
		return nil, fmt.Errorf("derive fork chain.Config: Bor (Polygon) consensus parents are not yet supported")
	}
	if parent.Aura != nil {
		return nil, fmt.Errorf("derive fork chain.Config: AuRa (Gnosis) consensus parents are not yet supported")
	}
	if parent.ChainID == nil || parent.ChainID.Sign() == 0 {
		return nil, fmt.Errorf("derive fork chain.Config: parent chain.Config has no ChainID set")
	}
	if cut.ParentChainID != parent.ChainID.Uint64() {
		return nil, fmt.Errorf("derive fork chain.Config: ParentCut chain id %d does not match parent chain.Config ChainID %s",
			cut.ParentChainID, parent.ChainID.String())
	}

	derived := &chain.Config{}
	if err := copier.CopyWithOption(derived, parent, copier.Option{DeepCopy: true}); err != nil {
		return nil, fmt.Errorf("derive fork chain.Config: deep copy failed: %w", err)
	}

	// Walk every *Block / *Time field via reflection (mirrors
	// p2p/forkid.GatherForks's walk). Drop activations strictly after
	// the cut so the fork starts with a clean post-cut activation
	// table the operator can populate.
	dropPostCutActivations(derived, cut.CutBlock, cut.CutBlockTimestamp)

	// Re-stamp identity.
	derived.ChainName = forkName

	// Fork-lineage fields (Phase 2a additions). Re-decode the
	// hex-encoded manifest hash; empty hex (pre-Phase-1 root parents)
	// leaves ParentManifestHash zero, which is legitimate.
	derived.Parent = cut.ParentChain
	derived.CutBlock = cut.CutBlock
	if cut.ParentManifestHash != "" {
		var hashBytes [20]byte
		decoded, err := hex.DecodeString(cut.ParentManifestHash)
		if err != nil || len(decoded) != 20 {
			return nil, fmt.Errorf("derive fork chain.Config: parent_manifest_hash decode: %w (hex=%q)", err, cut.ParentManifestHash)
		}
		copy(hashBytes[:], decoded)
		derived.ParentManifestHash = hashBytes
	}

	// Populate ParentGenesisHash from the local chainspec registry. Unknown
	// parent (not in the registry) leaves the field zero — the fork can
	// still boot; consumers that want E.2 cross-check will skip it when
	// the field is zero. See ValidateParentIdentity.
	if spec, err := chainspec.ChainSpecByName(cut.ParentChain); err == nil {
		derived.ParentGenesisHash = spec.GenesisHash
	}

	return derived, nil
}

// dropPostCutActivations sets every *Block field whose value > cutBlock
// AND every *Time field whose value > cutTime to nil. Block-genesis
// activations (value 0) are always preserved.
//
// Uses reflection to mirror GatherForks's "every field suffixed Block
// or Time" walk so we cover every fork without listing them by name —
// new forks added to chain.Config get the right treatment automatically.
func dropPostCutActivations(cfg *chain.Config, cutBlock uint64, cutTime uint64) {
	kind := reflect.TypeFor[chain.Config]()
	val := reflect.ValueOf(cfg).Elem()

	uint64PtrType := reflect.TypeFor[*uint64]()
	for i := 0; i < kind.NumField(); i++ {
		field := kind.Field(i)
		if field.Type != uint64PtrType {
			continue
		}
		isBlock := strings.HasSuffix(field.Name, "Block")
		isTime := strings.HasSuffix(field.Name, "Time")
		if !isBlock && !isTime {
			continue
		}
		fieldVal := val.Field(i)
		ptr := fieldVal.Interface().(*uint64)
		if ptr == nil {
			continue
		}
		threshold := cutBlock
		if isTime {
			threshold = cutTime
		}
		if *ptr > threshold {
			// Strictly after the cut → drop.
			fieldVal.Set(reflect.Zero(uint64PtrType))
		}
	}
}

// ParentSectionOpts carries the caller-supplied fields ParentSectionFromCut
// merges with the ParentCut. Kept as a struct to avoid a long positional
// signature — every field is optional except NetworkID (a fork with
// NetworkID=0 collides with the parent's p2p network).
type ParentSectionOpts struct {
	// NetworkID is the fork's p2p network identity. Distinct from the
	// parent's chain id (which the fork inherits for EL replay
	// protection). Must be non-zero.
	NetworkID uint64

	// ParentGenesisHash is the parent chain's genesis block hash,
	// captured at fork-from time. Emitted as parent_genesis_hash on
	// the manifest for the E.2 cross-check.
	ParentGenesisHash common.Hash

	// ParentForks is the parent chain's activated continuous fork
	// schedule at cut time. Emitted as [[parent.parent_forks]] on the
	// manifest for the E.2 fork-ID cross-check. Callers typically
	// derive this via BuildChainIdentity(parent.Config, ...).
	ParentForks []ForkActivation

	// CLGenesisValidatorsRoot / CLForkVersion / CLConfigName come from
	// the fork's CL setup (Phase 2c-CL). Zero values are legal — a
	// pre-CL-integration fork can emit an EL-only ParentSection.
	CLGenesisValidatorsRoot [32]byte
	CLForkVersion           [32]byte
	CLConfigName            string

	// ValidParentTrustRoots is the operator's accept-set captured at
	// fork-from time. nil/empty leaves the manifest field omitted; a
	// fork-follower then falls back to its own --accept-parent-
	// trust-roots config.
	ValidParentTrustRoots []chain.ParentTrustRoot
}

// ParentSectionFromCut populates a manifest [parent] section (the V2
// schema's ParentSection) from a ParentCut plus caller-supplied fields
// carried in opts.
func ParentSectionFromCut(cut *ParentCut, opts ParentSectionOpts) (*ParentSection, error) {
	if cut == nil {
		return nil, fmt.Errorf("parent-section-from-cut: nil parent-cut")
	}
	if err := cut.Validate(); err != nil {
		return nil, fmt.Errorf("parent-section-from-cut: invalid parent-cut: %w", err)
	}
	cutTxNum := uint64(0) // populated by the caller once it has block→txnum mapping; see Phase 2c-EL follow-on
	var genesisHex string
	if (opts.ParentGenesisHash != common.Hash{}) {
		genesisHex = hexNoPrefix(opts.ParentGenesisHash[:])
	}
	return &ParentSection{
		Chain:                   cut.ParentChain,
		ManifestHash:            cut.ParentManifestHash,
		CutBlock:                cut.CutBlock,
		CutTxNum:                cutTxNum,
		CutBlockHash:            cut.CutBlockHash.Hex(),
		Name:                    "", // populated by caller — same as the fork's ChainName
		NetworkID:               opts.NetworkID,
		CLGenesisValidatorsRoot: hexNoPrefix(opts.CLGenesisValidatorsRoot[:]),
		CLForkVersion:           hexNoPrefix(opts.CLForkVersion[:4]),
		CLConfigName:            opts.CLConfigName,
		ValidParentTrustRoots:   trustRootsToEntries(opts.ValidParentTrustRoots),
		ParentGenesisHash:       genesisHex,
		ParentForks:             opts.ParentForks,
	}, nil
}

// trustRootsToEntries converts chain.ParentTrustRoot (raw-bytes pubkey,
// for JSON in chain.Config) to the V2 manifest's ParentTrustRootEntry
// (hex-string pubkey, for TOML). Returns nil for empty/nil input so the
// optional `valid_parent_trust_roots` TOML field is omitted entirely
// when the operator didn't pin a set.
func trustRootsToEntries(roots []chain.ParentTrustRoot) []ParentTrustRootEntry {
	if len(roots) == 0 {
		return nil
	}
	out := make([]ParentTrustRootEntry, len(roots))
	for i, r := range roots {
		out[i] = ParentTrustRootEntry{
			Kind:   r.Kind,
			Pubkey: hexNoPrefix(r.Pubkey),
			DID:    r.DID,
		}
	}
	return out
}

// EntriesToTrustRoots is the inverse of trustRootsToEntries: V2
// manifest hex form → chain.Config raw-bytes form. Used by consumers
// reading a fork's manifest to populate the structured field for
// downstream trust-root verification. Hex-decode errors yield an empty
// pubkey on that entry — the verifier will reject it on subsequent
// equality check against a configured root.
func EntriesToTrustRoots(entries []ParentTrustRootEntry) []chain.ParentTrustRoot {
	if len(entries) == 0 {
		return nil
	}
	out := make([]chain.ParentTrustRoot, len(entries))
	for i, e := range entries {
		pk, err := hex.DecodeString(e.Pubkey)
		if err != nil {
			pk = nil
		}
		out[i] = chain.ParentTrustRoot{
			Kind:   e.Kind,
			Pubkey: pk,
			DID:    e.DID,
		}
	}
	return out
}

func hexNoPrefix(b []byte) string {
	return hex.EncodeToString(b)
}

// ensure ChainName comparison works against the empty parent (e.g.
// caller passes a brand-new config).
var _ = (*big.Int)(nil)
