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

	"github.com/erigontech/erigon/execution/chain"
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

// ParentSectionFromCut populates a manifest [parent] section (the V2
// schema's ParentSection) from a ParentCut. clGenesisValidatorsRoot
// and clForkVersion come from the fork's CL setup (Phase 2c-CL); the
// caller threads them in. clConfigName is optional human-readable
// (e.g. "msf-0"); empty is fine.
//
// network_id is the fork's p2p network identity — distinct from the
// parent's chain id so the fork's p2p network is distinguishable.
//
// validParentTrustRoots is the operator's accept-set captured at
// fork-from time, propagated from the derived chain.Config. nil/empty
// is legal (means the operator didn't pin a parent-trust-root set;
// fork-followers fall back to whatever their own --accept-parent-
// trust-roots config says). See memory/fork-trust-root-model-2026-05-24.
func ParentSectionFromCut(cut *ParentCut, networkID uint64, clGenesisValidatorsRoot, clForkVersion [32]byte, clConfigName string, validParentTrustRoots []chain.ParentTrustRoot) (*ParentSection, error) {
	if cut == nil {
		return nil, fmt.Errorf("parent-section-from-cut: nil parent-cut")
	}
	if err := cut.Validate(); err != nil {
		return nil, fmt.Errorf("parent-section-from-cut: invalid parent-cut: %w", err)
	}
	cutTxNum := uint64(0) // populated by the caller once it has block→txnum mapping; see Phase 2c-EL follow-on
	return &ParentSection{
		Chain:                   cut.ParentChain,
		ManifestHash:            cut.ParentManifestHash,
		CutBlock:                cut.CutBlock,
		CutTxNum:                cutTxNum,
		CutBlockHash:            cut.CutBlockHash.Hex(),
		Name:                    "", // populated by caller — same as the fork's ChainName
		NetworkID:               networkID,
		CLGenesisValidatorsRoot: hexNoPrefix(clGenesisValidatorsRoot[:]),
		CLForkVersion:           hexNoPrefix(clForkVersion[:4]),
		CLConfigName:            clConfigName,
		ValidParentTrustRoots:   trustRootsToEntries(validParentTrustRoots),
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
