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

package snapshotauth

import (
	"encoding/hex"
	"fmt"
	"strings"
)

// Capability is a colon-namespaced authorisation token. The package
// recognises a fixed set of snapshot capabilities; unrecognised values
// are rejected at parse time so a typo in a delegation surfaces
// immediately rather than at trust-evaluation time.
type Capability string

const (
	// CapAdvertise authorises the audience to advertise V2 manifests
	// the verifier should treat as trusted source claims.
	CapAdvertise Capability = "snapshot:advertise"

	// CapServe authorises the audience to serve snapshot file bytes
	// to the verifier (i.e. be picked as a download source).
	CapServe Capability = "snapshot:serve"

	// CapDelegate authorises the audience to issue further
	// delegations under a chain rooted at this delegation. Without
	// this, the audience is a leaf authority — it can advertise/serve
	// per its own caps but cannot pass authority on.
	CapDelegate Capability = "snapshot:delegate"

	// CapContentHashPrefix is the prefix of a Content-UCAN capability.
	// Unlike the fixed snapshot:* capabilities, a content-hash
	// capability is dynamic: chain.v2:hash:<sha256-hex> binds a Content
	// UCAN to the exact manifest bytes it attests. Recognised
	// prefix-wise (see ParseContentHashCapability), not by exact match.
	CapContentHashPrefix = "chain.v2:hash:"

	// CapForkedFromPrefix is the prefix of a Fork-Authority-UCAN
	// capability binding a fork's authority to the specific parent trust
	// root that vetted ParentManifestHash at fork creation. Dynamic
	// (parsed prefix-wise via ParseForkedFromCapability), embedded
	// during MintForkAuthorityUCAN. The form is
	// fork:from:<33-byte-compressed-secp256k1-pubkey-hex> — the parent
	// trust root's cryptographic identity. Kind (did/enr/bootnode) is
	// recoverable from the operator's accept-set lookup keyed by
	// pubkey; it is not embedded in the capability.
	//
	// Embedding this capability ties the fork's authority UCAN
	// (signed by the fork's trust root) to a specific parent trust
	// root, closing the trust loop: a fork-follower who trusts the
	// fork's trust root transitively trusts the fork's claim about
	// which parent state was vetted. See memory/fork-trust-root-model-
	// 2026-05-24.
	CapForkedFromPrefix = "fork:from:"
)

// AllCapabilities is the canonical set the package recognises.
var AllCapabilities = []Capability{
	CapAdvertise,
	CapServe,
	CapDelegate,
}

// ParseCapabilities parses a comma-separated list of capability
// strings, returning the canonical (sorted, de-duplicated) form. An
// unrecognised value is reported as an error so a CLI typo is caught
// at issue time rather than verify time.
func ParseCapabilities(s string) ([]string, error) {
	if strings.TrimSpace(s) == "" {
		return nil, fmt.Errorf("capability list is empty")
	}
	known := make(map[string]struct{}, len(AllCapabilities))
	for _, c := range AllCapabilities {
		known[string(c)] = struct{}{}
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if _, ok := known[p]; !ok {
			return nil, fmt.Errorf("unknown capability %q (known: %s)", p, capabilityNamesJoined())
		}
		out = append(out, p)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("capability list resolved to empty after trimming")
	}
	return canonicalCapabilities(out), nil
}

// ContentHashCapability builds the Content-UCAN capability binding an
// attestation to a manifest: chain.v2:hash:<sha256-hex>. hashHex is the
// lowercase hex sha256 of the manifest's .toml bytes.
func ContentHashCapability(hashHex string) string {
	return CapContentHashPrefix + hashHex
}

// ParseContentHashCapability reports whether cap is a content-hash
// capability and, if so, returns the hex hash it binds. ok=false for
// any non-content-hash capability.
func ParseContentHashCapability(capability string) (hashHex string, ok bool) {
	if !strings.HasPrefix(capability, CapContentHashPrefix) {
		return "", false
	}
	return strings.TrimPrefix(capability, CapContentHashPrefix), true
}

// ForkedFromCapability builds a Fork-Authority-UCAN capability binding
// the fork to a specific parent trust root: fork:from:<pubkey-hex>.
// parentTrustRootPubkey is the 33-byte compressed secp256k1 public key
// of the parent trust root that vetted ParentManifestHash at fork-from
// time. Returns an error on wrong-length input — better to surface a
// mistake at mint time than ship a malformed UCAN.
func ForkedFromCapability(parentTrustRootPubkey []byte) (string, error) {
	if len(parentTrustRootPubkey) != PubKeyLen {
		return "", fmt.Errorf("ForkedFromCapability: pubkey length %d (want %d)", len(parentTrustRootPubkey), PubKeyLen)
	}
	return CapForkedFromPrefix + hex.EncodeToString(parentTrustRootPubkey), nil
}

// ParseForkedFromCapability reports whether cap is a forked-from
// capability and, if so, returns the decoded 33-byte parent trust root
// pubkey it binds. ok=false for any non-forked-from capability or for
// a malformed pubkey hex.
func ParseForkedFromCapability(capability string) (parentTrustRootPubkey []byte, ok bool) {
	if !strings.HasPrefix(capability, CapForkedFromPrefix) {
		return nil, false
	}
	hexPub := strings.TrimPrefix(capability, CapForkedFromPrefix)
	pub, err := hex.DecodeString(hexPub)
	if err != nil || len(pub) != PubKeyLen {
		return nil, false
	}
	return pub, true
}

func capabilityNamesJoined() string {
	parts := make([]string, len(AllCapabilities))
	for i, c := range AllCapabilities {
		parts[i] = string(c)
	}
	return strings.Join(parts, ", ")
}
