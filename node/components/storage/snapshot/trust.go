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

// Package snapshot provides the file inventory, trust model, and range
// arithmetic for decentralized snapshot distribution.
//
// Trust levels are incremental — each builds on the previous:
//
//   - None: file downloaded, no verification beyond BitTorrent content integrity
//   - Consensus: multiple independent peers agree on the same torrent hash
//   - Verified: cryptographic proof via UCAN delegation chain, or locally generated
//
// See issues #19657 (POC), #19658 (consensus), #19659 (UCAN verification).
package snapshot

import "fmt"

// TrustLevel represents how a snapshot file's integrity has been established.
// Higher values indicate stronger trust. Files move up the trust ladder over
// their lifecycle (promotion) but never move down.
type TrustLevel int

const (
	// TrustNone means the file was downloaded from a peer without any
	// verification beyond BitTorrent content integrity (hash of chunks).
	// A single malicious peer could have advertised a bad info-hash.
	TrustNone TrustLevel = iota

	// TrustConsensus means multiple independent peers (threshold M-of-N)
	// reported the same torrent hash for this step range. Defeats isolated
	// bad actors but not coordinated Sybil attacks.
	TrustConsensus

	// TrustVerified means the file has cryptographic provenance:
	// - Locally generated (collation/merge produced it)
	// - From the compiled-in preverified registry
	// - UCAN delegation chain verified from a root authority
	// - Content independently verified against a known commitment root
	TrustVerified
)

func (t TrustLevel) String() string {
	switch t {
	case TrustNone:
		return "none"
	case TrustConsensus:
		return "consensus"
	case TrustVerified:
		return "verified"
	default:
		return fmt.Sprintf("unknown(%d)", int(t))
	}
}

// ParseTrustLevel parses a trust level string from chain.toml or CLI flags.
func ParseTrustLevel(s string) (TrustLevel, error) {
	switch s {
	case "none":
		return TrustNone, nil
	case "consensus":
		return TrustConsensus, nil
	case "verified":
		return TrustVerified, nil
	default:
		return TrustNone, fmt.Errorf("unknown trust level: %q (expected none, consensus, or verified)", s)
	}
}

// Satisfies returns true if this trust level meets or exceeds the required level.
// Used by consumers to filter files: only accept files whose trust >= required.
func (t TrustLevel) Satisfies(required TrustLevel) bool {
	return t >= required
}

// Promote returns the higher of two trust levels. Used when a file gains
// additional trust (e.g. downloaded at TrustNone, then consensus reached).
func (t TrustLevel) Promote(other TrustLevel) TrustLevel {
	if other > t {
		return other
	}
	return t
}
