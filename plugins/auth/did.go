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

// Package auth provides UCAN-based authorization for Erigon plugins.
//
// The core library handles UCAN token creation, validation, delegation chain
// verification, and capability matching. It has no dependency on Erigon internals
// and can be tested standalone.
package auth

import (
	"fmt"
	"strings"

	"github.com/erigontech/erigon/common"
)

// DID represents a Decentralized Identifier.
// Supported methods: did:pkh (blockchain addresses), did:key (standalone keys).
type DID struct {
	Method  string         // "pkh" or "key"
	Raw     string         // full DID string
	chainID uint64         // for did:pkh only
	address common.Address // for did:pkh only
}

// ParseDID parses a DID string into its components.
//
// Supported formats:
//   - did:pkh:eip155:{chainId}:{address} — Ethereum address (EOA, contract, AA)
//   - did:key:{multibase-encoded-key}    — standalone cryptographic key
func ParseDID(s string) (DID, error) {
	if !strings.HasPrefix(s, "did:") {
		return DID{}, fmt.Errorf("invalid DID: must start with 'did:': %q", s)
	}

	parts := strings.SplitN(s, ":", 3)
	if len(parts) < 3 {
		return DID{}, fmt.Errorf("invalid DID: too few components: %q", s)
	}

	method := parts[1]
	switch method {
	case "pkh":
		return parseDIDPKH(s)
	case "key":
		return DID{Method: "key", Raw: s}, nil
	default:
		return DID{}, fmt.Errorf("unsupported DID method: %q", method)
	}
}

// parseDIDPKH parses did:pkh:eip155:{chainId}:{address}
func parseDIDPKH(s string) (DID, error) {
	// did:pkh:eip155:1:0xab5801a7d398351b8be11c439e05c5b3259aec9b
	parts := strings.Split(s, ":")
	if len(parts) != 5 {
		return DID{}, fmt.Errorf("invalid did:pkh: expected 5 components, got %d: %q", len(parts), s)
	}
	if parts[2] != "eip155" {
		return DID{}, fmt.Errorf("unsupported did:pkh namespace: %q (only eip155 supported)", parts[2])
	}

	var chainID uint64
	if _, err := fmt.Sscanf(parts[3], "%d", &chainID); err != nil {
		return DID{}, fmt.Errorf("invalid chain ID in did:pkh: %q: %w", parts[3], err)
	}

	if !common.IsHexAddress(parts[4]) {
		return DID{}, fmt.Errorf("invalid address in did:pkh: %q", parts[4])
	}

	return DID{
		Method:  "pkh",
		Raw:     s,
		chainID: chainID,
		address: common.HexToAddress(parts[4]),
	}, nil
}

// Address returns the Ethereum address for did:pkh DIDs.
// Returns zero address for other DID methods.
func (d DID) Address() common.Address {
	return d.address
}

// ChainID returns the chain ID for did:pkh DIDs. Returns 0 for other methods.
func (d DID) ChainID() uint64 {
	return d.chainID
}

// String returns the full DID string.
func (d DID) String() string {
	return d.Raw
}

// IsPKH returns true if this is a did:pkh (blockchain address) DID.
func (d DID) IsPKH() bool {
	return d.Method == "pkh"
}

// IsKey returns true if this is a did:key (standalone key) DID.
func (d DID) IsKey() bool {
	return d.Method == "key"
}

// NewDIDPKH creates a did:pkh DID from a chain ID and address.
func NewDIDPKH(chainID uint64, addr common.Address) DID {
	raw := fmt.Sprintf("did:pkh:eip155:%d:%s", chainID, addr.Hex())
	return DID{
		Method:  "pkh",
		Raw:     raw,
		chainID: chainID,
		address: addr,
	}
}
