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
	"bytes"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/mr-tron/base58"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/p2p/enode"
)

// didKeySecp256k1Prefix is the secp256k1-pub multicodec (0xe7) in
// unsigned-varint form, the leading bytes of a did:key payload for a
// secp256k1 public key.
var didKeySecp256k1Prefix = []byte{0xe7, 0x01}

// ParseTrustRoots parses the --snapshot.trust-roots specification (a
// comma-separated list) into TrustRoots. Each element is one of:
//
//   - "enr:..." / "enode://..." — a node record; the embedded
//     secp256k1 pubkey becomes a RootENR.
//   - "did:key:z..."            — a base58btc did:key carrying a
//     secp256k1 pubkey; becomes a RootDID (the DID string is retained
//     for diagnostics).
//   - 66 hex chars               — a raw 33-byte compressed secp256k1
//     pubkey; becomes a RootENR.
//
// Whitespace around elements and empty elements are ignored; an empty
// spec yields a nil slice and no error.
func ParseTrustRoots(spec string) ([]TrustRoot, error) {
	var roots []TrustRoot
	for _, raw := range strings.Split(spec, ",") {
		elem := strings.TrimSpace(raw)
		if elem == "" {
			continue
		}
		root, err := parseTrustRoot(elem)
		if err != nil {
			return nil, fmt.Errorf("trust root %q: %w", elem, err)
		}
		roots = append(roots, root)
	}
	return roots, nil
}

func parseTrustRoot(elem string) (TrustRoot, error) {
	switch {
	case strings.HasPrefix(elem, "did:web:"):
		return TrustRoot{}, fmt.Errorf("did:web trust roots are not yet supported")
	case strings.HasPrefix(elem, "did:"):
		return parseDIDKeyRoot(elem)
	case strings.HasPrefix(elem, "enr:"), strings.HasPrefix(elem, "enode://"):
		n, err := enode.Parse(enode.ValidSchemes, elem)
		if err != nil {
			return TrustRoot{}, fmt.Errorf("parse node record: %w", err)
		}
		pub := n.Pubkey()
		if pub == nil {
			return TrustRoot{}, fmt.Errorf("node record carries no secp256k1 key")
		}
		return TrustRoot{Kind: RootENR, Pubkey: crypto.CompressPubkey(pub)}, nil
	default:
		pub, err := hex.DecodeString(strings.TrimPrefix(elem, "0x"))
		if err != nil {
			return TrustRoot{}, fmt.Errorf("not an enr:, did:key:, or hex pubkey")
		}
		if err := validateCompressedPubkey(pub); err != nil {
			return TrustRoot{}, err
		}
		return TrustRoot{Kind: RootENR, Pubkey: pub}, nil
	}
}

func parseDIDKeyRoot(did string) (TrustRoot, error) {
	const prefix = "did:key:z"
	if !strings.HasPrefix(did, prefix) {
		return TrustRoot{}, fmt.Errorf("only did:key with base58btc multibase (z...) is supported")
	}
	decoded, err := base58.Decode(strings.TrimPrefix(did, prefix))
	if err != nil {
		return TrustRoot{}, fmt.Errorf("base58 decode: %w", err)
	}
	if !bytes.HasPrefix(decoded, didKeySecp256k1Prefix) {
		return TrustRoot{}, fmt.Errorf("did:key is not a secp256k1 key")
	}
	pub := decoded[len(didKeySecp256k1Prefix):]
	if err := validateCompressedPubkey(pub); err != nil {
		return TrustRoot{}, err
	}
	return TrustRoot{Kind: RootDID, Pubkey: pub, DID: did}, nil
}

func validateCompressedPubkey(pub []byte) error {
	if len(pub) != 33 {
		return fmt.Errorf("compressed secp256k1 pubkey must be 33 bytes, got %d", len(pub))
	}
	if _, err := crypto.DecompressPubkey(pub); err != nil {
		return fmt.Errorf("invalid compressed secp256k1 pubkey: %w", err)
	}
	return nil
}
