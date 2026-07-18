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
	"errors"
	"fmt"
)

// Sentinel errors returned by PickIssuerFromAcceptSet.
var (
	ErrPickIssuerParentNotRoot         = errors.New("parent authority UCAN is not a root (has ParentHash)")
	ErrPickIssuerNotInAcceptSet        = errors.New("parent authority UCAN issuer is not in the operator's accept-set")
	ErrPickIssuerEmptyAcceptSet        = errors.New("accept-set is empty")
	ErrPickIssuerAcceptSetEntryInvalid = errors.New("accept-set entry has invalid pubkey length")
)

// PickIssuerFromAcceptSet reads the parent chain's authority UCAN,
// resolves its root Issuer (the parent's trust-root pubkey), and
// returns the index of the matching entry in the operator's accept
// set. This is the A.4 picker — at fork-from time, the operator
// supplied a `--valid-parent-trust-roots` set; the picker records
// which specific entry from that set the parent actually signs under,
// so the fork-authority UCAN can embed that exact pubkey as its
// forked-from:<id> capability.
//
// parentAuthorityUCAN must be a root delegation — ParentHash nil.
// A nested chain would require the caller to first walk to the root
// (out of scope; this picker is a leaf primitive).
//
// Byte-length invariant: every accept-set entry must be PubKeyLen
// (compressed secp256k1). A wrong-length entry is a caller bug and
// returns ErrPickIssuerAcceptSetEntryInvalid — safer to fail loud
// than accept a shape that couldn't possibly match the Issuer.
func PickIssuerFromAcceptSet(parentAuthorityUCAN []byte, acceptSet [][]byte) (int, error) {
	if len(acceptSet) == 0 {
		return -1, ErrPickIssuerEmptyAcceptSet
	}
	for i, entry := range acceptSet {
		if len(entry) != PubKeyLen {
			return -1, fmt.Errorf("%w: index=%d len=%d want=%d", ErrPickIssuerAcceptSetEntryInvalid, i, len(entry), PubKeyLen)
		}
	}
	d, err := Decode(parentAuthorityUCAN)
	if err != nil {
		return -1, fmt.Errorf("decode parent authority UCAN: %w", err)
	}
	if d.ParentHash != nil {
		return -1, ErrPickIssuerParentNotRoot
	}
	for i, entry := range acceptSet {
		if bytes.Equal(entry, d.Issuer) {
			return i, nil
		}
	}
	return -1, fmt.Errorf("%w: issuer=%x", ErrPickIssuerNotInAcceptSet, d.Issuer)
}
