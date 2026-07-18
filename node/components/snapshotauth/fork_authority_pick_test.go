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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// mintRootAuthorityUCAN produces a self-rooted authority UCAN — the
// shape a parent chain publishes, where Issuer is the trust root's
// compressed pubkey and ParentHash is nil.
func mintRootAuthorityUCAN(t *testing.T) (trustRootPub []byte, encoded []byte) {
	t.Helper()
	trustRoot := newKey(t)
	operator := newKey(t)
	now := time.Now()
	d, err := New(&trustRoot.PublicKey, &operator.PublicKey,
		[]string{string(CapAdvertise), string(CapServe), string(CapDelegate)},
		now, now.Add(24*time.Hour), 16, nil)
	require.NoError(t, err)
	require.NoError(t, d.Sign(trustRoot))
	encoded, err = d.Encode()
	require.NoError(t, err)
	return compressed(t, trustRoot), encoded
}

func TestPickIssuerFromAcceptSet_HappyPathReturnsMatchingIndex(t *testing.T) {
	trustRootPub, parentUCAN := mintRootAuthorityUCAN(t)
	otherPub := compressed(t, newKey(t))

	acceptSet := [][]byte{otherPub, trustRootPub}
	idx, err := PickIssuerFromAcceptSet(parentUCAN, acceptSet)
	require.NoError(t, err)
	require.Equal(t, 1, idx, "should return index of trustRootPub within acceptSet")
}

func TestPickIssuerFromAcceptSet_IssuerNotInAcceptSetRejects(t *testing.T) {
	_, parentUCAN := mintRootAuthorityUCAN(t)
	other1 := compressed(t, newKey(t))
	other2 := compressed(t, newKey(t))

	idx, err := PickIssuerFromAcceptSet(parentUCAN, [][]byte{other1, other2})
	require.ErrorIs(t, err, ErrPickIssuerNotInAcceptSet)
	require.Equal(t, -1, idx)
}

func TestPickIssuerFromAcceptSet_EmptyAcceptSetRejects(t *testing.T) {
	_, parentUCAN := mintRootAuthorityUCAN(t)

	idx, err := PickIssuerFromAcceptSet(parentUCAN, nil)
	require.ErrorIs(t, err, ErrPickIssuerEmptyAcceptSet)
	require.Equal(t, -1, idx)
}

func TestPickIssuerFromAcceptSet_InvalidAcceptSetEntryRejects(t *testing.T) {
	trustRootPub, parentUCAN := mintRootAuthorityUCAN(t)
	badLength := []byte{0x02, 0x03}

	idx, err := PickIssuerFromAcceptSet(parentUCAN, [][]byte{trustRootPub, badLength})
	require.ErrorIs(t, err, ErrPickIssuerAcceptSetEntryInvalid)
	require.Equal(t, -1, idx)
}

func TestPickIssuerFromAcceptSet_NonRootUCANRejects(t *testing.T) {
	// A non-root delegation (ParentHash != nil) is not the parent's
	// authority. The picker refuses it — caller must walk to root
	// first if the UCAN chain is nested.
	trustRootPub, parentUCAN := mintRootAuthorityUCAN(t)

	// Build a child delegation with the parent authority as its parent.
	child := newKey(t)
	grandchild := newKey(t)
	now := time.Now()
	nested, err := New(&child.PublicKey, &grandchild.PublicKey,
		[]string{string(CapAdvertise)},
		now, now.Add(time.Hour), 8, parentUCAN)
	require.NoError(t, err)
	require.NoError(t, nested.Sign(child))
	nestedEncoded, err := nested.Encode()
	require.NoError(t, err)

	idx, err := PickIssuerFromAcceptSet(nestedEncoded, [][]byte{trustRootPub})
	require.ErrorIs(t, err, ErrPickIssuerParentNotRoot)
	require.Equal(t, -1, idx)
}

func TestPickIssuerFromAcceptSet_MalformedUCANReturnsDecodeError(t *testing.T) {
	garbage := []byte{0xff, 0xfe, 0xfd}
	idx, err := PickIssuerFromAcceptSet(garbage, [][]byte{
		compressed(t, newKey(t)),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "decode parent authority UCAN")
	require.Equal(t, -1, idx)
}
