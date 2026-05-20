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
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestJSONView_RootDelegation(t *testing.T) {
	issuer := newKey(t)
	audience := newKey(t)
	d, err := New(&issuer.PublicKey, &audience.PublicKey,
		[]string{string(CapAdvertise), string(CapServe)},
		time.Time{},
		time.Unix(1893456000, 0),
		2, nil)
	require.NoError(t, err)
	require.NoError(t, d.Sign(issuer))

	raw, err := json.Marshal(d)
	require.NoError(t, err)
	s := string(raw)

	require.Contains(t, s, `"version":2`)
	require.Contains(t, s, `"capabilities":[`)
	require.Contains(t, s, `"snapshot:advertise"`)
	require.Contains(t, s, `"snapshot:serve"`)
	require.Contains(t, s, `"notBefore":"immediately"`)
	require.Contains(t, s, `"expires":"2030-01-01T00:00:00Z"`)
	require.Contains(t, s, `"depthCap":2`)
	require.NotContains(t, s, `"parentHash"`,
		"root delegation should not emit a parentHash field")
}

func TestJSONView_ParentRendersAsHash(t *testing.T) {
	root := newKey(t)
	mid := newKey(t)
	leaf := newKey(t)

	rootDel, err := New(&root.PublicKey, &mid.PublicKey,
		[]string{string(CapAdvertise), string(CapDelegate)},
		time.Time{}, time.Time{}, 3, nil)
	require.NoError(t, err)
	require.NoError(t, rootDel.Sign(root))
	rootEnc, err := rootDel.Encode()
	require.NoError(t, err)

	midDel, err := New(&mid.PublicKey, &leaf.PublicKey,
		[]string{string(CapAdvertise)},
		time.Time{}, time.Time{}, 2, rootEnc)
	require.NoError(t, err)
	require.NoError(t, midDel.Sign(mid))

	raw, err := json.Marshal(midDel)
	require.NoError(t, err)
	s := string(raw)

	// v2 stores the parent by hash; the JSON view renders it as a hex
	// string, not a nested inline parent object.
	require.Contains(t, s, `"depthCap":2`)
	require.NotContains(t, s, `"parent":{`,
		"v2 delegations no longer unfold the parent chain inline")
	require.Contains(t, s, `"parentHash":"`+hex.EncodeToString(HashOf(rootEnc))+`"`,
		"mid delegation should expose its parent as a hex hash")
	// Only the mid delegation renders — exactly one version stamp.
	require.Equal(t, 1, strings.Count(s, `"version":2`),
		"only the mid delegation renders; the parent is referenced by hash")
}

func TestJSONView_IndefiniteExpirySentinel(t *testing.T) {
	issuer := newKey(t)
	audience := newKey(t)
	d, err := New(&issuer.PublicKey, &audience.PublicKey,
		[]string{string(CapServe)}, time.Time{}, time.Time{}, 0, nil)
	require.NoError(t, err)
	require.NoError(t, d.Sign(issuer))

	raw, err := json.Marshal(d)
	require.NoError(t, err)
	require.Contains(t, string(raw), `"expires":"indefinite"`)
}
