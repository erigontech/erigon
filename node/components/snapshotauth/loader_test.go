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
	"crypto/elliptic"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoadOrGenerateDelegation_ConfiguredPathReadsExisting(t *testing.T) {
	dir := t.TempDir()

	signer := newKey(t)
	d, err := New(&signer.PublicKey, &signer.PublicKey,
		[]string{string(CapAdvertise)},
		time.Time{}, time.Time{}, 1, nil)
	require.NoError(t, err)
	require.NoError(t, d.Sign(signer))
	encoded, err := d.Encode()
	require.NoError(t, err)

	path := filepath.Join(dir, "explicit.ucan")
	require.NoError(t, os.WriteFile(path, encoded, 0o600))

	got, err := LoadOrGenerateDelegation(path, "", nil, nil)
	require.NoError(t, err)
	require.Equal(t, encoded, got)
}

func TestLoadOrGenerateDelegation_ConfiguredPathMissingErrors(t *testing.T) {
	_, err := LoadOrGenerateDelegation("/nonexistent/path/snapshot.ucan", "", nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "reading snapshot delegation")
}

func TestLoadOrGenerateDelegation_ConfiguredPathCorruptErrors(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "corrupt.ucan")
	require.NoError(t, os.WriteFile(path, []byte("not-a-valid-cbor"), 0o600))

	_, err := LoadOrGenerateDelegation(path, "", nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "decoding snapshot delegation")
}

func TestLoadOrGenerateDelegation_DefaultPathReadsExisting(t *testing.T) {
	datadir := t.TempDir()

	signer := newKey(t)
	d, err := New(&signer.PublicKey, &signer.PublicKey,
		[]string{string(CapServe)},
		time.Time{}, time.Time{}, 1, nil)
	require.NoError(t, err)
	require.NoError(t, d.Sign(signer))
	encoded, err := d.Encode()
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(
		filepath.Join(datadir, DefaultDelegationFileName), encoded, 0o600))

	got, err := LoadOrGenerateDelegation("", datadir, nil, nil)
	require.NoError(t, err)
	require.Equal(t, encoded, got)
}

func TestLoadOrGenerateDelegation_DefaultPathGeneratesBootstrap(t *testing.T) {
	datadir := t.TempDir()
	nodeKey := newKey(t)

	got, err := LoadOrGenerateDelegation("", datadir, nodeKey, nil)
	require.NoError(t, err)
	require.NotEmpty(t, got)

	// The file is now on disk.
	fromDisk, err := os.ReadFile(filepath.Join(datadir, DefaultDelegationFileName))
	require.NoError(t, err)
	require.Equal(t, got, fromDisk)

	// Decoded shape: self-signed (issuer == audience == nodeKey),
	// indefinite expiry, all caps, BootstrapDepthCap.
	d, err := Decode(got)
	require.NoError(t, err)

	wantPub := elliptic.MarshalCompressed(nodeKey.PublicKey.Curve,
		nodeKey.PublicKey.X, nodeKey.PublicKey.Y)
	require.Equal(t, wantPub, d.Issuer)
	require.Equal(t, wantPub, d.Audience)
	require.Equal(t, []string{
		string(CapAdvertise),
		string(CapDelegate),
		string(CapServe),
	}, d.Capabilities)
	require.Equal(t, int64(0), d.Expires, "indefinite")
	require.Equal(t, BootstrapDepthCap, d.DepthCap)
	require.NoError(t, d.VerifySignature())

	// Second call reads the file we just wrote — does not regenerate.
	got2, err := LoadOrGenerateDelegation("", datadir, nodeKey, nil)
	require.NoError(t, err)
	require.Equal(t, got, got2, "second call must be idempotent — read existing, not regenerate")
}

func TestLoadOrGenerateDelegation_DefaultPathNoNodeKeyReturnsNil(t *testing.T) {
	datadir := t.TempDir()

	got, err := LoadOrGenerateDelegation("", datadir, nil, nil)
	require.NoError(t, err)
	require.Nil(t, got, "no key + no file → nil bytes (publisher runs V2-only)")

	// File MUST NOT have been created.
	_, statErr := os.Stat(filepath.Join(datadir, DefaultDelegationFileName))
	require.True(t, os.IsNotExist(statErr),
		"loader must not write a file when nodeKey is nil")
}

func TestLoadOrGenerateDelegation_DefaultPathCorruptErrors(t *testing.T) {
	datadir := t.TempDir()
	require.NoError(t, os.WriteFile(
		filepath.Join(datadir, DefaultDelegationFileName),
		[]byte("garbage"), 0o600))

	nodeKey := newKey(t)
	_, err := LoadOrGenerateDelegation("", datadir, nodeKey, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "decoding snapshot delegation")
}

func TestLoadOrGenerateDelegation_NoDatadirAndNoPathReturnsNil(t *testing.T) {
	got, err := LoadOrGenerateDelegation("", "", newKey(t), nil)
	require.NoError(t, err)
	require.Nil(t, got, "no path AND no datadir → ad-hoc invocation, no delegation")
}
