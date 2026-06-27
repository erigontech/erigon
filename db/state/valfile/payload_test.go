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

package valfile

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPayloadInlineRoundTrip(t *testing.T) {
	t.Parallel()
	v := []byte("small-branch")
	payload := EncodeInline(nil, v)
	// inline must not consult the value-file
	failGet := func(Handle, []byte) ([]byte, error) { return nil, errors.New("get must not be called for inline") }
	got, err := DecodePayload(payload, nil, failGet)
	require.NoError(t, err)
	require.Equal(t, v, got)
}

func TestPayloadExternalRoundTrip(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "v1.0-commitment.0-1.cvl")
	w, err := NewWriter(path)
	require.NoError(t, err)
	w.DisableFsync()
	v := []byte("a large branch value that lives in the file")
	h, err := w.Append(v)
	require.NoError(t, err)
	require.NoError(t, w.Sync())
	require.NoError(t, w.Close())

	payload := EncodeExternal(nil, h)

	r, err := OpenReader(path)
	require.NoError(t, err)
	defer r.Close()
	got, err := DecodePayload(payload, nil, r.Get)
	require.NoError(t, err)
	require.Equal(t, v, got)
}

func TestPayloadInlinePreservesPrefix(t *testing.T) {
	t.Parallel()
	step := []byte{0, 0, 0, 0, 0, 0, 0, 9} // ^step prefix lives in front of the payload
	v := []byte("x")
	full := EncodeInline(append([]byte{}, step...), v)
	require.Equal(t, step, full[:8])
	got, err := DecodePayload(full[8:], nil, nil)
	require.NoError(t, err)
	require.Equal(t, v, got)
}

func TestDecodePayloadEmptyErrors(t *testing.T) {
	t.Parallel()
	_, err := DecodePayload(nil, nil, nil)
	require.Error(t, err)
}
