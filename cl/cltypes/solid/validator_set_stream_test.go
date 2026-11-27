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

package solid

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/ssz"
)

var streamTestCounts = []int{0, 1, 84, 85, 86, 255, 256, 257, 300, 1365}

func TestValidatorSetEncodeSSZToMatchesEncodeSSZ(t *testing.T) {
	for _, progressive := range []bool{false, true} {
		for _, count := range streamTestCounts {
			t.Run(fmt.Sprint(progressive, count), func(t *testing.T) {
				set := allTestSet(t, count, progressive).set
				want, err := set.EncodeSSZ(nil)
				require.NoError(t, err)
				var got bytes.Buffer
				require.NoError(t, set.EncodeSSZTo(&got))
				require.Equal(t, want, got.Bytes())
			})
		}
	}
}

type failingWriter struct{ err error }

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }

func TestValidatorSetEncodeSSZToReturnsWriterError(t *testing.T) {
	set := allTestSet(t, 10, false).set
	sentinel := fmt.Errorf("disk full")
	require.ErrorIs(t, set.EncodeSSZTo(failingWriter{sentinel}), sentinel)
}

func TestValidatorSetDecodeSSZFromMatchesDecodeSSZ(t *testing.T) {
	for _, progressive := range []bool{false, true} {
		for _, count := range streamTestCounts {
			t.Run(fmt.Sprint(progressive, count), func(t *testing.T) {
				encoded, err := allTestSet(t, count, progressive).set.EncodeSSZ(nil)
				require.NoError(t, err)

				want := NewValidatorSet(1 << 20)
				want.SetProgressiveHashing(progressive)
				require.NoError(t, want.DecodeSSZ(encoded, 0))
				got := NewValidatorSet(1 << 20)
				got.SetProgressiveHashing(progressive)
				require.NoError(t, got.DecodeSSZFrom(bytes.NewReader(encoded), len(encoded)))

				require.Equal(t, want.Length(), got.Length())
				require.Equal(t, want.phase0Data, got.phase0Data)
				require.Equal(t, want.attesterBits, got.attesterBits)
				wantRoot, err := want.hashWithLength()
				require.NoError(t, err)
				gotRoot, err := got.hashWithLength()
				require.NoError(t, err)
				require.Equal(t, wantRoot, gotRoot)
				reencoded, err := got.EncodeSSZ(nil)
				require.NoError(t, err)
				require.Equal(t, encoded, reencoded)
			})
		}
	}
}

func TestValidatorSetDecodeSSZFromRejectsBadInput(t *testing.T) {
	encoded, err := allTestSet(t, 300, false).set.EncodeSSZ(nil)
	require.NoError(t, err)

	t.Run("short input", func(t *testing.T) {
		set := NewValidatorSet(1 << 20)
		require.Error(t, set.DecodeSSZFrom(bytes.NewReader(encoded[:len(encoded)-1]), len(encoded)))
	})
	t.Run("not rounded", func(t *testing.T) {
		set := NewValidatorSet(1 << 20)
		err := set.DecodeSSZFrom(bytes.NewReader(encoded), len(encoded)-1)
		require.ErrorIs(t, err, ssz.ErrBufferNotRounded)
	})
	t.Run("over limit is rejected before reading", func(t *testing.T) {
		set := NewValidatorSet(10)
		r := &countingReader{r: bytes.NewReader(encoded)}
		require.Error(t, set.DecodeSSZFrom(r, len(encoded)))
		require.Zero(t, r.n)
		require.Zero(t, set.Length())
	})
}

type countingReader struct {
	r io.Reader
	n int
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += n
	return n, err
}
