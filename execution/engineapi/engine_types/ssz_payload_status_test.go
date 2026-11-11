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

package engine_types

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

func TestPayloadStatusSSZValidationErrorTruncation(t *testing.T) {
	t.Parallel()

	ascii := func(n int) string { return strings.Repeat("a", n) }

	tests := []struct {
		name string
		msg  string
		want string
	}{
		{
			name: "ascii below limit",
			msg:  ascii(sszMaxValidationError - 1),
			want: ascii(sszMaxValidationError - 1),
		},
		{
			name: "ascii at limit",
			msg:  ascii(sszMaxValidationError),
			want: ascii(sszMaxValidationError),
		},
		{
			name: "ascii above limit",
			msg:  ascii(sszMaxValidationError + 10),
			want: ascii(sszMaxValidationError),
		},
		{
			name: "two byte character crossing the limit",
			msg:  ascii(sszMaxValidationError-1) + "é",
			want: ascii(sszMaxValidationError - 1),
		},
		{
			name: "three byte character crossing the limit",
			msg:  ascii(sszMaxValidationError-2) + "€",
			want: ascii(sszMaxValidationError - 2),
		},
		{
			name: "four byte character crossing the limit",
			msg:  ascii(sszMaxValidationError-1) + "\U0001F600",
			want: ascii(sszMaxValidationError - 1),
		},
		{
			name: "multibyte character ending exactly at the limit",
			msg:  ascii(sszMaxValidationError-2) + "é" + "tail",
			want: ascii(sszMaxValidationError-2) + "é",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			in := &PayloadStatus{Status: InvalidStatus, ValidationError: NewStringifiedErrorFromString(tt.msg)}
			enc, err := in.EncodeSSZ(nil)
			require.NoError(t, err)

			var out PayloadStatus
			require.NoError(t, out.DecodeSSZ(enc, 0))
			require.NotNil(t, out.ValidationError)

			got := out.ValidationError.Error().Error()
			require.Equal(t, tt.want, got)
			require.LessOrEqual(t, len(got), sszMaxValidationError)
			require.True(t, utf8.ValidString(got))
		})
	}
}

func TestPayloadStatusSSZValidationErrorAbsentAndEmpty(t *testing.T) {
	t.Parallel()

	absent := &PayloadStatus{Status: ValidStatus}
	enc, err := absent.EncodeSSZ(nil)
	require.NoError(t, err)
	var out PayloadStatus
	require.NoError(t, out.DecodeSSZ(enc, 0))
	require.Nil(t, out.ValidationError)

	empty := &PayloadStatus{Status: InvalidStatus, ValidationError: NewStringifiedErrorFromString("")}
	enc, err = empty.EncodeSSZ(nil)
	require.NoError(t, err)
	out = PayloadStatus{}
	require.NoError(t, out.DecodeSSZ(enc, 0))
	require.NotNil(t, out.ValidationError)
	require.Empty(t, out.ValidationError.Error().Error())
}
