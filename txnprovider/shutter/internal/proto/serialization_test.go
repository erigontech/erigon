// Copyright 2025 The Erigon Authors
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

package proto_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	shutterproto "github.com/erigontech/erigon/txnprovider/shutter/internal/proto"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/testhelpers"
)

func TestUnmarshallDecryptionKeys(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		data    []byte
		wantErr error
	}{
		{
			name: "invalid envelope version",
			data: testhelpers.TestMustMockDecryptionKeysEnvelopeData(t, testhelpers.MockDecryptionKeysEnvelopeDataOptions{
				Version: "XXX",
			}),
			wantErr: shutterproto.ErrEnveloperVersionMismatch,
		},
		{
			name: "empty envelope message",
			data: func(t *testing.T) []byte {
				data, err := proto.Marshal(&shutterproto.Envelope{
					Version: shutterproto.EnvelopeVersion,
				})
				require.NoError(t, err)
				return data
			}(t),
			wantErr: shutterproto.ErrEmptyEnvelope,
		},
		{
			name:    "invalid data bytes",
			data:    []byte("invalid"),
			wantErr: shutterproto.ErrProtoUnmarshall,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, haveErr := shutterproto.UnmarshallDecryptionKeys(tc.data)
			require.ErrorIs(t, haveErr, tc.wantErr)
		})
	}
}
