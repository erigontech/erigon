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
	"encoding/binary"
	"slices"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common/hexutil"
	commonssz "github.com/erigontech/erigon/common/ssz"
	"github.com/erigontech/erigon/execution/types"
	"github.com/stretchr/testify/require"
)

func insertSSZGap(buf []byte, insertAt int, offsetPositions ...int) []byte {
	malformed := make([]byte, len(buf)+1)
	copy(malformed[:insertAt], buf[:insertAt])
	copy(malformed[insertAt+1:], buf[insertAt:])
	for _, offsetAt := range offsetPositions {
		offset := binary.LittleEndian.Uint32(malformed[offsetAt:])
		binary.LittleEndian.PutUint32(malformed[offsetAt:], offset+1)
	}
	return malformed
}

func payloadAttributesStrictDecodeFallbackAllowed(obj commonssz.Unmarshaler) bool {
	_, ok := obj.(*cltypes.Withdrawal)
	return ok
}

func TestPayloadAttributesStrictSchemaCoverage(t *testing.T) {
	for version := clparams.BellatrixVersion; version <= clparams.GloasVersion; version++ {
		t.Run(version.String(), func(t *testing.T) {
			attributes := NewPayloadAttributesSSZ(version)
			fields := payloadAttributesDecodeFields{withdrawals: newWithdrawalList(nil)}
			schema := attributes.decodeSSZSchema(&fields)
			if version >= clparams.CapellaVersion {
				schema = append(schema, (*cltypes.Withdrawal)(nil))
			}
			for _, field := range schema {
				obj, ok := field.(commonssz.Unmarshaler)
				if !ok {
					continue
				}
				if _, ok := obj.(commonssz.StrictUnmarshaler); ok {
					continue
				}
				require.Truef(
					t,
					payloadAttributesStrictDecodeFallbackAllowed(obj),
					"%T must implement StrictUnmarshaler or be explicitly allowed",
					obj,
				)
			}
		})
	}
}

func TestExecutionPayloadDecodeSSZStrictRoundTrip(t *testing.T) {
	const version = clparams.CapellaVersion
	payload := NewExecutionPayloadSSZ(version)
	payload.ExtraData = hexutil.Bytes{0x01, 0x02}
	payload.Transactions = []hexutil.Bytes{{0x03}, {0x04, 0x05}}
	payload.Withdrawals = []*types.Withdrawal{{Index: 1, Validator: 2, Amount: 3}}

	encoded, err := payload.EncodeSSZ(nil)
	require.NoError(t, err)

	decoded := NewExecutionPayloadSSZ(version)
	require.NoError(t, decoded.DecodeSSZStrict(encoded, int(version)))

	reencoded, err := decoded.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Equal(t, encoded, reencoded)
}

func TestExecutionPayloadDecodeSSZStrictRejectsNonCanonicalOffset(t *testing.T) {
	payload := NewExecutionPayloadSSZ(clparams.BellatrixVersion)
	encoded, err := payload.EncodeSSZ(nil)
	require.NoError(t, err)

	const (
		payloadFixedSize        = 508
		extraDataOffsetPosition = 436
		txsOffsetPosition       = 504
	)
	malformed := insertSSZGap(encoded, payloadFixedSize, extraDataOffsetPosition, txsOffsetPosition)

	require.NoError(t, NewExecutionPayloadSSZ(clparams.BellatrixVersion).DecodeSSZ(malformed, int(clparams.BellatrixVersion)))
	require.ErrorIs(
		t,
		NewExecutionPayloadSSZ(clparams.BellatrixVersion).DecodeSSZStrict(malformed, int(clparams.BellatrixVersion)),
		commonssz.ErrBadOffset,
	)
}

func TestExecutionPayloadDecodeSSZStrictRejectsOversizedExtraData(t *testing.T) {
	const version = clparams.BellatrixVersion
	payload := NewExecutionPayloadSSZ(version)
	payload.ExtraData = make(hexutil.Bytes, 32)
	encoded, err := payload.EncodeSSZ(nil)
	require.NoError(t, err)

	const txsOffsetPosition = 504
	malformed := insertSSZGap(encoded, len(encoded), txsOffsetPosition)

	require.NoError(t, NewExecutionPayloadSSZ(version).DecodeSSZ(malformed, int(version)))
	require.ErrorIs(t, NewExecutionPayloadSSZ(version).DecodeSSZStrict(malformed, int(version)), commonssz.ErrTooBigList)
}

func TestPayloadAttributesDecodeSSZStrictRejectsTrailingBytes(t *testing.T) {
	attributes := NewPayloadAttributesSSZ(clparams.BellatrixVersion)
	encoded, err := attributes.EncodeSSZ(nil)
	require.NoError(t, err)

	malformed := slices.Concat(encoded, []byte{0xff})

	require.NoError(t, NewPayloadAttributesSSZ(clparams.BellatrixVersion).DecodeSSZ(malformed, int(clparams.BellatrixVersion)))
	require.ErrorIs(
		t,
		NewPayloadAttributesSSZ(clparams.BellatrixVersion).DecodeSSZStrict(malformed, int(clparams.BellatrixVersion)),
		commonssz.ErrTrailingBytes,
	)
}

func TestPayloadAttributesDecodeSSZStrictRejectsNonCanonicalOffset(t *testing.T) {
	attributes := NewPayloadAttributesSSZ(clparams.CapellaVersion)
	encoded, err := attributes.EncodeSSZ(nil)
	require.NoError(t, err)

	const (
		attributesFixedSize       = 64
		withdrawalsOffsetPosition = 60
	)
	malformed := insertSSZGap(encoded, attributesFixedSize, withdrawalsOffsetPosition)

	require.NoError(t, NewPayloadAttributesSSZ(clparams.CapellaVersion).DecodeSSZ(malformed, int(clparams.CapellaVersion)))
	require.ErrorIs(
		t,
		NewPayloadAttributesSSZ(clparams.CapellaVersion).DecodeSSZStrict(malformed, int(clparams.CapellaVersion)),
		commonssz.ErrBadOffset,
	)
}
