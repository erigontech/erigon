// Copyright 2024 The Erigon Authors
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

package cltypes

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

func newHezeBid(t *testing.T) *ExecutionPayloadBid {
	t.Helper()
	body := NewBeaconBody(&clparams.MainnetBeaconConfig, clparams.HezeVersion)
	bid := body.SignedExecutionPayloadBid.Message
	require.NotNil(t, bid.InclusionListBits)
	require.NoError(t, bid.InclusionListBits.SetBitAt(3, true))
	bid.Slot = 7
	return bid
}

func TestHezeBidSSZ(t *testing.T) {
	bid := newHezeBid(t)
	encoded, err := bid.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Equal(t, bid.EncodingSizeSSZ(), len(encoded))
	require.Equal(t, []byte{0x08, 0x00}, encoded[len(encoded)-2:])

	decoded := &ExecutionPayloadBid{}
	require.NoError(t, decoded.DecodeSSZ(encoded, int(clparams.HezeVersion)))
	require.True(t, decoded.InclusionListBits.GetBitAt(3))
	require.Equal(t, uint64(7), decoded.Slot)

	root, err := bid.HashSSZ()
	require.NoError(t, err)
	decodedRoot, err := decoded.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, root, decodedRoot)

	empty := bid.Copy()
	empty.InclusionListBits = solid.NewBitVector(16)
	emptyRoot, err := empty.HashSSZ()
	require.NoError(t, err)
	require.NotEqual(t, root, emptyRoot)

	gloas := bid.Copy()
	gloas.InclusionListBits = nil
	gloasEncoded, err := gloas.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Equal(t, len(gloasEncoded)+2, len(encoded))
	gloasDecoded := &ExecutionPayloadBid{}
	require.NoError(t, gloasDecoded.DecodeSSZ(gloasEncoded, int(clparams.GloasVersion)))
	require.Nil(t, gloasDecoded.InclusionListBits)
}

func TestHezeBidCopyKeepsInclusionListBits(t *testing.T) {
	bid := newHezeBid(t)
	copied := bid.Copy()
	require.True(t, copied.InclusionListBits.GetBitAt(3))
	require.NoError(t, copied.InclusionListBits.SetBitAt(4, true))
	require.False(t, bid.InclusionListBits.GetBitAt(4))
}

func TestHezeBeaconBodyDropsEth1Fields(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	gloas := NewBeaconBody(cfg, clparams.GloasVersion)
	heze := NewBeaconBody(cfg, clparams.HezeVersion)

	gloasEncoded, err := gloas.EncodeSSZ(nil)
	require.NoError(t, err)
	encoded, err := heze.EncodeSSZ(nil)
	require.NoError(t, err)
	// eth1_data (72 bytes) and the deposits offset (4 bytes) are gone, inclusion_list_bits adds 2 bytes.
	require.Equal(t, len(gloasEncoded)-72-4+2, len(encoded))

	root, err := heze.HashSSZ()
	require.NoError(t, err)
	heze.Eth1Data = &Eth1Data{DepositCount: 5}
	heze.rootCache = nil
	changedRoot, err := heze.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, root, changedRoot)

	gloasRoot, err := gloas.HashSSZ()
	require.NoError(t, err)
	require.NotEqual(t, root, gloasRoot)

	changedEncoded, err := heze.EncodeSSZ(nil)
	require.NoError(t, err)
	require.Equal(t, encoded, changedEncoded)

	for _, decode := range []func(*BeaconBody) error{
		func(b *BeaconBody) error { return b.DecodeSSZ(encoded, int(clparams.HezeVersion)) },
		func(b *BeaconBody) error { return b.DecodeSSZStrict(encoded, int(clparams.HezeVersion)) },
	} {
		decoded := NewBeaconBody(cfg, clparams.HezeVersion)
		require.NoError(t, decode(decoded))
		decodedRoot, err := decoded.HashSSZ()
		require.NoError(t, err)
		require.Equal(t, root, decodedRoot)
	}
}

func TestHezeBeaconBodyJSON(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	body := NewBeaconBody(cfg, clparams.HezeVersion)
	require.NoError(t, body.SignedExecutionPayloadBid.Message.InclusionListBits.SetBitAt(1, true))

	encoded, err := json.Marshal(body)
	require.NoError(t, err)
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(encoded, &fields))
	require.NotContains(t, fields, "eth1_data")
	require.NotContains(t, fields, "deposits")
	require.Contains(t, string(fields["signed_execution_payload_bid"]), `"inclusion_list_bits":"0x0200"`)

	decoded := NewBeaconBody(cfg, clparams.HezeVersion)
	require.NoError(t, json.Unmarshal(encoded, decoded))
	want, err := body.HashSSZ()
	require.NoError(t, err)
	got, err := decoded.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, want, got)

	gloasEncoded, err := json.Marshal(NewBeaconBody(cfg, clparams.GloasVersion))
	require.NoError(t, err)
	require.Contains(t, string(gloasEncoded), `"eth1_data"`)
	require.NotContains(t, string(gloasEncoded), "inclusion_list_bits")
}

func marshalBodyFields(t *testing.T, version clparams.StateVersion) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(NewBeaconBody(&clparams.MainnetBeaconConfig, version))
	require.NoError(t, err)
	var fields map[string]any
	require.NoError(t, json.Unmarshal(encoded, &fields))
	return fields
}

func unmarshalBody(t *testing.T, version clparams.StateVersion, fields map[string]any) error {
	t.Helper()
	encoded, err := json.Marshal(fields)
	require.NoError(t, err)
	return json.Unmarshal(encoded, NewBeaconBody(&clparams.MainnetBeaconConfig, version))
}

func TestBeaconBodyJSONInclusionListBitsFollowFork(t *testing.T) {
	bidMessage := func(fields map[string]any) map[string]any {
		return fields["signed_execution_payload_bid"].(map[string]any)["message"].(map[string]any)
	}

	gloas := marshalBodyFields(t, clparams.GloasVersion)
	require.NoError(t, unmarshalBody(t, clparams.GloasVersion, gloas))
	bidMessage(gloas)["inclusion_list_bits"] = "0x0000"
	require.Error(t, unmarshalBody(t, clparams.GloasVersion, gloas))

	heze := marshalBodyFields(t, clparams.HezeVersion)
	require.NoError(t, unmarshalBody(t, clparams.HezeVersion, heze))
	bidMessage(heze)["inclusion_list_bits"] = nil
	require.Error(t, unmarshalBody(t, clparams.HezeVersion, heze))
	delete(bidMessage(heze), "inclusion_list_bits")
	require.Error(t, unmarshalBody(t, clparams.HezeVersion, heze))
}

func TestHezeBeaconBodyJSONRejectsRemovedFields(t *testing.T) {
	for name, value := range map[string]any{
		"eth1_data": map[string]any{"deposit_root": "0x" + strings.Repeat("00", 32), "deposit_count": "9", "block_hash": "0x" + strings.Repeat("00", 32)},
		"deposits":  []any{},
	} {
		fields := marshalBodyFields(t, clparams.HezeVersion)
		fields[name] = value
		require.Error(t, unmarshalBody(t, clparams.HezeVersion, fields), name)
	}
	require.NoError(t, unmarshalBody(t, clparams.HezeVersion, marshalBodyFields(t, clparams.HezeVersion)))
}
