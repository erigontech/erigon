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

package cltypes

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	ssz2 "github.com/erigontech/erigon/cl/ssz"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
)

func validSignedBuilderRequestAuth() *SignedBuilderRequestAuth {
	return &SignedBuilderRequestAuth{
		Message: &BuilderRequestAuth{Data: hexutil.Bytes("builder-auth"), Slot: 12},
	}
}

func TestBuilderRequestAuthJSONAndSSZRoundTrip(t *testing.T) {
	want := &BuilderRequestAuth{Data: hexutil.Bytes{0x12, 0x34}, Slot: 42}

	encodedJSON, err := json.Marshal(want)
	require.NoError(t, err)
	require.JSONEq(t, `{"data":"0x1234","slot":"42"}`, string(encodedJSON))
	var fromJSON BuilderRequestAuth
	require.NoError(t, json.Unmarshal(encodedJSON, &fromJSON))
	require.Equal(t, want, &fromJSON)

	encodedSSZ, err := want.EncodeSSZ(nil)
	require.NoError(t, err)
	var fromSSZ BuilderRequestAuth
	require.NoError(t, fromSSZ.DecodeSSZ(encodedSSZ, 0))
	require.Equal(t, want, &fromSSZ)
}

func TestBuilderRequestAuthRejectsInvalidData(t *testing.T) {
	for _, input := range []string{
		`{"data":"0x","slot":"1"}`,
		`{"data":"0x01","slot":"1","extra":true}`,
		`{"data":null,"slot":"1"}`,
		`{"slot":"1"}`,
		`{"data":"0x01"}`,
		`{"data":"0x` + strings.Repeat("ab", MaxBuilderAuthDataSize+1) + `","slot":"1"}`,
	} {
		var auth BuilderRequestAuth
		require.Error(t, json.Unmarshal([]byte(input), &auth), input)
	}

	for _, data := range [][]byte{nil, make([]byte, MaxBuilderAuthDataSize+1)} {
		auth := &BuilderRequestAuth{Data: data, Slot: 1}
		_, err := auth.EncodeSSZ(nil)
		require.Error(t, err)
	}
	var decoded BuilderRequestAuth
	require.Error(t, decoded.DecodeSSZ(make([]byte, 12), 0))
}

func TestSignedBuilderRequestAuthRejectsNilMessage(t *testing.T) {
	var auth SignedBuilderRequestAuth
	_, err := auth.EncodeSSZ(nil)
	require.Error(t, err)
	require.Error(t, json.Unmarshal([]byte(`{"message":null,"signature":"0x`+strings.Repeat("00", 96)+`"}`), &auth))
}

func TestBuilderEntryJSONAndSSZRoundTrip(t *testing.T) {
	entry := &BuilderEntry{
		URL:                 "https://builder.example",
		Auth:                validSignedBuilderRequestAuth(),
		BuilderPubkeys:      []common.Bytes48{{1}, {2}},
		MaxExecutionPayment: 20,
		MinBid:              10,
		BuilderBoostFactor:  100,
	}

	encodedJSON, err := json.Marshal(entry)
	require.NoError(t, err)
	var fromJSON BuilderEntry
	require.NoError(t, json.Unmarshal(encodedJSON, &fromJSON))
	require.Equal(t, entry, &fromJSON)

	encodedSSZ, err := entry.EncodeSSZ(nil)
	require.NoError(t, err)
	var fromSSZ BuilderEntry
	require.NoError(t, fromSSZ.DecodeSSZ(encodedSSZ, 0))
	require.Equal(t, entry, &fromSSZ)
}

func TestBuilderEntryRejectsInvalidFields(t *testing.T) {
	valid, err := json.Marshal(&BuilderEntry{
		URL:                 "https://builder.example",
		Auth:                validSignedBuilderRequestAuth(),
		BuilderPubkeys:      []common.Bytes48{},
		MaxExecutionPayment: 20,
		MinBid:              10,
		BuilderBoostFactor:  100,
	})
	require.NoError(t, err)

	for _, mutate := range []func(map[string]any){
		func(v map[string]any) { v["url"] = "" },
		func(v map[string]any) { v["url"] = strings.Repeat("x", MaxBuilderURLSize+1) },
		func(v map[string]any) { v["auth"] = nil },
		func(v map[string]any) { delete(v, "builder_pubkeys") },
		func(v map[string]any) { v["builder_pubkeys"] = make([]common.Bytes48, MaxBuilderPubkeys+1) },
		func(v map[string]any) { v["unexpected"] = true },
	} {
		var value map[string]any
		require.NoError(t, json.Unmarshal(valid, &value))
		mutate(value)
		input, err := json.Marshal(value)
		require.NoError(t, err)
		var entry BuilderEntry
		require.Error(t, json.Unmarshal(input, &entry), string(input))
	}
}

func TestBuilderConfigAndPreferencesLimits(t *testing.T) {
	configJSON := `{"min_bid":"0","builder_boost_factor":"100","builders":[]}`
	var config BuilderConfig
	require.NoError(t, json.Unmarshal([]byte(configJSON), &config))
	encoded, err := config.EncodeSSZ(nil)
	require.NoError(t, err)
	var decoded BuilderConfig
	require.NoError(t, decoded.DecodeSSZ(encoded, 0))
	require.Equal(t, &config, &decoded)

	config.Builders = make([]*BuilderEntry, MaxBuilderEntries+1)
	_, err = config.EncodeSSZ(nil)
	require.Error(t, err)

	prefs := &BuilderPreferencesRequest{
		Preferences: &BuilderPreferences{MaxExecutionPayment: 11},
		Auth:        validSignedBuilderRequestAuth(),
	}
	encoded, err = prefs.EncodeSSZ(nil)
	require.NoError(t, err)
	var decodedPrefs BuilderPreferencesRequest
	require.NoError(t, decodedPrefs.DecodeSSZ(encoded, 0))
	require.Equal(t, prefs, &decodedPrefs)
	require.Error(t, json.Unmarshal([]byte(`{"preferences":null,"auth":null}`), &decodedPrefs))
}

func TestBuilderConfigStructuralSSZDecodeIsolatesSemanticEntryErrors(t *testing.T) {
	entries := []*rawBuilderEntry{
		{entry: &BuilderEntry{URL: "ftp:////builder.example", Auth: &SignedBuilderRequestAuth{Message: &BuilderRequestAuth{Data: []byte("auth"), Slot: 12}}}},
		{entry: &BuilderEntry{URL: "https://empty-auth.example", Auth: &SignedBuilderRequestAuth{Message: &BuilderRequestAuth{Slot: 12}}}},
		{entry: &BuilderEntry{URL: "https://builder.example", Auth: validSignedBuilderRequestAuth()}},
	}
	encoded, err := ssz2.MarshalSSZ(nil, uint64(0), uint64(100), &rawBuilderEntryList{values: &entries})
	require.NoError(t, err)

	var strict BuilderConfig
	require.Error(t, strict.DecodeSSZStrict(encoded, 0))
	var structural BuilderConfig
	require.NoError(t, structural.DecodeSSZStrictStructural(encoded, 0))
	require.Len(t, structural.Builders, 3)
	require.Error(t, structural.Builders[0].Validate())
	require.Error(t, structural.Builders[1].Validate())
	require.NoError(t, structural.Builders[2].Validate())
}

func TestBuilderPreferencesEntryRejectsEmptyURL(t *testing.T) {
	entry := &BuilderPreferencesEntry{Auth: validSignedBuilderRequestAuth()}
	_, err := entry.EncodeSSZ(nil)
	require.Error(t, err)
}

func TestBuilderPreferencesEntriesJSONAndSSZ(t *testing.T) {
	want := BuilderPreferencesEntries{
		&BuilderPreferencesEntry{
			ProposerPubkey:      common.Bytes48{1},
			URL:                 "https://builder.example",
			Auth:                validSignedBuilderRequestAuth(),
			MaxExecutionPayment: 15,
		},
	}
	encodedJSON, err := json.Marshal(want)
	require.NoError(t, err)
	var fromJSON BuilderPreferencesEntries
	require.NoError(t, json.Unmarshal(encodedJSON, &fromJSON))
	require.Equal(t, want, fromJSON)

	encodedSSZ, err := want.EncodeSSZ(nil)
	require.NoError(t, err)
	var fromSSZ BuilderPreferencesEntries
	require.NoError(t, fromSSZ.DecodeSSZ(encodedSSZ, 0))
	require.Equal(t, want, fromSSZ)

	tooMany := make(BuilderPreferencesEntries, MaxBuilderPreferencesEntries+1)
	_, err = tooMany.EncodeSSZ(nil)
	require.Error(t, err)
	require.Error(t, json.Unmarshal([]byte("null"), &fromJSON))
}
