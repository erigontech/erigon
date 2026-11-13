package cltypes

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common"
)

func TestIndexedPayloadAttestationJSONRoundTrip(t *testing.T) {
	for _, limit := range []int{16, 32, 512} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			cfg := clparams.MainnetBeaconConfig
			cfg.PtcSize = uint64(limit)
			source := NewIndexedPayloadAttestation(&cfg)
			for _, index := range []uint64{1, 2, 3} {
				source.AttestingIndices.Append(index)
			}
			source.Data.Slot = 42
			source.Signature[0] = 9
			data, err := json.Marshal(source)
			require.NoError(t, err)
			encoded, err := source.EncodeSSZ(nil)
			require.NoError(t, err)
			expected, err := source.HashSSZ()
			require.NoError(t, err)
			decoded := NewIndexedPayloadAttestation(&cfg)
			require.NoError(t, json.Unmarshal(data, decoded))
			root, err := decoded.HashSSZ()
			require.NoError(t, err)
			require.Equal(t, expected, root)
			roundtrip, err := decoded.EncodeSSZ(nil)
			require.NoError(t, err)
			require.Equal(t, encoded, roundtrip)
		})
	}
}

func TestIndexedPayloadAttestationJSONDefaultsToMainnetLimit(t *testing.T) {
	source := NewIndexedPayloadAttestation(&clparams.MainnetBeaconConfig)
	source.AttestingIndices.Append(1)
	data, err := json.Marshal(source)
	require.NoError(t, err)
	expected, err := source.HashSSZ()
	require.NoError(t, err)

	var decoded IndexedPayloadAttestation
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, 512, decoded.AttestingIndices.Cap())
	root, err := decoded.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, expected, root)
}

func TestIndexedPayloadAttestationJSONLengthBoundaries(t *testing.T) {
	for _, limit := range []int{16, 32, 512} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			cfg := clparams.MainnetBeaconConfig
			cfg.PtcSize = uint64(limit)
			for _, size := range []int{0, limit, limit + 1} {
				t.Run(fmt.Sprint(size), func(t *testing.T) {
					values := make([]string, size)
					for i := range values {
						values[i] = "1"
					}
					payload, err := json.Marshal(map[string]any{
						"attesting_indices": values,
						"data":              PayloadAttestationData{},
						"signature":         common.Bytes96{},
					})
					require.NoError(t, err)
					decoded := NewIndexedPayloadAttestation(&cfg)
					err = json.Unmarshal(payload, decoded)
					if size > limit {
						require.Error(t, err)
						return
					}
					require.NoError(t, err)
					require.Equal(t, limit, decoded.AttestingIndices.Cap())
					require.Equal(t, size, decoded.AttestingIndices.Length())
				})
			}
		})
	}
}

func TestIndexedPayloadAttestationJSONRejectsIncompleteObject(t *testing.T) {
	signature, err := json.Marshal(common.Bytes96{})
	require.NoError(t, err)
	for _, tc := range []struct {
		name string
		data string
	}{
		{"null object", `null`},
		{"empty object", `{}`},
		{"missing indices", `{"data":{},"signature":` + string(signature) + `}`},
		{"null indices", `{"attesting_indices":null,"data":{},"signature":` + string(signature) + `}`},
		{"missing data", `{"attesting_indices":[],"signature":` + string(signature) + `}`},
		{"null data", `{"attesting_indices":[],"data":null,"signature":` + string(signature) + `}`},
		{"missing signature", `{"attesting_indices":[],"data":{}}`},
		{"null signature", `{"attesting_indices":[],"data":{},"signature":null}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var decoded IndexedPayloadAttestation
			require.Error(t, json.Unmarshal([]byte(tc.data), &decoded))
		})
	}
}
