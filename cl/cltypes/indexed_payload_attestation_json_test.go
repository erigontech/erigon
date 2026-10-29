package cltypes

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
)

func TestIndexedPayloadAttestationJSONLimits(t *testing.T) {
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
			if limit == 512 {
				var zero IndexedPayloadAttestation
				require.NoError(t, json.Unmarshal(data, &zero))
				require.Equal(t, limit, zero.AttestingIndices.Cap())
				root, err = zero.HashSSZ()
				require.NoError(t, err)
				require.Equal(t, expected, root)
			}
			for _, size := range []int{0, limit, limit + 1} {
				values := make([]string, size)
				for i := range values {
					values[i] = "1"
				}
				payload, err := json.Marshal(map[string]any{"attesting_indices": values, "data": source.Data, "signature": source.Signature})
				require.NoError(t, err)
				err = json.Unmarshal(payload, decoded)
				if size > limit {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
					require.Equal(t, limit, decoded.AttestingIndices.Cap())
					require.Equal(t, size, decoded.AttestingIndices.Length())
				}
			}
		})
	}
}

func TestIndexedPayloadAttestationJSONRejectsIncompleteObject(t *testing.T) {
	for _, data := range []string{`null`, `{}`, `{"attesting_indices":null,"data":{}}`, `{"attesting_indices":[],"data":null}`, `{"data":{}}`, `{"attesting_indices":[]}`} {
		t.Run(data, func(t *testing.T) {
			var decoded IndexedPayloadAttestation
			require.Error(t, json.Unmarshal([]byte(data), &decoded))
		})
	}
}
