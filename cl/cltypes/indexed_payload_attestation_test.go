package cltypes

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
)

func TestIndexedPayloadAttestationPreservesLimit(t *testing.T) {
	for _, limit := range []int{16, 32, 512} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			cfg := clparams.MainnetBeaconConfig
			cfg.PtcSize = uint64(limit)
			source := NewIndexedPayloadAttestation(&cfg)
			require.Equal(t, limit, source.AttestingIndices.Cap())
			for _, index := range []uint64{1, 2, 3} {
				source.AttestingIndices.Append(index)
			}
			source.Data.Slot = 42
			source.Signature[0] = 9
			roots := map[int]string{
				16:  "ed114baf42aac42d5c115ed017862e26138544d8e8fbd9b58466da9dfa0b2f55",
				32:  "eac541ed75add596f34e7d491f512397ad78e73126db30505ac611ea7eeca09c",
				512: "f273bb09e1c6e64f67d52646a780c2a19a2ef6621e724b4ed7982ca505c9bada",
			}
			listRoot, err := source.AttestingIndices.HashSSZ()
			require.NoError(t, err)
			require.Equal(t, common.HexToHash(roots[limit]), common.Hash(listRoot))
			expected, err := source.HashSSZ()
			require.NoError(t, err)
			encoded, err := source.EncodeSSZ(nil)
			require.NoError(t, err)
			t.Run("decode", func(t *testing.T) {
				cfg := clparams.MainnetBeaconConfig
				cfg.PtcSize = uint64(limit)
				decoded := NewIndexedPayloadAttestation(&cfg)
				require.Equal(t, limit, decoded.AttestingIndices.Cap())
				require.NoError(t, decoded.DecodeSSZ(encoded, int(clparams.GloasVersion)))
				root, err := decoded.HashSSZ()
				require.NoError(t, err)
				require.Equal(t, expected, root)
				require.Equal(t, limit, decoded.AttestingIndices.Cap())
			})
			t.Run("clone", func(t *testing.T) {
				cloned := source.Clone().(*IndexedPayloadAttestation)
				root, err := cloned.HashSSZ()
				require.NoError(t, err)
				require.Equal(t, expected, root)
				require.Equal(t, limit, cloned.AttestingIndices.Cap())
				cloned.AttestingIndices.Set(0, 99)
				cloned.Data.Slot++
				require.Equal(t, uint64(1), source.AttestingIndices.Get(0))
				require.Equal(t, uint64(42), source.Data.Slot)
			})
		})
	}
}

func TestIndexedPayloadAttestationDecodeLimit(t *testing.T) {
	for _, limit := range []int{16, 32, 512} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			for _, size := range []int{0, limit, limit + 1} {
				source := NewIndexedPayloadAttestation(nil)
				source.AttestingIndices = solid.NewRawUint64List(limit+1, make([]uint64, size))
				encoded, err := source.EncodeSSZ(nil)
				require.NoError(t, err)
				cfg := clparams.MainnetBeaconConfig
				cfg.PtcSize = uint64(limit)
				decoded := NewIndexedPayloadAttestation(&cfg)
				require.Equal(t, limit, decoded.AttestingIndices.Cap())
				err = decoded.DecodeSSZ(encoded, int(clparams.GloasVersion))
				if size > limit {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			}
		})
	}
}

func TestIndexedPayloadAttestationDecodeMainnetDefault(t *testing.T) {
	source := NewIndexedPayloadAttestation(nil)
	source.AttestingIndices.Append(1)
	encoded, err := source.EncodeSSZ(nil)
	require.NoError(t, err)
	var decoded IndexedPayloadAttestation
	require.NoError(t, decoded.DecodeSSZ(encoded, int(clparams.GloasVersion)))
	require.Equal(t, int(clparams.MainnetBeaconConfig.PtcSize), decoded.AttestingIndices.Cap())
	expected, err := source.HashSSZ()
	require.NoError(t, err)
	root, err := decoded.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, expected, root)
}

func TestIndexedPayloadAttestationEmptyConfig(t *testing.T) {
	att := NewIndexedPayloadAttestation(&clparams.BeaconChainConfig{})
	require.Equal(t, int(clparams.MaxPtcSize), att.AttestingIndices.Cap())
	root, err := att.HashSSZ()
	require.NoError(t, err)
	expected, err := NewIndexedPayloadAttestation(nil).HashSSZ()
	require.NoError(t, err)
	require.Equal(t, expected, root)
}

func TestIndexedPayloadAttestationDecodeZeroLimit(t *testing.T) {
	source := NewIndexedPayloadAttestation(nil)
	source.AttestingIndices.Append(1)
	encoded, err := source.EncodeSSZ(nil)
	require.NoError(t, err)
	expected, err := source.HashSSZ()
	require.NoError(t, err)

	decoded := &IndexedPayloadAttestation{AttestingIndices: solid.NewRawUint64List(0, nil)}
	require.NoError(t, decoded.DecodeSSZ(encoded, int(clparams.GloasVersion)))
	require.Equal(t, int(clparams.MaxPtcSize), decoded.AttestingIndices.Cap())
	root, err := decoded.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, expected, root)
}

func TestIndexedPayloadAttestationCloneZeroLimit(t *testing.T) {
	source := IndexedPayloadAttestation{AttestingIndices: solid.NewRawUint64List(0, []uint64{1}), Data: &PayloadAttestationData{}}
	require.Equal(t, 0, source.AttestingIndices.Cap())
	cloned := source.Clone().(*IndexedPayloadAttestation)
	require.Equal(t, int(clparams.MaxPtcSize), cloned.AttestingIndices.Cap())
	expected := NewIndexedPayloadAttestation(nil)
	expected.AttestingIndices.Append(1)
	expectedRoot, err := expected.HashSSZ()
	require.NoError(t, err)
	root, err := cloned.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, expectedRoot, root)
	require.Equal(t, 0, source.AttestingIndices.Cap())
}
