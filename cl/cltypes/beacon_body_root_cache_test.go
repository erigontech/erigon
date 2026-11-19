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
	"bytes"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
)

func TestGloasBodyRootCacheNestedMutation(t *testing.T) {
	body := gloasBodyRootCacheTestBody(t)
	previous := requireBodyRoot(t, body)

	attestation := body.Attestations.Get(0)
	attestation.AggregationBits.Set(0, attestation.AggregationBits.Get(0)|0b10)
	requireRootChanged(t, body, previous)
}

func TestGloasBodyRootCacheStructCopy(t *testing.T) {
	body := gloasBodyRootCacheTestBody(t)
	originalRoot := requireBodyRoot(t, body)

	cp := *body
	cp.Graffiti[0]++

	requireRootChanged(t, &cp, originalRoot)
	require.Equal(t, originalRoot, requireBodyRoot(t, body))
	require.Equal(t, requireFreshBodyRoot(t, body), originalRoot)
}

func TestGloasBodyRootCacheHit(t *testing.T) {
	body := gloasBodyRootCacheTestBody(t)
	firstRoot := requireBodyRoot(t, body)
	firstEntry := body.rootCache.Load()
	require.NotNil(t, firstEntry)

	require.Equal(t, firstRoot, requireBodyRoot(t, body))
	require.Same(t, firstEntry, body.rootCache.Load())

	body.Graffiti[0]++
	requireRootChanged(t, body, firstRoot)
	require.NotSame(t, firstEntry, body.rootCache.Load())
}

func TestGloasBodyRootCacheVersionGate(t *testing.T) {
	tests := []struct {
		name    string
		version clparams.StateVersion
		caches  bool
	}{
		{name: "fulu", version: clparams.FuluVersion},
		{name: "gloas", version: clparams.GloasVersion, caches: true},
		{name: "heze", version: clparams.HezeVersion, caches: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := NewBeaconBody(&clparams.MainnetBeaconConfig, test.version)
			root, err := body.HashSSZ()
			require.NoError(t, err)
			if !test.caches {
				require.Nil(t, body.rootCache.Load())
				return
			}
			require.NotNil(t, body.rootCache.Load())
			require.Equal(t, requireFreshBodyRoot(t, body), root)
		})
	}
}

func TestGloasBodyRootCacheNilCache(t *testing.T) {
	body := gloasBodyRootCacheTestBody(t)
	body.rootCache = nil

	root, err := body.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, requireFreshBodyRoot(t, body), root)
	require.Nil(t, body.rootCache)
}

func TestGloasBodyRootCacheConcurrent(t *testing.T) {
	body := gloasBodyRootCacheTestBody(t)
	want := requireFreshBodyRoot(t, body)

	const workers = 64
	roots := make([][32]byte, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() {
			roots[i], errs[i] = body.HashSSZ()
		})
	}
	wg.Wait()

	for i := range workers {
		require.NoError(t, errs[i])
		require.Equal(t, want, roots[i])
	}
}

func gloasBodyRootCacheTestBody(t testing.TB) *BeaconBody {
	t.Helper()
	body := NewBeaconBody(&clparams.MainnetBeaconConfig, clparams.GloasVersion)
	body.RandaoReveal[0] = 0x11
	body.Eth1Data.Root[0] = 0x12
	body.Eth1Data.DepositCount = 13
	body.Eth1Data.BlockHash[0] = 0x14
	body.Graffiti[0] = 0x15
	body.SyncAggregate = NewSyncAggregate()
	body.SyncAggregate.SyncCommiteeBits[0] = 0x16
	body.ProposerSlashings.Append(&ProposerSlashing{
		Header1: &SignedBeaconBlockHeader{Header: &BeaconBlockHeader{Slot: 17}},
		Header2: &SignedBeaconBlockHeader{Header: &BeaconBlockHeader{Slot: 18}},
	})
	body.VoluntaryExits.Append(&SignedVoluntaryExit{
		VoluntaryExit: &VoluntaryExit{Epoch: 19, ValidatorIndex: 20},
	})
	body.Attestations.Append(gloasBodyRootCacheTestAttestation(t, 1, nil))
	body.Attestations.Append(gloasBodyRootCacheTestAttestation(t, 2, nil))
	body.PayloadAttestations.Append(gloasBodyRootCacheTestPayloadAttestation(t, 1, false))
	body.PayloadAttestations.Append(gloasBodyRootCacheTestPayloadAttestation(t, 2, false))
	return body
}

func gloasBodyRootCacheTestAttestation(t testing.TB, value byte, aggregationBits []byte) *solid.Attestation {
	t.Helper()
	if aggregationBits == nil {
		aggregationBits = []byte{0b00000101}
	}
	committeeBits := solid.NewBitVector(int(clparams.MainnetBeaconConfig.MaxCommitteesPerSlot))
	require.NoError(t, committeeBits.SetBitAt(int(value), true))
	attestation := &solid.Attestation{
		AggregationBits: solid.BitlistFromBytes(bytes.Clone(aggregationBits), int(clparams.MainnetBeaconConfig.MaxCommitteesPerSlot*clparams.MainnetBeaconConfig.MaxValidatorsPerCommittee)),
		Data: &solid.AttestationData{
			Slot:            uint64(value),
			BeaconBlockRoot: common.Hash{value},
		},
		CommitteeBits: committeeBits,
	}
	attestation.Signature[0] = value
	attestation.SetVersion(clparams.GloasVersion)
	return attestation
}

func gloasBodyRootCacheTestPayloadAttestation(t testing.TB, value byte, allBits bool) *PayloadAttestation {
	t.Helper()
	bits := solid.NewBitVector(int(clparams.MainnetBeaconConfig.PtcSize))
	if allBits {
		for bit := range int(clparams.MainnetBeaconConfig.PtcSize) {
			require.NoError(t, bits.SetBitAt(bit, true))
		}
	} else {
		require.NoError(t, bits.SetBitAt(int(value), true))
	}
	attestation := &PayloadAttestation{
		AggregationBits: bits,
		Data: &PayloadAttestationData{
			BeaconBlockRoot:   common.Hash{value},
			Slot:              uint64(value),
			PayloadPresent:    true,
			BlobDataAvailable: value%2 == 0,
		},
	}
	attestation.Signature[0] = value
	return attestation
}

func requireBodyRoot(t testing.TB, body *BeaconBody) [32]byte {
	t.Helper()
	root, err := body.HashSSZ()
	require.NoError(t, err)
	return root
}

func requireFreshBodyRoot(t testing.TB, body *BeaconBody) [32]byte {
	t.Helper()
	root, err := body.hashSSZGloas()
	require.NoError(t, err)
	return root
}

func requireRootChanged(t testing.TB, body *BeaconBody, previous [32]byte) {
	t.Helper()
	root := requireBodyRoot(t, body)
	require.Equal(t, requireFreshBodyRoot(t, body), root)
	require.NotEqual(t, previous, root)
}
