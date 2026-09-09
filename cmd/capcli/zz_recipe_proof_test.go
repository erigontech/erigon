package main

import (
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common"
)

// sources returns the endpoints to probe. They come from the environment so no operator's
// infrastructure is baked into the repository.
func sources(t *testing.T) *archiveSource {
	t.Helper()
	beacon, archive := os.Getenv("RECIPE_BEACON"), os.Getenv("RECIPE_ARCHIVE")
	if beacon == "" || archive == "" {
		t.Skip("set RECIPE_BEACON and RECIPE_ARCHIVE to run")
	}
	return newArchiveSource(splitEndpoints(beacon), archive, 5, 60*time.Second)
}

// End-to-end proof of the recovery path against real sources, using the same functions the
// repair uses.
func TestZZArchiveRecipeEndToEnd(t *testing.T) {
	src := sources(t)
	const (
		slot     = 29405528
		wantRoot = "0x0df45d656cdd763c9115aeb0d82af94e1f0b43bff6583337b106b9cf94df62b5"
	)
	_, beaconCfg, _, err := clparams.GetConfigsByNetworkName("gnosis")
	require.NoError(t, err)

	block, err := src.fullBlock(t.Context(), slot, beaconCfg)
	require.NoError(t, err)
	require.NotNil(t, block, "peer must serve the canonical block")

	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, wantRoot, "0x"+hex.EncodeToString(root[:]),
		"the block we build proofs from must be the canonical one")
	t.Logf("canonical block verified: %s (version %d)", wantRoot, block.Version())

	commitments := block.Block.Body.BlobKzgCommitments
	require.Equal(t, 1, commitments.Len())

	payloads := map[uint64][]byte{}
	for i := range commitments.Len() {
		c := common.Bytes48(*commitments.Get(i))
		vh := versionedHashFor(c)
		payload, found, err := src.blobPayload(t.Context(), vh)
		require.NoError(t, err)
		require.True(t, found, "archive must hold %s", vh.Hex())
		require.Len(t, payload, blobLenBytes)
		payloads[uint64(i)] = payload
		t.Logf("archive served index %d via %s (%d bytes)", i, vh.Hex(), len(payload))
	}

	sidecars, err := buildVerifiedSidecars(block, payloads)
	require.NoError(t, err, "assembly must pass commitment, inclusion and kzg verification")
	require.Len(t, sidecars, 1)
	require.Equal(t, commitments.Len(), len(sidecars))
	t.Log("sidecars assembled and fully verified; ready for insertion")
}

// A payload that is not the block's blob must be refused even though it is a well formed
// blob, because the archive is an external source and this is the only thing tying its bytes
// to the chain.
func TestZZArchiveRecipeRejectsASubstitutedPayload(t *testing.T) {
	src := sources(t)
	_, beaconCfg, _, err := clparams.GetConfigsByNetworkName("gnosis")
	require.NoError(t, err)

	block, err := src.fullBlock(t.Context(), 29405528, beaconCfg)
	require.NoError(t, err)
	require.NotNil(t, block)

	tampered := make([]byte, blobLenBytes)
	tampered[0] = 0x02
	_, err = buildVerifiedSidecars(block, map[uint64][]byte{0: tampered})
	require.Error(t, err)
	require.ErrorContains(t, err, "commitment mismatch")
}
