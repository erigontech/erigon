package raw

import (
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

// TestGloasSSZRoundTrip verifies that a GLOAS beacon state can be encoded and decoded
// without losing data. This is critical for state reconstruction from disk dumps.
func TestGloasSSZRoundTrip(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	s1 := New(&cfg)
	s1.version = clparams.GloasVersion

	// Set some non-zero values to make the test meaningful
	s1.genesisTime = 12345
	s1.slot = 44
	s1.latestBlockHeader = &cltypes.BeaconBlockHeader{
		Slot:          44,
		ProposerIndex: 5,
		ParentRoot:    common.HexToHash("0x1111"),
		Root:          common.Hash{}, // zero, as after ProcessBlockHeader
		BodyRoot:      common.HexToHash("0x2222"),
	}
	s1.latestBlockHash = common.HexToHash("0x3333")
	s1.nextWithdrawalBuilderIndex = 77
	s1.eth1DepositIndex = 99
	s1.depositRequestsStartIndex = 100

	// Initialize the execution payload header (pre-GLOAS, but still in state)
	s1.latestExecutionPayloadHeader = cltypes.NewEth1Header(clparams.GloasVersion)

	// Properly initialize latestExecutionPayloadBid like UpgradeToGloas does
	s1.latestExecutionPayloadBid = &cltypes.ExecutionPayloadBid{
		BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48),
	}

	// Step 1: Compute hash of original state
	hash1, err := s1.HashSSZ()
	require.NoError(t, err)
	t.Logf("Original state HashSSZ: %x", hash1)

	// Step 2: Encode to SSZ
	encoded, err := s1.EncodeSSZ(nil)
	require.NoError(t, err)
	t.Logf("Encoded SSZ size: %d bytes", len(encoded))

	// Step 3: Decode from SSZ
	s2 := New(&cfg)
	err = s2.DecodeSSZ(encoded, int(clparams.GloasVersion))
	require.NoError(t, err)
	t.Logf("Decoded state slot: %d, version: %d", s2.slot, s2.version)

	// Step 4: Compute hash of decoded state
	hash2, err := s2.HashSSZ()
	require.NoError(t, err)
	t.Logf("Decoded state HashSSZ: %x", hash2)

	// Step 5: Compare hashes - this is the critical check
	if hash1 != hash2 {
		t.Errorf("HashSSZ MISMATCH after SSZ round-trip!")
		t.Errorf("  Original: %x", hash1)
		t.Errorf("  Decoded:  %x", hash2)

		// Compare leaves
		t.Log("=== Leaf comparison ===")
		for i := 0; i < StateLeafSizeGloas; i++ {
			leaf1 := common.BytesToHash(s1.leaves[i*32 : (i+1)*32])
			leaf2 := common.BytesToHash(s2.leaves[i*32 : (i+1)*32])
			if leaf1 != leaf2 {
				t.Errorf("Leaf %d MISMATCH: original=%x, decoded=%x", i, leaf1, leaf2)
			}
		}
		t.FailNow()
	}

	// Step 6: Check EncodeSSZ round-trip
	reencoded, err := s2.EncodeSSZ(nil)
	require.NoError(t, err)

	if len(encoded) != len(reencoded) {
		t.Fatalf("SSZ size mismatch: original=%d, reencoded=%d", len(encoded), len(reencoded))
	}

	for i := range encoded {
		if encoded[i] != reencoded[i] {
			t.Fatalf("SSZ data mismatch at byte %d: original=0x%02x, reencoded=0x%02x", i, encoded[i], reencoded[i])
		}
	}

	// Step 7: Verify specific GLOAS fields survived round-trip
	require.Equal(t, s1.slot, s2.slot)
	require.Equal(t, s1.latestBlockHash, s2.latestBlockHash, "latestBlockHash mismatch")
	require.Equal(t, s1.nextWithdrawalBuilderIndex, s2.nextWithdrawalBuilderIndex, "nextWithdrawalBuilderIndex mismatch")
	require.Equal(t, s1.latestBlockHeader.Slot, s2.latestBlockHeader.Slot, "LatestBlockHeader.Slot mismatch")
	require.Equal(t, s1.latestBlockHeader.Root, s2.latestBlockHeader.Root, "LatestBlockHeader.Root mismatch")
}
