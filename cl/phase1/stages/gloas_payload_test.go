package stages

import (
	"context"
	"encoding/binary"
	"math/big"
	"slices"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	state2 "github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
)

func TestValidateAnchorEnvelope(t *testing.T) {
	cfg, st, bid, env, anchorRoot := validAnchorEnvelopeFixture(t, 1)

	require.NoError(t, validateAnchorEnvelope(cfg, st, anchorRoot, bid, env))

	tests := []struct {
		name    string
		mutate  func(*cltypes.ExecutionPayloadBid, *cltypes.SignedExecutionPayloadEnvelope)
		wantErr string
	}{
		{
			name: "beacon root mismatch",
			mutate: func(_ *cltypes.ExecutionPayloadBid, env *cltypes.SignedExecutionPayloadEnvelope) {
				env.Message.BeaconBlockRoot = common.HexToHash("0x99")
			},
			wantErr: "beacon block root mismatch",
		},
		{
			name: "parent root mismatch",
			mutate: func(_ *cltypes.ExecutionPayloadBid, env *cltypes.SignedExecutionPayloadEnvelope) {
				env.Message.ParentBeaconBlockRoot = common.HexToHash("0x98")
			},
			wantErr: "parent beacon block root mismatch",
		},
		{
			name: "builder index mismatch",
			mutate: func(_ *cltypes.ExecutionPayloadBid, env *cltypes.SignedExecutionPayloadEnvelope) {
				env.Message.BuilderIndex++
			},
			wantErr: "builder index mismatch",
		},
		{
			name: "block hash mismatch",
			mutate: func(bid *cltypes.ExecutionPayloadBid, _ *cltypes.SignedExecutionPayloadEnvelope) {
				bid.BlockHash = common.HexToHash("0x97")
			},
			wantErr: "block hash mismatch",
		},
		{
			name: "parent block hash mismatch",
			mutate: func(_ *cltypes.ExecutionPayloadBid, env *cltypes.SignedExecutionPayloadEnvelope) {
				env.Message.Payload.ParentHash = common.HexToHash("0x96")
			},
			wantErr: "parent block hash mismatch",
		},
		{
			name: "prev randao mismatch",
			mutate: func(_ *cltypes.ExecutionPayloadBid, env *cltypes.SignedExecutionPayloadEnvelope) {
				env.Message.Payload.PrevRandao = common.HexToHash("0x95")
			},
			wantErr: "prev randao mismatch",
		},
		{
			name: "fee recipient mismatch",
			mutate: func(_ *cltypes.ExecutionPayloadBid, env *cltypes.SignedExecutionPayloadEnvelope) {
				env.Message.Payload.FeeRecipient = common.HexToAddress("0x0000000000000000000000000000000000000094")
			},
			wantErr: "fee recipient mismatch",
		},
		{
			name: "gas limit mismatch",
			mutate: func(_ *cltypes.ExecutionPayloadBid, env *cltypes.SignedExecutionPayloadEnvelope) {
				env.Message.Payload.GasLimit++
			},
			wantErr: "gas limit mismatch",
		},
		{
			name: "slot mismatch",
			mutate: func(_ *cltypes.ExecutionPayloadBid, env *cltypes.SignedExecutionPayloadEnvelope) {
				env.Message.Payload.SlotNumber++
			},
			wantErr: "slot mismatch",
		},
		{
			name: "nil execution requests",
			mutate: func(_ *cltypes.ExecutionPayloadBid, env *cltypes.SignedExecutionPayloadEnvelope) {
				env.Message.ExecutionRequests = nil
			},
			wantErr: "nil execution requests",
		},
		{
			name: "execution requests root mismatch",
			mutate: func(bid *cltypes.ExecutionPayloadBid, _ *cltypes.SignedExecutionPayloadEnvelope) {
				bid.ExecutionRequestsRoot = common.HexToHash("0x93")
			},
			wantErr: "execution requests root mismatch",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, st, bid, env, anchorRoot := validAnchorEnvelopeFixture(t, 1)
			tt.mutate(bid, env)
			require.ErrorContains(t, validateAnchorEnvelope(cfg, st, anchorRoot, bid, env), tt.wantErr)
		})
	}
}

func TestCollectUnverifiedGloasPayloadsIncludesCanonicalAndHighestSeenForks(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&cfg)
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0
	cfg.InitializeForkSchedule()
	parentRoot := common.HexToHash("0x10")
	rootA := common.HexToHash("0xa")
	rootB := common.HexToHash("0xb")
	blocks := map[common.Hash]*cltypes.SignedBeaconBlock{
		parentRoot: testGloasVerificationBlock(&cfg, 1, common.HexToHash("0x01")),
		rootA:      testGloasVerificationBlock(&cfg, 2, parentRoot),
		rootB:      testGloasVerificationBlock(&cfg, 2, parentRoot),
	}
	getBlock := func(root common.Hash) (*cltypes.SignedBeaconBlock, bool) {
		block, ok := blocks[root]
		return block, ok
	}
	shouldVerify := func(root common.Hash) bool {
		_, ok := blocks[root]
		return ok
	}

	for _, tt := range []struct {
		name   string
		starts []common.Hash
	}{
		{name: "canonical B and highest-seen A", starts: []common.Hash{rootB, rootA}},
		{name: "canonical A and highest-seen B", starts: []common.Hash{rootA, rootB}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			items, _ := collectUnverifiedGloasPayloads(tt.starts, 0, &cfg, getBlock, shouldVerify)
			roots := verificationRoots(items)
			require.Len(t, roots, 2)
			require.Equal(t, parentRoot, roots[0])
			require.Equal(t, tt.starts[0], roots[1])
		})
	}
}

func TestCollectUnverifiedGloasPayloadsDoesNotStarveLineageAtScanLimit(t *testing.T) {
	cfg := testGloasVerificationConfig()
	blocks := make(map[common.Hash]*cltypes.SignedBeaconBlock, maxGloasVerificationScanPerLineage+2)
	canonicalRoot := common.Hash{}
	for i := 1; i <= maxGloasVerificationScanPerLineage+1; i++ {
		root := testGloasVerificationRoot(uint64(i))
		blocks[root] = testGloasVerificationBlock(&cfg, uint64(i), canonicalRoot)
		canonicalRoot = root
	}
	sideRoot := testGloasVerificationRoot(10_000)
	blocks[sideRoot] = testGloasVerificationBlock(&cfg, 1, common.Hash{})
	getBlock := func(root common.Hash) (*cltypes.SignedBeaconBlock, bool) {
		block, ok := blocks[root]
		return block, ok
	}

	for _, length := range []int{maxGloasVerificationScanPerLineage, maxGloasVerificationScanPerLineage + 1} {
		canonicalRoot = testGloasVerificationRoot(uint64(length))
		for _, starts := range [][]common.Hash{{canonicalRoot, sideRoot}, {sideRoot, canonicalRoot}} {
			items, _ := collectUnverifiedGloasPayloads(starts, 0, &cfg, getBlock, func(root common.Hash) bool {
				return root == sideRoot
			})
			require.Contains(t, verificationRoots(items), sideRoot)
		}
	}
}

func TestCollectUnverifiedGloasPayloadsReturnsDeepLineageContinuation(t *testing.T) {
	cfg := testGloasVerificationConfig()
	blocks := make(map[common.Hash]*cltypes.SignedBeaconBlock, maxGloasVerificationScanPerLineage+1)
	parentRoot := common.Hash{}
	oldestRoot := common.Hash{}
	for i := 1; i <= maxGloasVerificationScanPerLineage+1; i++ {
		root := testGloasVerificationRoot(uint64(i))
		blocks[root] = testGloasVerificationBlock(&cfg, uint64(i), parentRoot)
		if i == 1 {
			oldestRoot = root
		}
		parentRoot = root
	}
	newestRoot := parentRoot
	verified := make(map[common.Hash]bool)
	shouldVerify := func(root common.Hash) bool { return !verified[root] }
	items, continuations := collectUnverifiedGloasPayloads(
		[]common.Hash{newestRoot},
		0,
		&cfg,
		func(root common.Hash) (*cltypes.SignedBeaconBlock, bool) {
			block, ok := blocks[root]
			return block, ok
		},
		shouldVerify,
	)

	require.Empty(t, items)
	require.Equal(t, []common.Hash{oldestRoot}, continuations)
	items, continuations = collectUnverifiedGloasPayloads(
		continuations,
		0,
		&cfg,
		func(root common.Hash) (*cltypes.SignedBeaconBlock, bool) {
			block, ok := blocks[root]
			return block, ok
		},
		shouldVerify,
	)
	require.Empty(t, continuations)
	require.Equal(t, []common.Hash{oldestRoot}, verificationRoots(items))
}

func TestCollectUnverifiedGloasPayloadsContinuesThroughEmptyBoundaryWithoutDeferringDescendant(t *testing.T) {
	cfg := testGloasVerificationConfig()
	blocks := make(map[common.Hash]*cltypes.SignedBeaconBlock, maxGloasVerificationScanPerLineage+1)
	parentRoot := common.Hash{}
	for i := 1; i <= maxGloasVerificationScanPerLineage+1; i++ {
		root := testGloasVerificationRoot(uint64(i))
		blocks[root] = testGloasVerificationBlock(&cfg, uint64(i), parentRoot)
		parentRoot = root
	}
	newestRoot := parentRoot
	items, continuations := collectUnverifiedGloasPayloads(
		[]common.Hash{newestRoot},
		0,
		&cfg,
		func(root common.Hash) (*cltypes.SignedBeaconBlock, bool) {
			block, ok := blocks[root]
			return block, ok
		},
		func(root common.Hash) bool { return root == newestRoot },
	)

	require.Equal(t, []common.Hash{testGloasVerificationRoot(1)}, continuations)
	require.Empty(t, items)
}

func TestCollectUnverifiedGloasPayloadsFindsWorkBehindNonActionableBoundary(t *testing.T) {
	cfg := testGloasVerificationConfig()
	blocks := make(map[common.Hash]*cltypes.SignedBeaconBlock, maxGloasVerificationScanPerLineage+2)
	parentRoot := common.Hash{}
	for i := 1; i <= maxGloasVerificationScanPerLineage+2; i++ {
		root := testGloasVerificationRoot(uint64(i))
		blocks[root] = testGloasVerificationBlock(&cfg, uint64(i), parentRoot)
		parentRoot = root
	}
	oldestRoot := testGloasVerificationRoot(1)
	boundaryRoot := testGloasVerificationRoot(2)
	blocks[oldestRoot].Block.Body.GetSignedExecutionPayloadBid().Message.BlockHash = common.HexToHash("0x01")
	blocks[boundaryRoot].Block.Body.GetSignedExecutionPayloadBid().Message.ParentBlockHash = common.HexToHash("0x02")
	getBlock := func(root common.Hash) (*cltypes.SignedBeaconBlock, bool) {
		block, ok := blocks[root]
		return block, ok
	}
	shouldVerify := func(root common.Hash) bool { return root == oldestRoot }

	items, continuations := collectUnverifiedGloasPayloads([]common.Hash{parentRoot}, 0, &cfg, getBlock, shouldVerify)
	require.Empty(t, items)
	require.Equal(t, []common.Hash{boundaryRoot}, continuations)
	items, continuations = collectUnverifiedGloasPayloads(continuations, 0, &cfg, getBlock, shouldVerify)
	require.Empty(t, continuations)
	require.Equal(t, []common.Hash{oldestRoot}, verificationRoots(items))
}

func TestCollectUnverifiedGloasPayloadsDoesNotDeferIndependentEmptyDescendants(t *testing.T) {
	cfg := testGloasVerificationConfig()
	blocks := make(map[common.Hash]*cltypes.SignedBeaconBlock, maxGloasVerificationScanPerLineage+1)
	parentRoot := common.Hash{}
	for i := 1; i <= maxGloasVerificationScanPerLineage+1; i++ {
		root := testGloasVerificationRoot(uint64(i))
		blocks[root] = testGloasVerificationBlock(&cfg, uint64(i), parentRoot)
		parentRoot = root
	}
	oldestRoot := testGloasVerificationRoot(1)
	oldestChildRoot := testGloasVerificationRoot(2)
	blocks[oldestRoot].Block.Body.GetSignedExecutionPayloadBid().Message.BlockHash = common.HexToHash("0x01")
	blocks[oldestChildRoot].Block.Body.GetSignedExecutionPayloadBid().Message.ParentBlockHash = common.HexToHash("0x02")
	items, continuations := collectUnverifiedGloasPayloads(
		[]common.Hash{parentRoot},
		0,
		&cfg,
		func(root common.Hash) (*cltypes.SignedBeaconBlock, bool) {
			block, ok := blocks[root]
			return block, ok
		},
		func(common.Hash) bool { return true },
	)

	require.Equal(t, []common.Hash{oldestRoot}, continuations)
	require.Empty(t, items)
}

func TestCollectUnverifiedGloasPayloadsDefersSharedForkUntilAncestor(t *testing.T) {
	cfg := testGloasVerificationConfig()
	blocks := make(map[common.Hash]*cltypes.SignedBeaconBlock, maxGloasVerificationScanPerLineage+2)
	parentRoot := common.Hash{}
	for i := 1; i <= maxGloasVerificationScanPerLineage+1; i++ {
		root := testGloasVerificationRoot(uint64(i))
		blocks[root] = testGloasVerificationBlock(&cfg, uint64(i), parentRoot)
		parentRoot = root
	}
	oldestRoot := testGloasVerificationRoot(1)
	sharedRoot := testGloasVerificationRoot(200)
	sideRoot := testGloasVerificationRoot(10_000)
	blocks[sideRoot] = testGloasVerificationBlock(&cfg, maxGloasVerificationScanPerLineage+2, sharedRoot)
	items, continuations := collectUnverifiedGloasPayloads(
		[]common.Hash{parentRoot, sideRoot},
		0,
		&cfg,
		func(root common.Hash) (*cltypes.SignedBeaconBlock, bool) {
			block, ok := blocks[root]
			return block, ok
		},
		func(common.Hash) bool { return true },
	)

	require.Equal(t, []common.Hash{oldestRoot}, continuations)
	require.NotEmpty(t, items)
	require.Equal(t, oldestRoot, items[0].root)
	require.NotContains(t, verificationRoots(items), sideRoot)
}

func TestCollectUnverifiedGloasPayloadPagesAdvancesBeyondContinuationCapacity(t *testing.T) {
	cfg := testGloasVerificationConfig()
	depth := maxGloasVerificationScanPerLineage*(maxGloasVerificationStartRootsPerCycle+1) + 1
	blocks := make(map[common.Hash]*cltypes.SignedBeaconBlock, depth)
	parentRoot := common.Hash{}
	for i := 1; i <= depth; i++ {
		root := testGloasVerificationRoot(uint64(i))
		blocks[root] = testGloasVerificationBlock(&cfg, uint64(i), parentRoot)
		parentRoot = root
	}
	verified := make(map[common.Hash]bool, depth)
	states := []gloasVerificationLineage{{origin: parentRoot, cursor: parentRoot}}
	for cycle := 0; cycle < 500 && len(states) > 0; cycle++ {
		items, next := collectUnverifiedGloasPayloadPages(
			states,
			0,
			&cfg,
			func(root common.Hash) (*cltypes.SignedBeaconBlock, bool) {
				block, ok := blocks[root]
				return block, ok
			},
			func(root common.Hash) bool { return !verified[root] },
		)
		for _, item := range items {
			verified[item.root] = true
		}
		states = next
	}

	require.Empty(t, states)
	require.True(t, verified[testGloasVerificationRoot(1)])
	require.True(t, verified[parentRoot])
}

func TestCollectUnverifiedGloasPayloadPagesDefersSharedSideUntilParent(t *testing.T) {
	cfg := testGloasVerificationConfig()
	blocks := make(map[common.Hash]*cltypes.SignedBeaconBlock, 66)
	parentRoot := common.Hash{}
	for i := 1; i <= 64; i++ {
		root := testGloasVerificationRoot(uint64(i))
		blocks[root] = testGloasVerificationBlock(&cfg, uint64(i), parentRoot)
		parentRoot = root
	}
	sharedRoot := parentRoot
	canonicalRoot := testGloasVerificationRoot(65)
	sideRoot := testGloasVerificationRoot(10_000)
	blocks[canonicalRoot] = testGloasVerificationBlock(&cfg, 65, sharedRoot)
	blocks[sideRoot] = testGloasVerificationBlock(&cfg, 65, sharedRoot)
	verified := make(map[common.Hash]bool)
	states := []gloasVerificationLineage{
		{origin: canonicalRoot, cursor: canonicalRoot},
		{origin: sideRoot, cursor: sideRoot},
	}
	sawSide := false
	for cycle := 0; cycle < 20 && len(states) > 0; cycle++ {
		items, next := collectUnverifiedGloasPayloadPages(
			states,
			0,
			&cfg,
			func(root common.Hash) (*cltypes.SignedBeaconBlock, bool) {
				block, ok := blocks[root]
				return block, ok
			},
			func(root common.Hash) bool { return !verified[root] },
		)
		for _, item := range items {
			if item.root == sideRoot {
				require.True(t, verified[sharedRoot])
				sawSide = true
			}
			verified[item.root] = true
		}
		states = next
	}
	require.True(t, sawSide)
}

func TestCollectUnverifiedGloasPayloadsSharesOutputAcrossLineages(t *testing.T) {
	cfg := testGloasVerificationConfig()
	blocks := make(map[common.Hash]*cltypes.SignedBeaconBlock, maxGloasVerificationSweepPerCycle+1)
	canonicalRoot := common.Hash{}
	for i := 1; i <= maxGloasVerificationSweepPerCycle; i++ {
		root := testGloasVerificationRoot(uint64(i))
		blocks[root] = testGloasVerificationBlock(&cfg, uint64(i), canonicalRoot)
		canonicalRoot = root
	}
	sideRoot := testGloasVerificationRoot(10_000)
	blocks[sideRoot] = testGloasVerificationBlock(&cfg, 1, common.Hash{})
	getBlock := func(root common.Hash) (*cltypes.SignedBeaconBlock, bool) {
		block, ok := blocks[root]
		return block, ok
	}

	for _, starts := range [][]common.Hash{{canonicalRoot, sideRoot}, {sideRoot, canonicalRoot}} {
		items, _ := collectUnverifiedGloasPayloads(starts, 0, &cfg, getBlock, func(common.Hash) bool { return true })
		require.LessOrEqual(t, len(items), maxGloasVerificationSweepPerCycle)
		require.Contains(t, verificationRoots(items), sideRoot)
		lastSlot := uint64(0)
		for _, item := range items {
			require.GreaterOrEqual(t, item.block.Block.Slot, lastSlot)
			lastSlot = item.block.Block.Slot
		}
	}
}

func TestCollectUnverifiedGloasPayloadsWalksHiddenLeafLineage(t *testing.T) {
	cfg := testGloasVerificationConfig()
	canonicalRoot := testGloasVerificationRoot(100)
	highestSeenRoot := testGloasVerificationRoot(200)
	hiddenRoots := []common.Hash{
		testGloasVerificationRoot(301),
		testGloasVerificationRoot(302),
		testGloasVerificationRoot(303),
	}
	blocks := map[common.Hash]*cltypes.SignedBeaconBlock{
		canonicalRoot:   testGloasVerificationBlock(&cfg, 4, common.Hash{}),
		highestSeenRoot: testGloasVerificationBlock(&cfg, 5, common.Hash{}),
		hiddenRoots[0]:  testGloasVerificationBlock(&cfg, 1, common.Hash{}),
		hiddenRoots[1]:  testGloasVerificationBlock(&cfg, 2, hiddenRoots[0]),
		hiddenRoots[2]:  testGloasVerificationBlock(&cfg, 3, hiddenRoots[1]),
	}
	items, _ := collectUnverifiedGloasPayloads(
		[]common.Hash{canonicalRoot, highestSeenRoot, hiddenRoots[2]},
		0,
		&cfg,
		func(root common.Hash) (*cltypes.SignedBeaconBlock, bool) {
			block, ok := blocks[root]
			return block, ok
		},
		func(root common.Hash) bool { return slices.Contains(hiddenRoots, root) },
	)

	require.Equal(t, hiddenRoots, verificationRoots(items))
}

func testGloasVerificationConfig() clparams.BeaconChainConfig {
	cfg := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&cfg)
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0
	cfg.InitializeForkSchedule()
	return cfg
}

func testGloasVerificationRoot(i uint64) common.Hash {
	var root common.Hash
	binary.BigEndian.PutUint64(root[len(root)-8:], i)
	return root
}

func verificationRoots(items []gloasVerificationBlock) []common.Hash {
	roots := make([]common.Hash, len(items))
	for i, item := range items {
		roots[i] = item.root
	}
	return roots
}

func testGloasVerificationBlock(cfg *clparams.BeaconChainConfig, slot uint64, parentRoot common.Hash) *cltypes.SignedBeaconBlock {
	block := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
	block.Block.Slot = slot
	block.Block.ParentRoot = parentRoot
	return block
}

func TestAnchorEnvelopeMatches(t *testing.T) {
	_, _, _, env, anchorRoot := validAnchorEnvelopeFixture(t, 1)

	require.True(t, anchorEnvelopeMatches(env, anchorRoot))

	// Server finalized ahead of the local anchor: the HTTP endpoint returns a valid
	// envelope for a newer block, which must be rejected so we fall back to a
	// root-specific P2P request instead of failing anchor validation.
	require.False(t, anchorEnvelopeMatches(env, common.HexToHash("0x99")))
	require.False(t, anchorEnvelopeMatches(nil, anchorRoot))
	require.False(t, anchorEnvelopeMatches(&cltypes.SignedExecutionPayloadEnvelope{}, anchorRoot))
}

func TestVerifyAnchorEnvelopeSignature(t *testing.T) {
	_, st, bid, env, _ := validAnchorEnvelopeFixture(t, 2)
	require.NoError(t, verifyAnchorEnvelopeSignature(st.BeaconConfig(), st, env, bid.Slot))

	cfg, st, bid, env, _ := validAnchorEnvelopeFixture(t, clparams.BuilderIndexSelfBuild)
	require.NoError(t, verifyAnchorEnvelopeSignature(cfg, st, env, bid.Slot))

	t.Run("invalid signature", func(t *testing.T) {
		cfg, st, bid, env, _ := validAnchorEnvelopeFixture(t, 1)
		env.Signature[0] ^= 0x01
		require.Error(t, verifyAnchorEnvelopeSignature(cfg, st, env, bid.Slot))
	})

	t.Run("builder index out of range", func(t *testing.T) {
		cfg, st, bid, env, _ := validAnchorEnvelopeFixture(t, 1)
		env.Message.BuilderIndex = 63
		require.ErrorContains(t, verifyAnchorEnvelopeSignature(cfg, st, env, bid.Slot), "builder index 63 out of range")
	})

	t.Run("nil builders", func(t *testing.T) {
		cfg, st, bid, env, _ := validAnchorEnvelopeFixture(t, 1)
		st.SetBuilders(nil)
		require.ErrorContains(t, verifyAnchorEnvelopeSignature(cfg, st, env, bid.Slot), "builders not found")
	})
}

func TestGloasPayloadHelpers(t *testing.T) {
	require.False(t, validPendingGloasPayload(forkchoice.PendingELPayload{}))
	require.False(t, validPendingGloasPayload(forkchoice.PendingELPayload{Block: &cltypes.SignedBeaconBlock{}}))

	hash, ok := gloasEnvelopePayloadHash(&cltypes.SignedExecutionPayloadEnvelope{})
	require.False(t, ok)
	require.Equal(t, common.Hash{}, hash)

	want := common.HexToHash("0x1234")
	hash, ok = gloasEnvelopePayloadHash(&cltypes.SignedExecutionPayloadEnvelope{
		Message: &cltypes.ExecutionPayloadEnvelope{
			Payload: &cltypes.Eth1Block{BlockHash: want},
		},
	})
	require.True(t, ok)
	require.Equal(t, want, hash)
}

func TestStandaloneExecutionClientDoesNotRunLocalGloasRetry(t *testing.T) {
	require.False(t, canRetryGloasPayloads(&Cfg{}))
	require.False(t, canRetryGloasPayloads(&Cfg{executionClient: &testExecutionEngine{supportInsertion: false}}))
	require.True(t, canRetryGloasPayloads(&Cfg{executionClient: &testExecutionEngine{supportInsertion: true}}))
}

func TestStandaloneExecutionClientCanValidateGloasPayloads(t *testing.T) {
	require.False(t, canValidateGloasPayloads(&Cfg{}))
	require.True(t, canValidateGloasPayloads(&Cfg{executionClient: &testExecutionEngine{supportInsertion: false}}))
	require.True(t, canValidateGloasPayloads(&Cfg{executionClient: &testExecutionEngine{supportInsertion: true}}))
}

func TestValidateAnchorPayloadUsesRemoteExecutionClient(t *testing.T) {
	cfg, _, bid, env, anchorRoot := validAnchorEnvelopeFixture(t, 1)
	remoteEL := &testExecutionEngine{
		supportInsertion: false,
		payloadStatus:    execution_client.PayloadStatusValidated,
	}

	require.NoError(t, validateAnchorPayloadWithExecutionClient(context.Background(), &Cfg{
		beaconCfg:       cfg,
		executionClient: remoteEL,
		forkChoice:      &forkchoice.ForkChoiceStore{},
	}, anchorRoot, bid, env))
	require.Equal(t, 1, remoteEL.newPayloadCalls)

	localEL := &testExecutionEngine{
		supportInsertion: true,
		payloadStatus:    execution_client.PayloadStatusValidated,
	}
	require.NoError(t, validateAnchorPayloadWithExecutionClient(context.Background(), &Cfg{
		beaconCfg:       cfg,
		executionClient: localEL,
		forkChoice:      &forkchoice.ForkChoiceStore{},
	}, anchorRoot, bid, env))
	require.Equal(t, 1, localEL.newPayloadCalls)
}

func TestValidateAnchorPayloadHandlesRemoteExecutionStatuses(t *testing.T) {
	for _, tt := range []struct {
		name   string
		status execution_client.PayloadStatus
	}{
		{name: "validated", status: execution_client.PayloadStatusValidated},
		{name: "syncing", status: execution_client.PayloadStatusNotValidated},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg, _, bid, env, anchorRoot := validAnchorEnvelopeFixture(t, 1)
			engine := &testExecutionEngine{payloadStatus: tt.status}
			err := validateAnchorPayloadWithExecutionClient(context.Background(), &Cfg{
				beaconCfg:       cfg,
				executionClient: engine,
				forkChoice:      &forkchoice.ForkChoiceStore{},
			}, anchorRoot, bid, env)
			require.NoError(t, err)
			require.Equal(t, 1, engine.newPayloadCalls)
		})
	}
}

func TestValidateAnchorPayloadWithELReturnsRemoteInvalidation(t *testing.T) {
	cfg, _, bid, env, _ := validAnchorEnvelopeFixture(t, 1)
	engine := &testExecutionEngine{payloadStatus: execution_client.PayloadStatusInvalidated}

	status, err := validateAnchorPayloadWithEL(context.Background(), &Cfg{
		beaconCfg:       cfg,
		executionClient: engine,
	}, bid, env)

	require.NoError(t, err)
	require.EqualValues(t, execution_client.PayloadStatusInvalidated, status)
	require.Equal(t, 1, engine.newPayloadCalls)
}

func TestDrainPendingGloasPayloadsRequeuesNotValidatedPayload(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&cfg)
	blockRoot := common.HexToHash("0x1234")
	parentRoot := common.HexToHash("0x5678")
	execHash := common.HexToHash("0x9abc")
	fc := &forkchoice.ForkChoiceStore{}
	engine := &testExecutionEngine{
		supportInsertion: true,
		payloadStatus:    execution_client.PayloadStatusNotValidated,
	}
	payload := cltypes.NewEth1Block(clparams.GloasVersion, &cfg)
	payload.BlockHash = execHash
	pending := forkchoice.PendingELPayload{
		Block: &cltypes.SignedBeaconBlock{
			Block: &cltypes.BeaconBlock{
				Slot:       1,
				ParentRoot: parentRoot,
				Body: &cltypes.BeaconBody{
					Version: clparams.GloasVersion,
					SignedExecutionPayloadBid: &cltypes.SignedExecutionPayloadBid{
						Message: &cltypes.ExecutionPayloadBid{
							BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48),
						},
					},
				},
			},
		},
		Envelope: &cltypes.SignedExecutionPayloadEnvelope{
			Message: &cltypes.ExecutionPayloadEnvelope{
				BeaconBlockRoot: blockRoot,
				Payload:         payload,
			},
		},
	}
	fc.RequeuePendingELPayload(pending)

	drainPendingGloasPayloads(context.Background(), &Cfg{
		beaconCfg:       &cfg,
		executionClient: engine,
		forkChoice:      fc,
	})

	require.Equal(t, 1, engine.newPayloadCalls)
	queued := fc.DrainPendingELPayloads()
	require.Len(t, queued, 1)
	require.Equal(t, blockRoot, queued[0].Envelope.Message.BeaconBlockRoot)
}

func validAnchorEnvelopeFixture(t *testing.T, builderIndex uint64) (*clparams.BeaconChainConfig, *state2.CachingBeaconState, *cltypes.ExecutionPayloadBid, *cltypes.SignedExecutionPayloadEnvelope, common.Hash) {
	t.Helper()

	cfg := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&cfg)
	cfg.GloasForkEpoch = 0
	cfg.GloasForkVersion = 0x80000038
	cfg.InitializeForkSchedule()

	st := state2.New(&cfg)
	st.SetVersion(clparams.GloasVersion)
	st.SetSlot(64)
	st.SetGenesisValidatorsRoot(common.HexToHash("0x01"))
	st.SetFork(&cltypes.Fork{
		PreviousVersion: utils.Uint32ToBytes4(uint32(cfg.GloasForkVersion)),
		CurrentVersion:  utils.Uint32ToBytes4(uint32(cfg.GloasForkVersion)),
		Epoch:           0,
	})
	st.SetLatestBlockHeader(&cltypes.BeaconBlockHeader{ProposerIndex: 0})

	privKey, err := bls.NewPrivateKeyFromIKM([]byte("01234567890123456789012345678901"))
	require.NoError(t, err)
	pubkey := common.Bytes48(bls.CompressPublicKey(privKey.PublicKey()))

	st.AddValidator(solid.NewValidatorFromParameters(pubkey, common.Hash{}, cfg.MaxEffectiveBalance, false, 0, 0, cfg.FarFutureEpoch, cfg.FarFutureEpoch), cfg.MaxEffectiveBalance)
	builders := solid.NewStaticListSSZ[*cltypes.Builder](64, 73)
	if builderIndex != clparams.BuilderIndexSelfBuild {
		for i := uint64(0); i <= builderIndex; i++ {
			builders.Append(&cltypes.Builder{Pubkey: pubkey})
		}
	}
	st.SetBuilders(builders)

	anchorRoot := common.HexToHash("0x12")
	parentRoot := common.HexToHash("0x11")
	parentHash := common.HexToHash("0x10")
	prevRandao := common.HexToHash("0x13")
	feeRecipient := common.HexToAddress("0x0000000000000000000000000000000000000014")
	requests := cltypes.NewExecutionRequestsWithVersion(&cfg, clparams.GloasVersion)
	requestsRoot, err := requests.HashSSZ()
	require.NoError(t, err)
	requestsHash := cltypes.ComputeExecutionRequestHash(cltypes.GetExecutionRequestsList(&cfg, requests))

	payload := cltypes.NewEth1Block(clparams.GloasVersion, &cfg)
	payload.ParentHash = parentHash
	payload.FeeRecipient = feeRecipient
	payload.PrevRandao = prevRandao
	payload.GasLimit = 30_000_000
	payload.SlotNumber = 64
	payload.Extra = solid.NewExtraData()
	payload.Transactions = &solid.TransactionsSSZ{}
	payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(cfg.MaxWithdrawalsPerPayload), 44)
	payload.BlockAccessList = solid.NewByteListSSZ(cfg.MaxBytesPerTransaction)
	payload.BlockHash = anchorPayloadHeaderHash(t, payload, parentRoot, requestsHash)

	bid := &cltypes.ExecutionPayloadBid{
		ParentBlockHash:       parentHash,
		ParentBlockRoot:       parentRoot,
		BlockHash:             payload.BlockHash,
		PrevRandao:            prevRandao,
		FeeRecipient:          feeRecipient,
		GasLimit:              payload.GasLimit,
		BuilderIndex:          builderIndex,
		Slot:                  payload.SlotNumber,
		BlobKzgCommitments:    *solid.NewStaticListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48),
		ExecutionRequestsRoot: requestsRoot,
	}
	env := &cltypes.SignedExecutionPayloadEnvelope{
		Message: &cltypes.ExecutionPayloadEnvelope{
			Payload:               payload,
			ExecutionRequests:     requests,
			BuilderIndex:          builderIndex,
			BeaconBlockRoot:       anchorRoot,
			ParentBeaconBlockRoot: parentRoot,
		},
	}
	signAnchorEnvelope(t, st, privKey, env, bid.Slot)
	return &cfg, st, bid, env, anchorRoot
}

func signAnchorEnvelope(t *testing.T, st *state2.CachingBeaconState, privKey *bls.PrivateKey, env *cltypes.SignedExecutionPayloadEnvelope, slot uint64) {
	t.Helper()

	domain, err := st.GetDomain(st.BeaconConfig().DomainBeaconBuilder, state2.GetEpochAtSlot(st.BeaconConfig(), slot))
	require.NoError(t, err)
	signingRoot, err := fork.ComputeSigningRoot(env.Message, domain)
	require.NoError(t, err)
	copy(env.Signature[:], privKey.Sign(signingRoot[:]).Bytes())
}

func anchorPayloadHeaderHash(t *testing.T, payload *cltypes.Eth1Block, parentRoot common.Hash, requestsHash common.Hash) common.Hash {
	t.Helper()

	withdrawalsHash := types.DeriveSha(types.Withdrawals(nil))
	blobGasUsed := payload.BlobGasUsed
	excessBlobGas := payload.ExcessBlobGas
	blockAccessListHash := empty.BlockAccessListHash
	slotNumber := payload.SlotNumber
	header := &types.Header{
		ParentHash:            payload.ParentHash,
		UncleHash:             empty.UncleHash,
		Coinbase:              payload.FeeRecipient,
		Root:                  payload.StateRoot,
		TxHash:                types.DeriveSha(types.BinaryTransactions(nil)),
		ReceiptHash:           payload.ReceiptsRoot,
		Bloom:                 payload.LogsBloom,
		Difficulty:            *merge.ProofOfStakeDifficulty,
		GasLimit:              payload.GasLimit,
		GasUsed:               payload.GasUsed,
		Time:                  payload.Time,
		Extra:                 nil,
		MixDigest:             payload.PrevRandao,
		Nonce:                 merge.ProofOfStakeNonce,
		BaseFee:               new(uint256.Int),
		WithdrawalsHash:       &withdrawalsHash,
		ParentBeaconBlockRoot: &parentRoot,
		BlobGasUsed:           &blobGasUsed,
		ExcessBlobGas:         &excessBlobGas,
		RequestsHash:          &requestsHash,
		BlockAccessListHash:   &blockAccessListHash,
		SlotNumber:            &slotNumber,
	}
	header.Number.SetUint64(payload.BlockNumber)
	return header.Hash()
}

type testExecutionEngine struct {
	supportInsertion bool
	payloadStatus    execution_client.PayloadStatus
	newPayloadCalls  int
}

func (t *testExecutionEngine) NewPayload(context.Context, *cltypes.Eth1Block, *common.Hash, []common.Hash, []hexutil.Bytes) (execution_client.PayloadStatus, error) {
	t.newPayloadCalls++
	return t.payloadStatus, nil
}

func (t *testExecutionEngine) ForkChoiceUpdate(context.Context, common.Hash, common.Hash, common.Hash, *engine_types.PayloadAttributes, clparams.StateVersion) ([]byte, error) {
	return nil, nil
}

func (t *testExecutionEngine) SupportInsertion() bool { return t.supportInsertion }

func (t *testExecutionEngine) InsertBlocks(context.Context, []*types.Block) error {
	return nil
}

func (t *testExecutionEngine) InsertBlock(context.Context, *types.Block) error { return nil }

func (t *testExecutionEngine) CurrentHeader(context.Context) (*types.Header, error) { return nil, nil }

func (t *testExecutionEngine) IsCanonicalHash(context.Context, common.Hash) (bool, error) {
	return false, nil
}

func (t *testExecutionEngine) Ready(context.Context) (bool, error) { return true, nil }

func (t *testExecutionEngine) GetBodiesByRange(context.Context, uint64, uint64) ([]*types.RawBody, error) {
	return nil, nil
}

func (t *testExecutionEngine) GetBodiesByHashes(context.Context, []common.Hash) ([]*types.RawBody, error) {
	return nil, nil
}

func (t *testExecutionEngine) HasBlock(context.Context, common.Hash) (bool, error) { return false, nil }

func (t *testExecutionEngine) FrozenBlocks(context.Context) uint64 { return 0 }

func (t *testExecutionEngine) HasGapInSnapshots(context.Context) bool { return false }

func (t *testExecutionEngine) GetAssembledBlock(context.Context, []byte, clparams.StateVersion) (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
	return nil, nil, nil, nil, nil
}

func (t *testExecutionEngine) GetBlobs(context.Context, []common.Hash, clparams.StateVersion) ([][]byte, [][][]byte, error) {
	return nil, nil, nil
}

func (t *testExecutionEngine) GetClientVersionV1(context.Context, *engine_types.ClientVersionV1) ([]engine_types.ClientVersionV1, error) {
	return nil, nil
}

var _ execution_client.ExecutionEngine = (*testExecutionEngine)(nil)
