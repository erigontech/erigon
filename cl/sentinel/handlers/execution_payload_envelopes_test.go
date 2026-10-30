package handlers

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"maps"
	"os"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/clparams/initial_state"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/envelope_history"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/sentinel/communication"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/snappypool"
	"github.com/erigontech/erigon/db/kv"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

type testEnvelopeRebuilder struct {
	envelopes map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope
	prepared  map[*envelope_history.PreparedRebuild]common.Hash
	rejected  map[common.Hash]struct{}
	onRebuild func()
	err       error
	called    []common.Hash
}

type envelopeHandlerStream struct {
	network.Stream
	request  *bytes.Reader
	response bytes.Buffer
	conn     network.Conn
	protocol protocol.ID
	expired  bool
}

func (s *envelopeHandlerStream) Read(p []byte) (int, error) { return s.request.Read(p) }
func (s *envelopeHandlerStream) Write(p []byte) (int, error) {
	if s.expired {
		return 0, os.ErrDeadlineExceeded
	}
	return s.response.Write(p)
}
func (s *envelopeHandlerStream) Conn() network.Conn    { return s.conn }
func (s *envelopeHandlerStream) Protocol() protocol.ID { return s.protocol }

func (r *testEnvelopeRebuilder) Prepare(_ context.Context, _ kv.Tx, _ uint64, blockRoot common.Hash) (*envelope_history.PreparedRebuild, error) {
	if _, rejected := r.rejected[blockRoot]; rejected {
		return nil, nil
	}
	prepared := new(envelope_history.PreparedRebuild)
	if r.prepared == nil {
		r.prepared = make(map[*envelope_history.PreparedRebuild]common.Hash)
	}
	r.prepared[prepared] = blockRoot
	return prepared, nil
}

func (r *testEnvelopeRebuilder) TryRebuild(_ context.Context, prepared *envelope_history.PreparedRebuild) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	blockRoot := r.prepared[prepared]
	r.called = append(r.called, blockRoot)
	if r.onRebuild != nil {
		r.onRebuild()
	}
	return r.envelopes[blockRoot], r.err
}

// getGloasEthClockAndConfig returns an EthereumClock and BeaconChainConfig
// with all fork epochs set to 0, so that GLOAS is active from the start.
func getGloasEthClockAndConfig(t *testing.T) (eth_clock.EthereumClock, *clparams.BeaconChainConfig) {
	s, err := initial_state.GetGenesisState(t.Context(), chainspec.MainnetChainID)
	require.NoError(t, err)

	cfg := s.BeaconConfig()
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0
	cfg.InitializeForkSchedule()

	clock := eth_clock.NewEthereumClock(s.GenesisTime(), s.GenesisValidatorsRoot(), cfg)
	return clock, cfg
}

type executionPayloadEnvelopesByRangeTestCase struct {
	name                string
	headPayloadStatus   cltypes.PayloadStatus
	unavailable         bool
	overflow            bool
	missingFirstFull    bool
	missingMiddleFull   bool
	incompleteBlock     bool
	incompleteBody      bool
	emptyRange          bool
	headIndexMismatch   bool
	historicalMismatch  bool
	headAncestorMatches bool
	requestCount        uint64
	canonicalBlockCount uint64
	allPayloadsFull     bool
	allPayloadsEmpty    bool
	incompleteBlockAt   uint64
	startAtGenesis      bool
	serveUnfinalized    bool
	verifyRateLimit     bool
	wantEnvelopeCount   int
	wantResponsePrefix  byte
}

func TestExecutionPayloadServeEpochIncludesUnfinalizedHistory(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.GloasForkEpoch = 10
	currentEpoch := cfg.MinEpochsForBlockRequests() + 100

	for _, tc := range []struct {
		name           string
		currentEpoch   uint64
		finalizedEpoch uint64
		want           uint64
	}{
		{name: "finalized before request window", currentEpoch: currentEpoch, finalizedEpoch: 20, want: 20},
		{name: "finalized before Gloas", currentEpoch: currentEpoch, finalizedEpoch: 5, want: 10},
		{name: "finalized at request window", currentEpoch: currentEpoch, finalizedEpoch: 100, want: 100},
		{name: "finalized after request window", currentEpoch: currentEpoch, finalizedEpoch: 101, want: 100},
		{name: "current epoch inside request window", currentEpoch: 50, finalizedEpoch: 5, want: 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, minExecutionPayloadServeEpoch(&cfg, tc.currentEpoch, tc.finalizedEpoch))
		})
	}
}

func TestExecutionPayloadEnvelopesByRangeHandler(t *testing.T) {
	for _, tc := range []executionPayloadEnvelopesByRangeTestCase{
		{name: "empty head payload", headPayloadStatus: cltypes.PayloadStatusEmpty},
		{name: "full head payload", headPayloadStatus: cltypes.PayloadStatusFull},
		{name: "unfinalized history before request window", headPayloadStatus: cltypes.PayloadStatusFull, serveUnfinalized: true},
		{name: "pending head payload", headPayloadStatus: cltypes.PayloadStatusPending},
		{name: "unavailable history", headPayloadStatus: cltypes.PayloadStatusFull, unavailable: true, wantResponsePrefix: ResourceUnavailablePrefix},
		{name: "overflowing range", headPayloadStatus: cltypes.PayloadStatusFull, overflow: true, wantResponsePrefix: InvalidRequestPrefix},
		{name: "missing first full envelope", headPayloadStatus: cltypes.PayloadStatusFull, missingFirstFull: true, wantResponsePrefix: ResourceUnavailablePrefix},
		{name: "missing middle full envelope", headPayloadStatus: cltypes.PayloadStatusFull, missingMiddleFull: true},
		{name: "incomplete canonical block", headPayloadStatus: cltypes.PayloadStatusFull, incompleteBlock: true, wantResponsePrefix: ResourceUnavailablePrefix},
		{name: "canonical block with nil body", headPayloadStatus: cltypes.PayloadStatusFull, incompleteBody: true, wantResponsePrefix: ResourceUnavailablePrefix},
		{name: "empty slot range ignores later incomplete block", headPayloadStatus: cltypes.PayloadStatusFull, incompleteBlock: true, emptyRange: true},
		{name: "head and canonical index mismatch", headPayloadStatus: cltypes.PayloadStatusFull, headIndexMismatch: true, wantResponsePrefix: ResourceUnavailablePrefix},
		{
			name:                "historical range remains available while canonical head catches up",
			headPayloadStatus:   cltypes.PayloadStatusFull,
			headIndexMismatch:   true,
			historicalMismatch:  true,
			headAncestorMatches: true,
			requestCount:        3,
		},
		{
			name:               "historical range on stale branch is unavailable",
			headPayloadStatus:  cltypes.PayloadStatusFull,
			headIndexMismatch:  true,
			historicalMismatch: true,
			requestCount:       3,
			wantResponsePrefix: ResourceUnavailablePrefix,
		},
		{
			name:                "first full payload at scan limit is returned",
			headPayloadStatus:   cltypes.PayloadStatusFull,
			requestCount:        129,
			canonicalBlockCount: 129,
			allPayloadsEmpty:    true,
		},
		{
			name:                "response remains capped for a larger request span",
			headPayloadStatus:   cltypes.PayloadStatusFull,
			requestCount:        129,
			canonicalBlockCount: 129,
			allPayloadsFull:     true,
			wantEnvelopeCount:   128,
		},
		{
			name:                "exhausted candidate scan is unavailable",
			headPayloadStatus:   cltypes.PayloadStatusEmpty,
			requestCount:        130,
			canonicalBlockCount: 130,
			allPayloadsEmpty:    true,
			incompleteBlockAt:   129,
			wantResponsePrefix:  ResourceUnavailablePrefix,
		},
		{
			name:                "first full payload beyond scan limit is unavailable",
			headPayloadStatus:   cltypes.PayloadStatusFull,
			requestCount:        130,
			canonicalBlockCount: 130,
			allPayloadsEmpty:    true,
			wantResponsePrefix:  ResourceUnavailablePrefix,
		},
		{
			name:              "maximum count pays the bounded response cost",
			headPayloadStatus: cltypes.PayloadStatusFull,
			requestCount:      ^uint64(0),
			startAtGenesis:    true,
			verifyRateLimit:   true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testExecutionPayloadEnvelopesByRangeHandler(t, tc)
		})
	}
}

func testExecutionPayloadEnvelopesByRangeHandler(
	t *testing.T,
	tc executionPayloadEnvelopesByRangeTestCase,
) {
	ctx := t.Context()

	// Set up two connected libp2p hosts
	host, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { host.Close() })

	host1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { host1.Close() })

	err = host.Connect(ctx, peer.AddrInfo{
		ID:    host1.ID(),
		Addrs: host1.Addrs(),
	})
	require.NoError(t, err)
	_, indiciesDB := setupStore(t)
	store := tests.NewMockBlockReader()

	tx, err := indiciesDB.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	ethClock, beaconCfg := getGloasEthClockAndConfig(t)

	// Keep the default range inside the request window.
	count := tc.canonicalBlockCount
	if count == 0 {
		count = 5
	}
	startSlot := ethClock.GetCurrentSlot() - count - 5
	finalizedEpoch := ethClock.GetCurrentEpoch()
	if tc.serveUnfinalized {
		require.Greater(t, finalizedEpoch, beaconCfg.MinEpochsForBlockRequests()+2)
		windowLowerBound := finalizedEpoch - beaconCfg.MinEpochsForBlockRequests()
		startSlot = (windowLowerBound - 1) * beaconCfg.SlotsPerEpoch
		finalizedEpoch = windowLowerBound - 2
	}

	// Populate database with blocks (needed for canonical root lookup)
	expBlocks := populateDatabaseWithBlocks(t, store, tx, startSlot, count-1)
	require.NoError(t, tx.Commit())

	// Create mock fork choice with envelopes
	fcMock := mock_services.NewForkChoiceStorageMock(t)
	fcMock.FinalizedCheckpointVal = solid.Checkpoint{Epoch: finalizedEpoch}

	// Create envelopes for each block and store them in mock.
	// The canonical block root is HashSSZ(header), computed by WriteBeaconBlockHeaderAndIndicies.
	expEnvelopes := make([]*cltypes.SignedExecutionPayloadEnvelope, 0, count)
	canonicalRoots := make([]common.Hash, 0, count)
	for i, block := range expBlocks {
		if uint64(i) >= count {
			break
		}
		bodyRoot, err := block.Block.Body.HashSSZ()
		require.NoError(t, err)

		// Compute the block root (hash of the header) — same as what
		// WriteBeaconBlockHeaderAndIndicies stores as canonical root.
		header := &cltypes.BeaconBlockHeader{
			Slot:          block.Block.Slot,
			ParentRoot:    block.Block.ParentRoot,
			ProposerIndex: block.Block.ProposerIndex,
			Root:          block.Block.StateRoot,
			BodyRoot:      bodyRoot,
		}
		blockRoot, err := header.HashSSZ()
		require.NoError(t, err)
		canonicalRoots = append(canonicalRoots, blockRoot)

		// Create a properly versioned Eth1Block for GLOAS
		payload := cltypes.NewEth1Block(clparams.GloasVersion, beaconCfg)
		payload.Transactions = &solid.TransactionsSSZ{}
		payload.Extra = solid.NewExtraData()
		payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(beaconCfg.MaxWithdrawalsPerPayload), 44)
		envelope := &cltypes.SignedExecutionPayloadEnvelope{
			Message: &cltypes.ExecutionPayloadEnvelope{
				Payload:           payload,
				ExecutionRequests: cltypes.NewExecutionRequestsWithVersion(beaconCfg, clparams.GloasVersion),
			},
		}
		envelope.Message.BeaconBlockRoot = blockRoot
		envelope.Message.BuilderIndex = uint64(i)

		fcMock.SetEnvelope(blockRoot, envelope)
		if tc.allPayloadsFull || (!tc.allPayloadsEmpty && (i == int(count)-3 || i == int(count)-2)) {
			expEnvelopes = append(expEnvelopes, envelope)
		} else if tc.headPayloadStatus == cltypes.PayloadStatusFull {
			if i == int(count)-1 {
				expEnvelopes = append(expEnvelopes, envelope)
			}
		}
	}
	for i, root := range canonicalRoots {
		block := cltypes.NewSignedBeaconBlock(beaconCfg, clparams.GloasVersion)
		block.Block.Slot = startSlot + uint64(i)
		if i > 0 {
			block.Block.ParentRoot = canonicalRoots[i-1]
		}
		block.Block.Body.SignedExecutionPayloadBid.Message.BlockHash = common.Hash{byte(i + 1)}
		if (tc.allPayloadsFull && i > 0) || (!tc.allPayloadsEmpty && (i == int(count)-2 || i == int(count)-1)) {
			block.Block.Body.SignedExecutionPayloadBid.Message.ParentBlockHash = common.Hash{byte(i)}
		}
		fcMock.Blocks[root] = block
	}
	fcMock.HeadVal = canonicalRoots[count-1]
	fcMock.HeadSlotVal = startSlot + count - 1
	fcMock.HeadPayloadStatusVal = tc.headPayloadStatus
	if tc.unavailable {
		lowestAvailableSlot := startSlot + 1
		fcMock.LowestAvailableSlotVal = &lowestAvailableSlot
	}
	if tc.missingFirstFull {
		fcMock.DeleteEnvelope(canonicalRoots[2])
	}
	if tc.missingMiddleFull {
		fcMock.DeleteEnvelope(canonicalRoots[3])
		expEnvelopes = expEnvelopes[:1]
	}
	if tc.incompleteBlock {
		incompleteRoot := canonicalRoots[1]
		if tc.emptyRange {
			incompleteRoot = canonicalRoots[0]
		}
		fcMock.Blocks[incompleteRoot] = &cltypes.SignedBeaconBlock{}
	}
	if tc.incompleteBlockAt != 0 {
		fcMock.Blocks[canonicalRoots[tc.incompleteBlockAt]] = &cltypes.SignedBeaconBlock{}
	}
	if tc.incompleteBody {
		fcMock.Blocks[canonicalRoots[1]].Block.Body = nil
	}
	if tc.headIndexMismatch {
		fcMock.HeadVal = common.Hash{0xee}
	}
	if tc.historicalMismatch {
		expEnvelopes = expEnvelopes[:1]
		if tc.headAncestorMatches {
			ancestorIndex := tc.requestCount
			fcMock.Ancestors[startSlot+ancestorIndex] = forkchoice.ForkChoiceNode{Root: canonicalRoots[ancestorIndex]}
		}
	}
	if tc.wantEnvelopeCount != 0 {
		expEnvelopes = expEnvelopes[:tc.wantEnvelopeCount]
	}

	c := NewConsensusHandlers(
		ctx,
		store,
		indiciesDB,
		host,
		&clparams.NetworkConfig{},
		nil,
		beaconCfg,
		ethClock,
		nil, fcMock, nil, nil, nil, true, nil,
	)
	c.Start()

	// Encode the request
	req := &cltypes.ExecutionPayloadEnvelopesByRangeRequest{
		StartSlot: startSlot,
		Count:     count,
	}
	if tc.requestCount != 0 {
		req.Count = tc.requestCount
	}
	if tc.startAtGenesis {
		req.StartSlot = 0
	}
	if tc.overflow {
		req.StartSlot = ^uint64(0)
		req.Count = 1
	}
	if tc.emptyRange {
		req.StartSlot = startSlot - 2
		req.Count = 1
		expEnvelopes = nil
	}
	var reqBuf bytes.Buffer
	err = ssz_snappy.EncodeAndWrite(&reqBuf, req)
	require.NoError(t, err)

	// Open stream to the handler
	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.ExecutionPayloadEnvelopesByRangeProtocolV1))
	require.NoError(t, err)

	_, err = stream.Write(reqBuf.Bytes())
	require.NoError(t, err)
	if tc.wantResponsePrefix != SuccessfulResponsePrefix {
		firstByte := make([]byte, 1)
		_, err = io.ReadFull(stream, firstByte)
		require.NoError(t, err)
		require.Equal(t, tc.wantResponsePrefix, firstByte[0])
		return
	}

	// Read response chunks
	sr := snappypool.Reader(stream)
	defer snappypool.PutReader(sr)
	for i := 0; i < len(expEnvelopes); i++ {
		// Read success byte
		firstByte := make([]byte, 1)
		_, err = stream.Read(firstByte)
		require.NoError(t, err)
		require.Equal(t, byte(SuccessfulResponsePrefix), firstByte[0], "expected success prefix for envelope %d", i)

		// Read fork digest (4 bytes)
		forkDigest := make([]byte, 4)
		_, err = stream.Read(forkDigest)
		require.NoError(t, err)

		respForkDigest := binary.BigEndian.Uint32(forkDigest)
		require.NotZero(t, respForkDigest, "fork digest should not be zero")

		// StateVersionByForkDigest may return FuluVersion for GLOAS-era XOR'd digests
		// that aren't in the pre-computed digest map — this is expected behavior.
		version, err := ethClock.StateVersionByForkDigest(utils.Uint32ToBytes4(respForkDigest))
		require.NoError(t, err)
		require.True(t, version >= clparams.FuluVersion, "expected Fulu+ version for envelope %d, got %d", i, version)

		// Read SSZ-snappy encoded envelope
		encodedLn, err := ssz_snappy.ReadUvarint(stream)
		require.NoError(t, err)

		raw := make([]byte, encodedLn)
		sr.Reset(stream)
		bytesRead := 0
		for bytesRead < int(encodedLn) {
			n, err := sr.Read(raw[bytesRead:])
			require.NoError(t, err)
			bytesRead += n
		}

		// Decode the envelope using GloasVersion (we know the data is from GLOAS fork)
		envelope := &cltypes.SignedExecutionPayloadEnvelope{
			Message: cltypes.NewExecutionPayloadEnvelope(beaconCfg),
		}
		err = envelope.DecodeSSZ(raw, int(clparams.GloasVersion))
		require.NoError(t, err)

		// Verify fields
		require.Equal(t, expEnvelopes[i].Message.BuilderIndex, envelope.Message.BuilderIndex)
	}

	// Verify stream is exhausted
	_, err = stream.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF, "stream should be empty after all envelopes")
	if tc.verifyRateLimit {
		stream, err = host1.NewStream(ctx, host.ID(), protocol.ID(communication.ExecutionPayloadEnvelopesByRangeProtocolV1))
		require.NoError(t, err)
		_, err = stream.Write(reqBuf.Bytes())
		require.NoError(t, err)
		firstByte := make([]byte, 1)
		_, err = io.ReadFull(stream, firstByte)
		require.NoError(t, err)
		require.Equal(t, byte(InvalidRequestPrefix), firstByte[0])
	}
}

func TestExecutionPayloadEnvelopesByRootHandler(t *testing.T) {
	ctx := context.Background()

	host, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { host.Close() })

	host1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { host1.Close() })

	err = host.Connect(ctx, peer.AddrInfo{
		ID:    host1.ID(),
		Addrs: host1.Addrs(),
	})
	require.NoError(t, err)

	_, indiciesDB := setupStore(t)
	store := tests.NewMockBlockReader()

	tx, err := indiciesDB.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	ethClock, beaconCfg := getGloasEthClockAndConfig(t)

	// Keep the requested roots inside the request window.
	startSlot := ethClock.GetCurrentSlot() - 10
	count := uint64(5)

	expBlocks := populateDatabaseWithBlocks(t, store, tx, startSlot, count)
	require.NoError(t, tx.Commit())

	fcMock := mock_services.NewForkChoiceStorageMock(t)
	fcMock.FinalizedCheckpointVal = solid.Checkpoint{Epoch: ethClock.GetCurrentEpoch()}

	// Create envelopes keyed by block root
	expEnvelopes := make([]*cltypes.SignedExecutionPayloadEnvelope, 0, count)
	blockRoots := make([]common.Hash, 0, count)
	for i, block := range expBlocks {
		if uint64(i) >= count {
			break
		}
		bodyRoot, err := block.Block.Body.HashSSZ()
		require.NoError(t, err)

		header := &cltypes.BeaconBlockHeader{
			Slot:          block.Block.Slot,
			ParentRoot:    block.Block.ParentRoot,
			ProposerIndex: block.Block.ProposerIndex,
			Root:          block.Block.StateRoot,
			BodyRoot:      bodyRoot,
		}
		blockRoot, err := header.HashSSZ()
		require.NoError(t, err)

		payload := cltypes.NewEth1Block(clparams.GloasVersion, beaconCfg)
		payload.Transactions = &solid.TransactionsSSZ{}
		payload.Extra = solid.NewExtraData()
		payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(beaconCfg.MaxWithdrawalsPerPayload), 44)
		envelope := &cltypes.SignedExecutionPayloadEnvelope{
			Message: &cltypes.ExecutionPayloadEnvelope{
				Payload:           payload,
				ExecutionRequests: cltypes.NewExecutionRequestsWithVersion(beaconCfg, clparams.GloasVersion),
			},
		}
		envelope.Message.BeaconBlockRoot = blockRoot
		envelope.Message.BuilderIndex = uint64(i)

		fcMock.SetEnvelope(blockRoot, envelope)
		expEnvelopes = append(expEnvelopes, envelope)
		blockRoots = append(blockRoots, blockRoot)
	}

	c := NewConsensusHandlers(
		ctx,
		store,
		indiciesDB,
		host,
		&clparams.NetworkConfig{},
		nil,
		beaconCfg,
		ethClock,
		nil, fcMock, nil, nil, nil, true, nil,
	)
	c.Start()

	// Encode the request: List[Root, MAX_REQUEST_PAYLOADS]
	reqRoots := solid.NewHashList(int(beaconCfg.MaxRequestPayloads))
	for _, root := range blockRoots {
		reqRoots.Append(root)
	}
	var reqBuf bytes.Buffer
	err = ssz_snappy.EncodeAndWrite(&reqBuf, reqRoots)
	require.NoError(t, err)

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.ExecutionPayloadEnvelopesByRootProtocolV1))
	require.NoError(t, err)

	_, err = stream.Write(reqBuf.Bytes())
	require.NoError(t, err)

	// Read response chunks
	sr := snappypool.Reader(stream)
	defer snappypool.PutReader(sr)
	for i := 0; i < len(expEnvelopes); i++ {
		firstByte := make([]byte, 1)
		_, err = stream.Read(firstByte)
		require.NoError(t, err)
		require.Equal(t, byte(SuccessfulResponsePrefix), firstByte[0], "expected success prefix for envelope %d", i)

		forkDigest := make([]byte, 4)
		_, err = stream.Read(forkDigest)
		require.NoError(t, err)

		respForkDigest := binary.BigEndian.Uint32(forkDigest)
		require.NotZero(t, respForkDigest, "fork digest should not be zero")

		version, err := ethClock.StateVersionByForkDigest(utils.Uint32ToBytes4(respForkDigest))
		require.NoError(t, err)
		require.True(t, version >= clparams.FuluVersion, "expected Fulu+ version for envelope %d, got %d", i, version)

		encodedLn, err := ssz_snappy.ReadUvarint(stream)
		require.NoError(t, err)

		raw := make([]byte, encodedLn)
		sr.Reset(stream)
		bytesRead := 0
		for bytesRead < int(encodedLn) {
			n, err := sr.Read(raw[bytesRead:])
			require.NoError(t, err)
			bytesRead += n
		}

		envelope := &cltypes.SignedExecutionPayloadEnvelope{
			Message: cltypes.NewExecutionPayloadEnvelope(beaconCfg),
		}
		err = envelope.DecodeSSZ(raw, int(clparams.GloasVersion))
		require.NoError(t, err)

		require.Equal(t, expEnvelopes[i].Message.BuilderIndex, envelope.Message.BuilderIndex)
	}

	_, err = stream.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF, "stream should be empty after all envelopes")
}

func TestExecutionPayloadEnvelopesByRootRebuildsMissingCanonicalEnvelope(t *testing.T) {
	ctx := t.Context()
	_, indiciesDB := setupStore(t)
	store := tests.NewMockBlockReader()
	tx, err := indiciesDB.BeginRw(ctx)
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	ethClock, beaconCfg := getGloasEthClockAndConfig(t)
	slot := ethClock.GetCurrentSlot() - 1
	blocks := populateDatabaseWithBlocks(t, store, tx, slot, 0)
	require.NoError(t, tx.Commit())
	root, err := blocks[0].Block.HashSSZ()
	require.NoError(t, err)
	payload := cltypes.NewEth1Block(clparams.GloasVersion, beaconCfg)
	payload.Extra = solid.NewExtraData()
	payload.Transactions = solid.NewProgressiveTransactionsSSZ()
	payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(beaconCfg.MaxWithdrawalsPerPayload), 44)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(beaconCfg)}
	envelope.Message.Payload = payload
	envelope.Message.BeaconBlockRoot = root
	rebuilder := &testEnvelopeRebuilder{envelopes: map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{root: envelope}}
	fcMock := mock_services.NewForkChoiceStorageMock(t)
	fcMock.FinalizedCheckpointVal = solid.Checkpoint{Epoch: ethClock.GetCurrentEpoch()}
	c := NewConsensusHandlers(ctx, store, indiciesDB, nil, &clparams.NetworkConfig{}, nil, beaconCfg, ethClock, nil, fcMock, nil, nil, nil, true, rebuilder)

	roots := solid.NewHashList(int(beaconCfg.MaxRequestPayloads))
	roots.Append(root)
	stream := newRootHandlerTestStream(roots, peer.ID("history-peer"))
	require.Equal(t, byte(SuccessfulResponsePrefix), stream.run(t, c)[0])
	require.Len(t, rebuilder.called, 1)
}

func TestExecutionPayloadEnvelopesByRootServesStoredEnvelopesBeforeRebuilds(t *testing.T) {
	ctx := t.Context()
	_, indiciesDB := setupStore(t)
	store := tests.NewMockBlockReader()
	ethClock, cfg := getGloasEthClockAndConfig(t)
	tx, err := indiciesDB.BeginRw(ctx)
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	blocks := populateDatabaseWithBlocks(t, store, tx, ethClock.GetCurrentSlot()-2, 1)
	require.NoError(t, tx.Commit())

	roots := solid.NewHashList(int(cfg.MaxRequestPayloads))
	for _, block := range blocks {
		root, err := block.Block.HashSSZ()
		require.NoError(t, err)
		roots.Append(root)
	}
	storedEnvelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
	storedEnvelope.Message.BuilderIndex = 99
	fc := mock_services.NewForkChoiceStorageMock(t)
	fc.FinalizedCheckpointVal = solid.Checkpoint{Epoch: ethClock.GetCurrentEpoch()}
	fc.SetEnvelope(roots.Get(1), storedEnvelope)
	rebuilder := &testEnvelopeRebuilder{err: context.DeadlineExceeded}
	c := NewConsensusHandlers(ctx, store, indiciesDB, nil, &clparams.NetworkConfig{}, nil, cfg, ethClock, nil, fc, nil, nil, nil, true, rebuilder)
	stream := newRootHandlerTestStream(roots, peer.ID("history-peer"))
	rebuilder.onRebuild = func() { stream.expired = true }

	require.Equal(t, []uint64{99}, readEnvelopeBuilderIndices(t, stream.run(t, c), cfg))
	require.Len(t, rebuilder.called, 1)
}

func TestExecutionPayloadEnvelopesByRootRebuildBudgetIsPerPeer(t *testing.T) {
	ctx := t.Context()
	_, indiciesDB := setupStore(t)
	store := tests.NewMockBlockReader()
	ethClock, cfg := getGloasEthClockAndConfig(t)
	tx, err := indiciesDB.BeginRw(ctx)
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	blocks := populateDatabaseWithBlocks(t, store, tx, ethClock.GetCurrentSlot()-2, 1)
	require.NoError(t, tx.Commit())
	roots := solid.NewHashList(int(cfg.MaxRequestPayloads))
	for _, block := range blocks {
		root, err := block.Block.HashSSZ()
		require.NoError(t, err)
		roots.Append(root)
	}
	rebuiltEnvelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
	rebuiltEnvelope.Message.BuilderIndex = 1
	rebuilder := &testEnvelopeRebuilder{envelopes: map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{roots.Get(1): rebuiltEnvelope}}
	fc := mock_services.NewForkChoiceStorageMock(t)
	fc.FinalizedCheckpointVal = solid.Checkpoint{Epoch: ethClock.GetCurrentEpoch()}
	storedEnvelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
	storedEnvelope.Message.BuilderIndex = 99
	fc.SetEnvelope(roots.Get(0), storedEnvelope)
	c := NewConsensusHandlers(ctx, store, indiciesDB, nil, &clparams.NetworkConfig{}, nil, cfg, ethClock, nil, fc, nil, nil, nil, true, rebuilder)
	run := func(peerID peer.ID) []uint64 {
		return readEnvelopeBuilderIndices(t, newRootHandlerTestStream(roots, peerID).run(t, c), cfg)
	}

	peerA := peer.ID("peer-a")
	bucket := c.rateLimiter.tryStartEnvelopeRebuild(peerA.String())
	require.NotNil(t, bucket)
	c.rateLimiter.finishEnvelopeRebuild(bucket, 2*time.Second, true)
	require.Equal(t, []uint64{99}, run(peerA))
	require.Empty(t, rebuilder.called)
	require.Equal(t, []uint64{99, 1}, run(peer.ID("peer-b")))
	require.Len(t, rebuilder.called, 1)
}

func TestExecutionPayloadEnvelopesByRootRebuildBudgetIgnoresCheapRejections(t *testing.T) {
	const cheapRejections = 33
	ctx := t.Context()
	_, indiciesDB := setupStore(t)
	store := tests.NewMockBlockReader()
	ethClock, cfg := getGloasEthClockAndConfig(t)
	startSlot := ethClock.GetCurrentSlot() - cheapRejections - 2
	tx, err := indiciesDB.BeginRw(ctx)
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	blocks := populateDatabaseWithBlocks(t, store, tx, startSlot, cheapRejections)
	require.NoError(t, tx.Commit())

	roots := solid.NewHashList(int(cfg.MaxRequestPayloads))
	rejected := make(map[common.Hash]struct{}, cheapRejections)
	for i, block := range blocks {
		root, err := block.Block.HashSSZ()
		require.NoError(t, err)
		roots.Append(root)
		if i < cheapRejections {
			rejected[root] = struct{}{}
		}
	}
	lastRoot, err := blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)
	lastEnvelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
	lastEnvelope.Message.BuilderIndex = 999
	rebuilder := &testEnvelopeRebuilder{
		envelopes: map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{lastRoot: lastEnvelope},
		rejected:  rejected,
	}
	fc := mock_services.NewForkChoiceStorageMock(t)
	fc.FinalizedCheckpointVal = solid.Checkpoint{Epoch: ethClock.GetCurrentEpoch()}
	c := NewConsensusHandlers(ctx, store, indiciesDB, nil, &clparams.NetworkConfig{}, nil, cfg, ethClock, nil, fc, nil, nil, nil, true, rebuilder)

	data := newRootHandlerTestStream(roots, peer.ID("history-peer")).run(t, c)
	require.Equal(t, []uint64{999}, readEnvelopeBuilderIndices(t, data, cfg))
	require.Len(t, rebuilder.called, 1)
}

func TestExecutionPayloadEnvelopesByRootRebuildsMoreThanThirtyTwoEnvelopes(t *testing.T) {
	const rebuildCount = 33
	ctx := t.Context()
	_, indiciesDB := setupStore(t)
	store := tests.NewMockBlockReader()
	ethClock, cfg := getGloasEthClockAndConfig(t)
	startSlot := ethClock.GetCurrentSlot() - rebuildCount - 1
	tx, err := indiciesDB.BeginRw(ctx)
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	roots, envelopes := populateGloasEnvelopeHistory(t, store, tx, cfg, startSlot, rebuildCount, -1)
	require.NoError(t, tx.Commit())

	requestRoots := solid.NewHashList(int(cfg.MaxRequestPayloads))
	wantIndices := make([]uint64, 0, rebuildCount)
	for i, root := range roots {
		requestRoots.Append(root)
		wantIndices = append(wantIndices, uint64(i))
	}
	rebuilder := &testEnvelopeRebuilder{envelopes: envelopes}
	fc := mock_services.NewForkChoiceStorageMock(t)
	fc.FinalizedCheckpointVal = solid.Checkpoint{Epoch: ethClock.GetCurrentEpoch()}
	c := NewConsensusHandlers(ctx, store, indiciesDB, nil, &clparams.NetworkConfig{}, nil, cfg, ethClock, nil, fc, nil, nil, nil, true, rebuilder)
	now := time.Now()
	c.rateLimiter.now = func() time.Time { return now }

	data := newRootHandlerTestStream(requestRoots, peer.ID("history-peer")).run(t, c)
	require.Equal(t, wantIndices, readEnvelopeBuilderIndices(t, data, cfg))
	require.Len(t, rebuilder.called, rebuildCount)
}

func TestExecutionPayloadEnvelopesByRootContinuesAfterRebuildError(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "EL error", err: errors.New("EL unavailable")},
		{name: "busy", err: envelope_history.ErrRebuildBusy},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()
			_, indiciesDB := setupStore(t)
			store := tests.NewMockBlockReader()
			ethClock, cfg := getGloasEthClockAndConfig(t)
			tx, err := indiciesDB.BeginRw(ctx)
			require.NoError(t, err)
			t.Cleanup(tx.Rollback)
			blocks := populateDatabaseWithBlocks(t, store, tx, ethClock.GetCurrentSlot()-3, 2)
			require.NoError(t, tx.Commit())

			roots := solid.NewHashList(int(cfg.MaxRequestPayloads))
			for _, block := range blocks {
				root, err := block.Block.HashSSZ()
				require.NoError(t, err)
				roots.Append(root)
			}
			storedEnvelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
			storedEnvelope.Message.BuilderIndex = 99
			fc := mock_services.NewForkChoiceStorageMock(t)
			fc.FinalizedCheckpointVal = solid.Checkpoint{Epoch: ethClock.GetCurrentEpoch()}
			fc.SetEnvelope(roots.Get(1), storedEnvelope)
			rebuilder := &testEnvelopeRebuilder{err: test.err}
			c := NewConsensusHandlers(ctx, store, indiciesDB, nil, &clparams.NetworkConfig{}, nil, cfg, ethClock, nil, fc, nil, nil, nil, true, rebuilder)

			data := newRootHandlerTestStream(roots, peer.ID("history-peer")).run(t, c)
			require.Equal(t, []uint64{99}, readEnvelopeBuilderIndices(t, data, cfg))
			require.Len(t, rebuilder.called, 1)
			require.Equal(t, []common.Hash{roots.Get(0)}, rebuilder.called)
		})
	}
}

func TestExecutionPayloadEnvelopesByRootSkipsOutsideServeWindow(t *testing.T) {
	ctx := t.Context()
	_, indiciesDB := setupStore(t)
	store := tests.NewMockBlockReader()
	ethClock, cfg := getGloasEthClockAndConfig(t)
	currentEpoch := ethClock.GetCurrentEpoch()
	require.Greater(t, currentEpoch, cfg.MinEpochsForBlockRequests()+1)
	slot := (currentEpoch - cfg.MinEpochsForBlockRequests() - 1) * cfg.SlotsPerEpoch
	tx, err := indiciesDB.BeginRw(ctx)
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	blocks := populateDatabaseWithBlocks(t, store, tx, slot, 0)
	require.NoError(t, tx.Commit())
	root, err := blocks[0].Block.HashSSZ()
	require.NoError(t, err)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
	rebuilder := &testEnvelopeRebuilder{envelopes: map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope{root: envelope}}
	fc := mock_services.NewForkChoiceStorageMock(t)
	fc.FinalizedCheckpointVal = solid.Checkpoint{Epoch: currentEpoch}
	c := NewConsensusHandlers(ctx, store, indiciesDB, nil, &clparams.NetworkConfig{}, nil, cfg, ethClock, nil, fc, nil, nil, nil, true, rebuilder)
	roots := solid.NewHashList(int(cfg.MaxRequestPayloads))
	roots.Append(root)

	require.Empty(t, newRootHandlerTestStream(roots, peer.ID("history-peer")).run(t, c))
	require.Empty(t, rebuilder.called)
}

func TestExecutionPayloadEnvelopesByRangeRebuildsHistoryBelowForkGraph(t *testing.T) {
	ctx := t.Context()
	_, indiciesDB := setupStore(t)
	store := tests.NewMockBlockReader()
	ethClock, beaconCfg := getGloasEthClockAndConfig(t)
	startSlot := ethClock.GetCurrentSlot() - 4
	tx, err := indiciesDB.BeginRw(ctx)
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	roots, envelopes := populateGloasEnvelopeHistory(t, store, tx, beaconCfg, startSlot, 3, -1)
	require.NoError(t, tx.Commit())

	rebuilder := &testEnvelopeRebuilder{envelopes: envelopes}
	fcMock := mock_services.NewForkChoiceStorageMock(t)
	fcMock.FinalizedCheckpointVal = solid.Checkpoint{Epoch: ethClock.GetCurrentEpoch()}
	fcMock.HeadVal = roots[2]
	fcMock.HeadSlotVal = startSlot + 2
	fcMock.LowestAvailableSlotVal = new(uint64)
	*fcMock.LowestAvailableSlotVal = startSlot + 2
	c := NewConsensusHandlers(ctx, store, indiciesDB, nil, &clparams.NetworkConfig{}, nil, beaconCfg, ethClock, nil, fcMock, nil, nil, nil, true, rebuilder)

	var request bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&request, &cltypes.ExecutionPayloadEnvelopesByRangeRequest{StartSlot: startSlot, Count: 2}))
	stream := &envelopeHandlerStream{
		request:  bytes.NewReader(request.Bytes()),
		conn:     &remotePeerConn{peerID: peer.ID("history-peer")},
		protocol: protocol.ID(communication.ExecutionPayloadEnvelopesByRangeProtocolV1),
	}
	require.NoError(t, c.executionPayloadEnvelopesByRangeHandler(stream))
	require.Equal(t, []uint64{0, 1}, readEnvelopeBuilderIndices(t, stream.response.Bytes(), beaconCfg))
	require.Len(t, rebuilder.called, 2)
}

func TestExecutionPayloadEnvelopesByRangeStartsAtFirstKnownCanonicalBlock(t *testing.T) {
	c, cfg, _, _, _, stream := newRangeHistoryHandler(t, 3, 2)
	firstIndexedSlot := stream.startSlot
	firstBlock := c.beaconDB.(*tests.MockBlockReader).U[firstIndexedSlot]
	require.NoError(t, c.indiciesDB.(kv.RwDB).Update(t.Context(), func(tx kv.RwTx) error {
		if err := tx.Delete(kv.CanonicalBlockRoots, base_encoding.Encode64ToBytes4(firstIndexedSlot-1)); err != nil {
			return err
		}
		return tx.Delete(kv.BlockRootToSlot, firstBlock.Block.ParentRoot[:])
	}))
	lowestAvailableSlot := firstIndexedSlot + 2
	c.forkChoiceReader.(*mock_services.ForkChoiceStorageMock).LowestAvailableSlotVal = &lowestAvailableSlot

	data := stream.run(t, c)
	require.Equal(t, []uint64{0, 1}, readEnvelopeBuilderIndices(t, data, cfg))
}

func TestExecutionPayloadEnvelopesByRangeRejectsUnknownLeadingHistory(t *testing.T) {
	for _, test := range []struct {
		name             string
		startOffset      uint64
		count            uint64
		removeParentSlot bool
	}{
		{name: "first canonical block is after start", startOffset: 1, count: 2},
		{name: "parent is not indexed", startOffset: 1, count: 2, removeParentSlot: true},
		{name: "next canonical block is after range", startOffset: 2, count: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, _, _, _, _, stream := newRangeHistoryHandler(t, 3, int(test.count))
			firstIndexedSlot := stream.startSlot
			firstBlock := c.beaconDB.(*tests.MockBlockReader).U[firstIndexedSlot]
			require.NoError(t, c.indiciesDB.(kv.RwDB).Update(t.Context(), func(tx kv.RwTx) error {
				if err := tx.Delete(kv.CanonicalBlockRoots, base_encoding.Encode64ToBytes4(firstIndexedSlot-1)); err != nil {
					return err
				}
				if test.removeParentSlot {
					return tx.Delete(kv.BlockRootToSlot, firstBlock.Block.ParentRoot[:])
				}
				return nil
			}))
			lowestAvailableSlot := firstIndexedSlot + 2
			c.forkChoiceReader.(*mock_services.ForkChoiceStorageMock).LowestAvailableSlotVal = &lowestAvailableSlot
			stream.startSlot -= test.startOffset

			data := stream.run(t, c)
			require.NotEmpty(t, data)
			require.Equal(t, byte(ResourceUnavailablePrefix), data[0])
		})
	}
}

func TestExecutionPayloadEnvelopesByRangeCrossesForkGraphBoundary(t *testing.T) {
	c, cfg, roots, envelopes, rebuilder, stream := newRangeHistoryHandler(t, 3, 3)
	c.forkChoiceReader.(*mock_services.ForkChoiceStorageMock).Blocks[roots[1]] = c.beaconDB.(*tests.MockBlockReader).U[stream.startSlot+1]
	c.forkChoiceReader.(*mock_services.ForkChoiceStorageMock).Blocks[roots[2]] = c.beaconDB.(*tests.MockBlockReader).U[stream.startSlot+2]
	c.forkChoiceReader.(*mock_services.ForkChoiceStorageMock).SetEnvelope(roots[1], envelopes[roots[1]])
	c.forkChoiceReader.(*mock_services.ForkChoiceStorageMock).SetEnvelope(roots[2], envelopes[roots[2]])
	c.forkChoiceReader.(*mock_services.ForkChoiceStorageMock).HeadPayloadStatusVal = cltypes.PayloadStatusFull
	delete(rebuilder.envelopes, roots[1])
	delete(rebuilder.envelopes, roots[2])

	data := stream.run(t, c)
	require.Equal(t, []uint64{0, 1, 2}, readEnvelopeBuilderIndices(t, data, cfg))
	require.Equal(t, []common.Hash{roots[0]}, rebuilder.called)
}

func TestExecutionPayloadEnvelopesByRangeStopsAtUnavailableEnvelope(t *testing.T) {
	for _, test := range []struct {
		name        string
		firstStored bool
		busy        bool
		wantIndices []uint64
		wantPrefix  byte
	}{
		{name: "first unavailable", wantPrefix: ResourceUnavailablePrefix},
		{name: "unavailable after write", firstStored: true, wantIndices: []uint64{0}},
		{name: "busy first", busy: true, wantPrefix: ResourceUnavailablePrefix},
		{name: "busy after write", firstStored: true, busy: true, wantIndices: []uint64{0}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, cfg, roots, envelopes, rebuilder, stream := newRangeHistoryHandler(t, 4, 3)
			delete(rebuilder.envelopes, roots[0])
			delete(rebuilder.envelopes, roots[1])
			if test.firstStored {
				if test.busy {
					c.forkChoiceReader.(*mock_services.ForkChoiceStorageMock).SetEnvelope(roots[0], envelopes[roots[0]])
				} else {
					rebuilder.envelopes[roots[0]] = envelopes[roots[0]]
				}
			}
			if test.busy {
				rebuilder.err = envelope_history.ErrRebuildBusy
			}
			data := stream.run(t, c)
			if test.wantPrefix != 0 {
				require.Equal(t, test.wantPrefix, data[0])
				return
			}
			require.Equal(t, test.wantIndices, readEnvelopeBuilderIndices(t, data, cfg))
		})
	}
}

func TestExecutionPayloadEnvelopesByRangeRebuildBudgetIsPerPeer(t *testing.T) {
	c, cfg, _, _, rebuilder, stream := newRangeHistoryHandler(t, 3, 1)
	peerA := peer.ID("peer-a")
	bucket := c.rateLimiter.tryStartEnvelopeRebuild(peerA.String())
	require.NotNil(t, bucket)
	c.rateLimiter.finishEnvelopeRebuild(bucket, 2*time.Second, true)
	stream.conn = &remotePeerConn{peerID: peerA}
	data := stream.run(t, c)
	require.Equal(t, byte(ResourceUnavailablePrefix), data[0])
	require.Empty(t, rebuilder.called)

	peerBStream := &rangeHandlerTestStream{
		envelopeHandlerStream: &envelopeHandlerStream{conn: &remotePeerConn{peerID: peer.ID("peer-b")}, protocol: protocol.ID(communication.ExecutionPayloadEnvelopesByRangeProtocolV1)},
		startSlot:             stream.startSlot,
		count:                 stream.count,
	}
	require.Equal(t, []uint64{0}, readEnvelopeBuilderIndices(t, peerBStream.run(t, c), cfg))
	require.Len(t, rebuilder.called, 1)
}

func TestExecutionPayloadEnvelopesByRangeChargesRebuildSlotTime(t *testing.T) {
	for _, test := range []struct {
		name       string
		err        error
		wantCredit bool
	}{
		{name: "rebuild", wantCredit: false},
		{name: "busy", err: envelope_history.ErrRebuildBusy, wantCredit: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, _, _, _, rebuilder, stream := newRangeHistoryHandler(t, 3, 1)
			now := time.Now()
			c.rateLimiter.now = func() time.Time { return now }
			rebuilder.err = test.err
			rebuilder.onRebuild = func() { now = now.Add(2 * time.Second) }

			stream.run(t, c)
			require.Len(t, rebuilder.called, 1)
			peerID := peer.ID("history-peer").String()
			bucket := c.rateLimiter.tryStartEnvelopeRebuild(peerID)
			require.Equal(t, test.wantCredit, bucket != nil)
			if bucket != nil {
				c.rateLimiter.finishEnvelopeRebuild(bucket, 0, false)
			}
		})
	}
}

func TestExecutionPayloadEnvelopesByRangeBlocksConcurrentRebuildForSamePeer(t *testing.T) {
	c, cfg, roots, _, rebuilder, stream := newRangeHistoryHandler(t, 3, 1)
	now := time.Now()
	c.rateLimiter.now = func() time.Time { return now }
	rebuilder.onRebuild = func() {
		rebuilder.onRebuild = nil
		now = now.Add(2 * time.Second)
		nested := &rangeHandlerTestStream{
			envelopeHandlerStream: &envelopeHandlerStream{conn: stream.conn, protocol: stream.protocol},
			startSlot:             stream.startSlot + 1,
			count:                 1,
		}
		data := nested.run(t, c)
		require.Equal(t, byte(ResourceUnavailablePrefix), data[0])
		require.Equal(t, []common.Hash{roots[0]}, rebuilder.called)
	}

	require.Equal(t, []uint64{0}, readEnvelopeBuilderIndices(t, stream.run(t, c), cfg))
	require.Equal(t, []common.Hash{roots[0]}, rebuilder.called)
	require.Nil(t, c.rateLimiter.tryStartEnvelopeRebuild(peer.ID("history-peer").String()))
}

func TestTryRebuildEnvelopeChargesPanic(t *testing.T) {
	now := time.Now()
	rebuilder := &testEnvelopeRebuilder{onRebuild: func() {
		now = now.Add(2 * time.Second)
		panic("rebuild failed")
	}}
	c := &ConsensusHandlers{rateLimiter: newPeerRateLimiter(), envelopeRebuilder: rebuilder}
	c.rateLimiter.now = func() time.Time { return now }
	peerID := peer.ID("history-peer").String()

	require.Panics(t, func() {
		_, _ = c.tryRebuildEnvelope(t.Context(), peerID, new(envelope_history.PreparedRebuild))
	})
	require.Nil(t, c.rateLimiter.tryStartEnvelopeRebuild(peerID))
	now = now.Add(2 * time.Second)
	bucket := c.rateLimiter.tryStartEnvelopeRebuild(peerID)
	require.NotNil(t, bucket)
	c.rateLimiter.finishEnvelopeRebuild(bucket, 0, false)
}

func TestExecutionPayloadEnvelopesByRangeDoesNotRebuildFullHeadWithoutChild(t *testing.T) {
	c, cfg, roots, envelopes, rebuilder, stream := newRangeHistoryHandler(t, 3, 3)
	fc := c.forkChoiceReader.(*mock_services.ForkChoiceStorageMock)
	for i := range 2 {
		fc.SetEnvelope(roots[i], envelopes[roots[i]])
		delete(rebuilder.envelopes, roots[i])
	}
	fc.HeadPayloadStatusVal = cltypes.PayloadStatusFull

	data := stream.run(t, c)
	require.Equal(t, []uint64{0, 1}, readEnvelopeBuilderIndices(t, data, cfg))
	require.Empty(t, rebuilder.called)
}

func TestExecutionPayloadEnvelopesByRangeSkipsEmptyHistoricalPayload(t *testing.T) {
	ctx := t.Context()
	_, indiciesDB := setupStore(t)
	store := tests.NewMockBlockReader()
	ethClock, cfg := getGloasEthClockAndConfig(t)
	startSlot := ethClock.GetCurrentSlot() - 5
	tx, err := indiciesDB.BeginRw(ctx)
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	roots, envelopes := populateGloasEnvelopeHistory(t, store, tx, cfg, startSlot, 4, 1)
	require.NoError(t, tx.Commit())
	rebuilder := &testEnvelopeRebuilder{envelopes: envelopes}
	fc := mock_services.NewForkChoiceStorageMock(t)
	fc.FinalizedCheckpointVal = solid.Checkpoint{Epoch: ethClock.GetCurrentEpoch()}
	fc.HeadVal = roots[3]
	fc.HeadSlotVal = startSlot + 3
	c := NewConsensusHandlers(ctx, store, indiciesDB, nil, &clparams.NetworkConfig{}, nil, cfg, ethClock, nil, fc, nil, nil, nil, true, rebuilder)
	stream := &rangeHandlerTestStream{
		envelopeHandlerStream: &envelopeHandlerStream{conn: &remotePeerConn{peerID: peer.ID("history-peer")}, protocol: protocol.ID(communication.ExecutionPayloadEnvelopesByRangeProtocolV1)},
		startSlot:             startSlot,
		count:                 3,
	}

	require.Equal(t, []uint64{0, 2}, readEnvelopeBuilderIndices(t, stream.run(t, c), cfg))
	require.Equal(t, []common.Hash{roots[0], roots[2]}, rebuilder.called)
}

type rangeHandlerTestStream struct {
	*envelopeHandlerStream
	startSlot uint64
	count     uint64
}

type rootHandlerTestStream struct {
	*envelopeHandlerStream
	roots solid.HashListSSZ
}

func newRootHandlerTestStream(roots solid.HashListSSZ, peerID peer.ID) *rootHandlerTestStream {
	return &rootHandlerTestStream{
		envelopeHandlerStream: &envelopeHandlerStream{conn: &remotePeerConn{peerID: peerID}, protocol: protocol.ID(communication.ExecutionPayloadEnvelopesByRootProtocolV1)},
		roots:                 roots,
	}
}

func (s *rootHandlerTestStream) run(t *testing.T, c *ConsensusHandlers) []byte {
	t.Helper()
	var request bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&request, s.roots))
	s.request = bytes.NewReader(request.Bytes())
	require.NoError(t, c.executionPayloadEnvelopesByRootHandler(s))
	return s.response.Bytes()
}

func (s *rangeHandlerTestStream) run(t *testing.T, c *ConsensusHandlers) []byte {
	t.Helper()
	var request bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&request, &cltypes.ExecutionPayloadEnvelopesByRangeRequest{StartSlot: s.startSlot, Count: s.count}))
	s.request = bytes.NewReader(request.Bytes())
	require.NoError(t, c.executionPayloadEnvelopesByRangeHandler(s))
	return s.response.Bytes()
}

func newRangeHistoryHandler(t *testing.T, blockCount, requestCount int) (*ConsensusHandlers, *clparams.BeaconChainConfig, []common.Hash, map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope, *testEnvelopeRebuilder, *rangeHandlerTestStream) {
	t.Helper()
	ctx := t.Context()
	_, indiciesDB := setupStore(t)
	store := tests.NewMockBlockReader()
	ethClock, beaconCfg := getGloasEthClockAndConfig(t)
	startSlot := ethClock.GetCurrentSlot() - uint64(blockCount) - 1
	tx, err := indiciesDB.BeginRw(ctx)
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	roots, envelopes := populateGloasEnvelopeHistory(t, store, tx, beaconCfg, startSlot, blockCount, -1)
	require.NoError(t, tx.Commit())
	rebuilder := &testEnvelopeRebuilder{envelopes: maps.Clone(envelopes)}
	fc := mock_services.NewForkChoiceStorageMock(t)
	fc.FinalizedCheckpointVal = solid.Checkpoint{Epoch: ethClock.GetCurrentEpoch()}
	fc.HeadVal = roots[len(roots)-1]
	fc.HeadSlotVal = startSlot + uint64(len(roots)-1)
	c := NewConsensusHandlers(ctx, store, indiciesDB, nil, &clparams.NetworkConfig{}, nil, beaconCfg, ethClock, nil, fc, nil, nil, nil, true, rebuilder)
	stream := &rangeHandlerTestStream{
		envelopeHandlerStream: &envelopeHandlerStream{
			conn:     &remotePeerConn{peerID: peer.ID("history-peer")},
			protocol: protocol.ID(communication.ExecutionPayloadEnvelopesByRangeProtocolV1),
		},
		startSlot: startSlot,
		count:     uint64(requestCount),
	}
	return c, beaconCfg, roots, envelopes, rebuilder, stream
}

func populateGloasEnvelopeHistory(t *testing.T, store *tests.MockBlockReader, tx kv.RwTx, cfg *clparams.BeaconChainConfig, startSlot uint64, count, emptyPayloadIndex int) ([]common.Hash, map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) {
	t.Helper()
	roots := make([]common.Hash, 0, count)
	envelopes := make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope, count)
	var parentRoot common.Hash
	var parentBlockHash common.Hash
	if startSlot > 0 {
		parent := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
		parent.Block.Slot = startSlot - 1
		parent.Block.Body.SignedExecutionPayloadBid.Message.BlockHash = common.Hash{0xff}
		var err error
		parentRoot, err = parent.Block.HashSSZ()
		require.NoError(t, err)
		parentBlockHash = parent.Block.Body.SignedExecutionPayloadBid.Message.BlockHash
		store.U[parent.Block.Slot] = parent
		require.NoError(t, beacon_indicies.WriteBeaconBlockAndIndicies(tx, parent, true))
	}
	for i := range count {
		block := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
		block.Block.Slot = startSlot + uint64(i)
		block.Block.ParentRoot = parentRoot
		bid := block.Block.Body.SignedExecutionPayloadBid.Message
		bid.ParentBlockRoot = parentRoot
		bid.ParentBlockHash = parentBlockHash
		if i == emptyPayloadIndex+1 {
			bid.ParentBlockHash = common.Hash{0xff}
		}
		bid.BlockHash = common.Hash{byte(i + 1)}
		bid.BuilderIndex = uint64(i)
		bid.Slot = block.Block.Slot
		root, err := block.Block.HashSSZ()
		require.NoError(t, err)
		store.U[block.Block.Slot] = block
		require.NoError(t, beacon_indicies.WriteBeaconBlockAndIndicies(tx, block, true))
		roots = append(roots, root)
		envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
		envelope.Message.BeaconBlockRoot = root
		envelope.Message.BuilderIndex = uint64(i)
		envelopes[root] = envelope
		parentRoot = root
		parentBlockHash = bid.BlockHash
	}
	return roots, envelopes
}

func readEnvelopeBuilderIndices(t *testing.T, data []byte, cfg *clparams.BeaconChainConfig) []uint64 {
	t.Helper()
	reader := bytes.NewReader(data)
	var indices []uint64
	for reader.Len() > 0 {
		code, err := reader.ReadByte()
		require.NoError(t, err)
		require.Equal(t, byte(SuccessfulResponsePrefix), code)
		forkDigest := make([]byte, 4)
		_, err = io.ReadFull(reader, forkDigest)
		require.NoError(t, err)
		encodedLen, err := ssz_snappy.ReadUvarint(reader)
		require.NoError(t, err)
		snappyReader := snappypool.Reader(reader)
		raw := make([]byte, encodedLen)
		_, err = io.ReadFull(snappyReader, raw)
		require.NoError(t, err)
		snappypool.PutReader(snappyReader)
		envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(cfg)}
		require.NoError(t, envelope.DecodeSSZ(raw, int(clparams.GloasVersion)))
		indices = append(indices, envelope.Message.BuilderIndex)
	}
	return indices
}

func TestExecutionPayloadEnvelopesByRootHandler_PreGloas(t *testing.T) {
	ctx := context.Background()

	host, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { host.Close() })

	host1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { host1.Close() })

	err = host.Connect(ctx, peer.AddrInfo{
		ID:    host1.ID(),
		Addrs: host1.Addrs(),
	})
	require.NoError(t, err)

	_, indiciesDB := setupStore(t)
	store := tests.NewMockBlockReader()

	ethClock := getEthClock(t)
	_, beaconCfg := clparams.GetConfigsByNetwork(1)

	fcMock := mock_services.NewForkChoiceStorageMock(t)

	c := NewConsensusHandlers(
		ctx,
		store,
		indiciesDB,
		host,
		&clparams.NetworkConfig{},
		nil,
		beaconCfg,
		ethClock,
		nil, fcMock, nil, nil, nil, true, nil,
	)
	c.Start()

	reqRoots := solid.NewHashList(int(beaconCfg.MaxRequestPayloads))
	reqRoots.Append(common.Hash{1, 2, 3})
	var reqBuf bytes.Buffer
	err = ssz_snappy.EncodeAndWrite(&reqBuf, reqRoots)
	require.NoError(t, err)

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.ExecutionPayloadEnvelopesByRootProtocolV1))
	require.NoError(t, err)

	_, err = stream.Write(reqBuf.Bytes())
	require.NoError(t, err)

	_, err = stream.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF, "should get EOF for pre-GLOAS request")
}

func TestExecutionPayloadEnvelopesByRootHandlerRejectsOverLimit(t *testing.T) {
	ctx := context.Background()

	host, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { host.Close() })

	host1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { host1.Close() })

	err = host.Connect(ctx, peer.AddrInfo{
		ID:    host1.ID(),
		Addrs: host1.Addrs(),
	})
	require.NoError(t, err)

	_, indiciesDB := setupStore(t)
	store := tests.NewMockBlockReader()
	ethClock, beaconCfg := getGloasEthClockAndConfig(t)
	beaconCfg.MaxRequestPayloads = 1
	fcMock := mock_services.NewForkChoiceStorageMock(t)

	c := NewConsensusHandlers(
		ctx,
		store,
		indiciesDB,
		host,
		&clparams.NetworkConfig{},
		nil,
		beaconCfg,
		ethClock,
		nil, fcMock, nil, nil, nil, true, nil,
	)
	c.Start()

	reqRoots := solid.NewHashList(int(beaconCfg.MaxRequestPayloads))
	reqRoots.Append(common.Hash{1})
	reqRoots.Append(common.Hash{2})
	var reqBuf bytes.Buffer
	err = ssz_snappy.EncodeAndWrite(&reqBuf, reqRoots)
	require.NoError(t, err)

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.ExecutionPayloadEnvelopesByRootProtocolV1))
	require.NoError(t, err)

	_, err = stream.Write(reqBuf.Bytes())
	require.NoError(t, err)

	_, err = stream.Read(make([]byte, 1))
	require.Error(t, err)
}

func TestExecutionPayloadEnvelopesByRangeHandler_PreGloas(t *testing.T) {
	ctx := context.Background()

	host, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { host.Close() })

	host1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { host1.Close() })

	err = host.Connect(ctx, peer.AddrInfo{
		ID:    host1.ID(),
		Addrs: host1.Addrs(),
	})
	require.NoError(t, err)

	_, indiciesDB := setupStore(t)
	store := tests.NewMockBlockReader()

	// Use mainnet config where GloasForkEpoch = MaxUint64 (pre-GLOAS)
	ethClock := getEthClock(t)
	_, beaconCfg := clparams.GetConfigsByNetwork(1)

	fcMock := mock_services.NewForkChoiceStorageMock(t)

	c := NewConsensusHandlers(
		ctx,
		store,
		indiciesDB,
		host,
		&clparams.NetworkConfig{},
		nil,
		beaconCfg,
		ethClock,
		nil, fcMock, nil, nil, nil, true, nil,
	)
	c.Start()

	req := &cltypes.ExecutionPayloadEnvelopesByRangeRequest{
		StartSlot: 100,
		Count:     5,
	}
	var reqBuf bytes.Buffer
	err = ssz_snappy.EncodeAndWrite(&reqBuf, req)
	require.NoError(t, err)

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.ExecutionPayloadEnvelopesByRangeProtocolV1))
	require.NoError(t, err)

	_, err = stream.Write(reqBuf.Bytes())
	require.NoError(t, err)

	// Handler should return empty response (no data before GLOAS fork)
	_, err = stream.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF, "should get EOF for pre-GLOAS request")
}
