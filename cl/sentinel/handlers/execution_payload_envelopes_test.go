package handlers

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"testing"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/clparams/initial_state"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/sentinel/communication"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/sentinel/peers"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/snappypool"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

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
	requestCount        uint64
	canonicalBlockCount uint64
	allPayloadsFull     bool
	allPayloadsEmpty    bool
	incompleteBlockAt   uint64
	startAtGenesis      bool
	verifyRateLimit     bool
	wantEnvelopeCount   int
	wantResponsePrefix  byte
}

func TestExecutionPayloadEnvelopesByRangeHandler(t *testing.T) {
	for _, tc := range []executionPayloadEnvelopesByRangeTestCase{
		{name: "empty head payload", headPayloadStatus: cltypes.PayloadStatusEmpty},
		{name: "full head payload", headPayloadStatus: cltypes.PayloadStatusFull},
		{name: "pending head payload", headPayloadStatus: cltypes.PayloadStatusPending},
		{name: "unavailable history", headPayloadStatus: cltypes.PayloadStatusFull, unavailable: true, wantResponsePrefix: ResourceUnavailablePrefix},
		{name: "overflowing range", headPayloadStatus: cltypes.PayloadStatusFull, overflow: true, wantResponsePrefix: InvalidRequestPrefix},
		{name: "missing first full envelope", headPayloadStatus: cltypes.PayloadStatusFull, missingFirstFull: true, wantResponsePrefix: ResourceUnavailablePrefix},
		{name: "missing middle full envelope", headPayloadStatus: cltypes.PayloadStatusFull, missingMiddleFull: true},
		{name: "incomplete canonical block", headPayloadStatus: cltypes.PayloadStatusFull, incompleteBlock: true, wantResponsePrefix: ResourceUnavailablePrefix},
		{name: "canonical block with nil body", headPayloadStatus: cltypes.PayloadStatusFull, incompleteBody: true, wantResponsePrefix: ResourceUnavailablePrefix},
		{name: "empty slot range ignores later incomplete block", headPayloadStatus: cltypes.PayloadStatusFull, incompleteBlock: true, emptyRange: true},
		{name: "head and canonical index mismatch", headPayloadStatus: cltypes.PayloadStatusFull, headIndexMismatch: true, wantResponsePrefix: ResourceUnavailablePrefix},
		{name: "request span may exceed response limit", headPayloadStatus: cltypes.PayloadStatusFull, requestCount: 129},
		{
			name:                "response remains capped for a larger request span",
			headPayloadStatus:   cltypes.PayloadStatusFull,
			requestCount:        129,
			canonicalBlockCount: 129,
			allPayloadsFull:     true,
			wantEnvelopeCount:   128,
		},
		{
			name:                "candidate scan remains capped for empty payloads",
			headPayloadStatus:   cltypes.PayloadStatusEmpty,
			requestCount:        130,
			canonicalBlockCount: 130,
			allPayloadsEmpty:    true,
			incompleteBlockAt:   129,
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

	peersPool := peers.NewPool(host)
	_, indiciesDB := setupStore(t)
	store := tests.NewMockBlockReader()

	tx, err := indiciesDB.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	ethClock, beaconCfg := getGloasEthClockAndConfig(t)

	// Use a startSlot near the current slot so blocks fall within the serve range
	// (the handler enforces minServeEpoch based on the current epoch).
	count := tc.canonicalBlockCount
	if count == 0 {
		count = 5
	}
	startSlot := ethClock.GetCurrentSlot() - count - 5

	// Populate database with blocks (needed for canonical root lookup)
	expBlocks := populateDatabaseWithBlocks(t, store, tx, startSlot, count-1)
	require.NoError(t, tx.Commit())

	// Create mock fork choice with envelopes
	fcMock := mock_services.NewForkChoiceStorageMock(t)

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
	if tc.wantEnvelopeCount != 0 {
		expEnvelopes = expEnvelopes[:tc.wantEnvelopeCount]
	}

	c := NewConsensusHandlers(
		ctx,
		store,
		indiciesDB,
		host,
		peersPool,
		&clparams.NetworkConfig{},
		nil,
		beaconCfg,
		ethClock,
		nil, fcMock, nil, nil, nil, true,
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
		encodedLn, _, err := ssz_snappy.ReadUvarint(stream)
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

	peersPool := peers.NewPool(host)
	_, indiciesDB := setupStore(t)
	store := tests.NewMockBlockReader()

	tx, err := indiciesDB.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	ethClock, beaconCfg := getGloasEthClockAndConfig(t)

	// Use a startSlot near the current slot so blocks fall within the serve range
	// (the handler enforces minServeEpoch based on the current epoch).
	startSlot := ethClock.GetCurrentSlot() - 10
	count := uint64(5)

	expBlocks := populateDatabaseWithBlocks(t, store, tx, startSlot, count)
	require.NoError(t, tx.Commit())

	fcMock := mock_services.NewForkChoiceStorageMock(t)

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
		peersPool,
		&clparams.NetworkConfig{},
		nil,
		beaconCfg,
		ethClock,
		nil, fcMock, nil, nil, nil, true,
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

		encodedLn, _, err := ssz_snappy.ReadUvarint(stream)
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

	peersPool := peers.NewPool(host)
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
		peersPool,
		&clparams.NetworkConfig{},
		nil,
		beaconCfg,
		ethClock,
		nil, fcMock, nil, nil, nil, true,
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

	peersPool := peers.NewPool(host)
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
		peersPool,
		&clparams.NetworkConfig{},
		nil,
		beaconCfg,
		ethClock,
		nil, fcMock, nil, nil, nil, true,
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

	peersPool := peers.NewPool(host)
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
		peersPool,
		&clparams.NetworkConfig{},
		nil,
		beaconCfg,
		ethClock,
		nil, fcMock, nil, nil, nil, true,
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
