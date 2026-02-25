package handlers

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"testing"

	"github.com/golang/snappy"
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
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

// getGloasEthClockAndConfig returns an EthereumClock and BeaconChainConfig
// with all fork epochs set to 0, so that GLOAS is active from the start.
func getGloasEthClockAndConfig(t *testing.T) (eth_clock.EthereumClock, *clparams.BeaconChainConfig) {
	s, err := initial_state.GetGenesisState(chainspec.MainnetChainID)
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

func TestExecutionPayloadEnvelopesByRangeHandler(t *testing.T) {
	ctx := context.Background()

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

	startSlot := uint64(100)
	count := uint64(5)

	// Populate database with blocks (needed for canonical root lookup)
	expBlocks := populateDatabaseWithBlocks(t, store, tx, startSlot, count)
	require.NoError(t, tx.Commit())

	ethClock, beaconCfg := getGloasEthClockAndConfig(t)

	// Create mock fork choice with envelopes
	fcMock := mock_services.NewForkChoiceStorageMock(t)

	// Create envelopes for each block and store them in mock.
	// The canonical block root is HashSSZ(header), computed by WriteBeaconBlockHeaderAndIndicies.
	expEnvelopes := make([]*cltypes.SignedExecutionPayloadEnvelope, 0, count)
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

		// Create a properly versioned Eth1Block for GLOAS
		payload := cltypes.NewEth1Block(clparams.GloasVersion, beaconCfg)
		payload.Transactions = &solid.TransactionsSSZ{}
		payload.Extra = solid.NewExtraData()
		payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(beaconCfg.MaxWithdrawalsPerPayload), 44)
		envelope := &cltypes.SignedExecutionPayloadEnvelope{
			Message: &cltypes.ExecutionPayloadEnvelope{
				Payload:           payload,
				ExecutionRequests: cltypes.NewExecutionRequests(beaconCfg),
			},
		}
		envelope.Message.Slot = block.Block.Slot
		envelope.Message.BeaconBlockRoot = blockRoot
		envelope.Message.BuilderIndex = uint64(i)
		envelope.Message.StateRoot = common.Hash{byte(i + 1)}

		fcMock.Envelopes[blockRoot] = envelope
		expEnvelopes = append(expEnvelopes, envelope)
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
	var reqBuf bytes.Buffer
	err = ssz_snappy.EncodeAndWrite(&reqBuf, req)
	require.NoError(t, err)

	// Open stream to the handler
	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.ExecutionPayloadEnvelopesByRangeProtocolV1))
	require.NoError(t, err)

	_, err = stream.Write(reqBuf.Bytes())
	require.NoError(t, err)

	// Read response chunks
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
		sr := snappy.NewReader(stream)
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
		require.Equal(t, expEnvelopes[i].Message.Slot, envelope.Message.Slot)
		require.Equal(t, expEnvelopes[i].Message.BuilderIndex, envelope.Message.BuilderIndex)
		require.Equal(t, expEnvelopes[i].Message.StateRoot, envelope.Message.StateRoot)
	}

	// Verify stream is exhausted
	_, err = stream.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF, "stream should be empty after all envelopes")
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

	startSlot := uint64(100)
	count := uint64(5)

	expBlocks := populateDatabaseWithBlocks(t, store, tx, startSlot, count)
	require.NoError(t, tx.Commit())

	ethClock, beaconCfg := getGloasEthClockAndConfig(t)

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
				ExecutionRequests: cltypes.NewExecutionRequests(beaconCfg),
			},
		}
		envelope.Message.Slot = block.Block.Slot
		envelope.Message.BeaconBlockRoot = blockRoot
		envelope.Message.BuilderIndex = uint64(i)
		envelope.Message.StateRoot = common.Hash{byte(i + 1)}

		fcMock.Envelopes[blockRoot] = envelope
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
	reqRoots := solid.NewHashList(int(beaconCfg.MaxRequestBlocksDeneb))
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
		sr := snappy.NewReader(stream)
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

		require.Equal(t, expEnvelopes[i].Message.Slot, envelope.Message.Slot)
		require.Equal(t, expEnvelopes[i].Message.BuilderIndex, envelope.Message.BuilderIndex)
		require.Equal(t, expEnvelopes[i].Message.StateRoot, envelope.Message.StateRoot)
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

	reqRoots := solid.NewHashList(int(beaconCfg.MaxRequestBlocksDeneb))
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
