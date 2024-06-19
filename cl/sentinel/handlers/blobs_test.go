package handlers

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"testing"

	"github.com/golang/snappy"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/cl/antiquary/tests"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/clparams/initial_state"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/persistence/blob_storage"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/ledgerwatch/erigon/cl/sentinel/communication"
	"github.com/ledgerwatch/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/ledgerwatch/erigon/cl/sentinel/peers"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cl/utils/eth_clock"
)

func getEthClock(t *testing.T) eth_clock.EthereumClock {
	s, err := initial_state.GetGenesisState(clparams.MainnetNetwork)
	require.NoError(t, err)
	return eth_clock.NewEthereumClock(s.GenesisTime(), s.GenesisValidatorsRoot(), s.BeaconConfig())
}

func getTestBlobSidecars(blockHeader *cltypes.SignedBeaconBlockHeader) []*cltypes.BlobSidecar {
	out := []*cltypes.BlobSidecar{}
	for i := 0; i < 4; i++ {
		out = append(out, cltypes.NewBlobSidecar(
			uint64(i),
			&cltypes.Blob{byte(i)},
			libcommon.Bytes48{byte(i)},
			libcommon.Bytes48{byte(i)},
			blockHeader,
			solid.NewHashVector(cltypes.CommitmentBranchSize),
		))
	}
	return out

}

func TestBlobsByRangeHandler(t *testing.T) {
	ctx := context.Background()

	listenAddrHost := "/ip4/127.0.0.1/tcp/6121"
	host, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost))
	require.NoError(t, err)

	listenAddrHost1 := "/ip4/127.0.0.1/tcp/6358"
	host1, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost1))
	require.NoError(t, err)

	err = host.Connect(ctx, peer.AddrInfo{
		ID:    host1.ID(),
		Addrs: host1.Addrs(),
	})
	require.NoError(t, err)

	peersPool := peers.NewPool()
	blobDb := memdb.NewTestDB(t)
	_, indiciesDB := setupStore(t)
	store := tests.NewMockBlockReader()

	tx, _ := indiciesDB.BeginRw(ctx)

	startSlot := uint64(100)
	count := uint64(10)

	expBlocks := populateDatabaseWithBlocks(t, store, tx, startSlot, count)
	h := expBlocks[0].SignedBeaconBlockHeader()
	sidecars := getTestBlobSidecars(h)
	_, beaconCfg := clparams.GetConfigsByNetwork(1)
	blobStorage := blob_storage.NewBlobStore(blobDb, afero.NewMemMapFs(), math.MaxUint64, beaconCfg, nil)
	r, _ := h.Header.HashSSZ()
	require.NoError(t, blobStorage.WriteBlobSidecars(ctx, r, sidecars))

	tx.Commit()

	ethClock := getEthClock(t)
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
		nil, &mock_services.ForkChoiceStorageMock{}, blobStorage, true,
	)
	c.Start()
	req := &cltypes.BlobsByRangeRequest{
		StartSlot: h.Header.Slot,
		Count:     1,
	}

	var reqBuf bytes.Buffer
	if err := ssz_snappy.EncodeAndWrite(&reqBuf, req); err != nil {
		return
	}

	reqData := libcommon.CopyBytes(reqBuf.Bytes())
	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.BlobSidecarByRangeProtocolV1))
	require.NoError(t, err)

	_, err = stream.Write(reqData)
	require.NoError(t, err)

	firstByte := make([]byte, 1)
	_, err = stream.Read(firstByte)
	require.NoError(t, err)
	require.Equal(t, firstByte[0], byte(0))

	for i := 0; i < len(sidecars); i++ {
		forkDigest := make([]byte, 4)
		_, err := stream.Read(forkDigest)
		if err != nil && err != io.EOF {
			require.NoError(t, err)
		}

		encodedLn, _, err := ssz_snappy.ReadUvarint(stream)
		require.NoError(t, err)

		raw := make([]byte, encodedLn)
		sr := snappy.NewReader(stream)
		bytesRead := 0
		for bytesRead < int(encodedLn) {
			n, err := sr.Read(raw[bytesRead:])
			if err != nil {
				require.NoError(t, err)
			}
			bytesRead += n
		}
		// Fork digests
		respForkDigest := binary.BigEndian.Uint32(forkDigest)
		if respForkDigest == 0 {
			require.NoError(t, fmt.Errorf("null fork digest"))
		}
		version, err := ethClock.StateVersionByForkDigest(utils.Uint32ToBytes4(respForkDigest))
		if err != nil {
			require.NoError(t, err)
		}

		sidecar := &cltypes.BlobSidecar{}
		if err = sidecar.DecodeSSZ(raw, int(version)); err != nil {
			require.NoError(t, err)
			return
		}
		require.Equal(t, sidecars[i], sidecar)
		stream.Read(make([]byte, 1))
	}

	_, err = stream.Read(make([]byte, 1))
	if err != io.EOF {
		t.Fatal("Stream is not empty")
	}

	defer indiciesDB.Close()
	defer tx.Rollback()
}

func TestBlobsByIdentifiersHandler(t *testing.T) {
	ctx := context.Background()

	listenAddrHost := "/ip4/127.0.0.1/tcp/6125"
	host, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost))
	require.NoError(t, err)

	listenAddrHost1 := "/ip4/127.0.0.1/tcp/6350"
	host1, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost1))
	require.NoError(t, err)

	err = host.Connect(ctx, peer.AddrInfo{
		ID:    host1.ID(),
		Addrs: host1.Addrs(),
	})
	require.NoError(t, err)

	peersPool := peers.NewPool()
	blobDb := memdb.NewTestDB(t)
	_, indiciesDB := setupStore(t)
	store := tests.NewMockBlockReader()

	tx, _ := indiciesDB.BeginRw(ctx)

	startSlot := uint64(100)
	count := uint64(10)

	ethClock := getEthClock(t)
	expBlocks := populateDatabaseWithBlocks(t, store, tx, startSlot, count)
	h := expBlocks[0].SignedBeaconBlockHeader()
	sidecars := getTestBlobSidecars(h)
	_, beaconCfg := clparams.GetConfigsByNetwork(1)
	blobStorage := blob_storage.NewBlobStore(blobDb, afero.NewMemMapFs(), math.MaxUint64, beaconCfg, ethClock)
	r, _ := h.Header.HashSSZ()
	require.NoError(t, blobStorage.WriteBlobSidecars(ctx, r, sidecars))

	tx.Commit()

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
		nil, &mock_services.ForkChoiceStorageMock{}, blobStorage, true,
	)
	c.Start()
	req := solid.NewStaticListSSZ[*cltypes.BlobIdentifier](40269, 40)
	req.Append(&cltypes.BlobIdentifier{BlockRoot: r, Index: 0})
	req.Append(&cltypes.BlobIdentifier{BlockRoot: r, Index: 1})
	req.Append(&cltypes.BlobIdentifier{BlockRoot: r, Index: 2})
	req.Append(&cltypes.BlobIdentifier{BlockRoot: r, Index: 3})

	var reqBuf bytes.Buffer
	if err := ssz_snappy.EncodeAndWrite(&reqBuf, req); err != nil {
		return
	}

	reqData := libcommon.CopyBytes(reqBuf.Bytes())
	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.BlobSidecarByRootProtocolV1))
	require.NoError(t, err)

	_, err = stream.Write(reqData)
	require.NoError(t, err)

	firstByte := make([]byte, 1)
	_, err = stream.Read(firstByte)
	require.NoError(t, err)
	require.Equal(t, firstByte[0], byte(0))

	for i := 0; i < len(sidecars); i++ {
		forkDigest := make([]byte, 4)
		_, err := stream.Read(forkDigest)
		if err != nil && err != io.EOF {
			require.NoError(t, err)
		}

		encodedLn, _, err := ssz_snappy.ReadUvarint(stream)
		require.NoError(t, err)

		raw := make([]byte, encodedLn)
		sr := snappy.NewReader(stream)
		bytesRead := 0
		for bytesRead < int(encodedLn) {
			n, err := sr.Read(raw[bytesRead:])
			if err != nil {
				require.NoError(t, err)
			}
			bytesRead += n
		}
		// Fork digests
		respForkDigest := binary.BigEndian.Uint32(forkDigest)
		if respForkDigest == 0 {
			require.NoError(t, fmt.Errorf("null fork digest"))
		}
		version, err := ethClock.StateVersionByForkDigest(utils.Uint32ToBytes4(respForkDigest))
		if err != nil {
			require.NoError(t, err)
		}

		sidecar := &cltypes.BlobSidecar{}
		if err = sidecar.DecodeSSZ(raw, int(version)); err != nil {
			require.NoError(t, err)
			return
		}
		require.Equal(t, sidecars[i], sidecar)
		stream.Read(make([]byte, 1))
	}

	_, err = stream.Read(make([]byte, 1))
	if err != io.EOF {
		t.Fatal("Stream is not empty")
	}

	defer indiciesDB.Close()
	defer tx.Rollback()
}
