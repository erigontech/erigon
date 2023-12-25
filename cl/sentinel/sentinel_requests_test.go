package sentinel

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"testing"

	"github.com/golang/snappy"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/cl/antiquary"
	"github.com/ledgerwatch/erigon/cl/antiquary/tests"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cl/persistence"
	state_accessors "github.com/ledgerwatch/erigon/cl/persistence/state"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/sentinel/communication"
	"github.com/ledgerwatch/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/log/v3"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func loadChain(t *testing.T) (db kv.RwDB, blocks []*cltypes.SignedBeaconBlock, f afero.Fs, preState, postState *state.CachingBeaconState) {
	blocks, preState, postState = tests.GetPhase0Random()
	db = memdb.NewTestDB(t)
	var reader *tests.MockBlockReader
	reader, f = tests.LoadChain(blocks, postState, db, t)

	ctx := context.Background()
	vt := state_accessors.NewStaticValidatorTable()
	a := antiquary.NewAntiquary(ctx, preState, vt, &clparams.MainnetBeaconConfig, datadir.New("/tmp"), nil, db, nil, reader, nil, log.New(), true, true, f)
	require.NoError(t, a.IncrementBeaconState(ctx, blocks[len(blocks)-1].Block.Slot+33))
	return
}

func TestSentinelBlocksByRange(t *testing.T) {
	listenAddrHost := "127.0.0.1"

	ctx := context.Background()
	db, blocks, f, _, _ := loadChain(t)
	raw := persistence.NewAferoRawBlockSaver(f, &clparams.MainnetBeaconConfig)
	genesisConfig, networkConfig, beaconConfig := clparams.GetConfigsByNetwork(clparams.MainnetNetwork)
	sentinel, err := New(ctx, &SentinelConfig{
		NetworkConfig: networkConfig,
		BeaconConfig:  beaconConfig,
		GenesisConfig: genesisConfig,
		IpAddr:        listenAddrHost,
		Port:          7070,
		EnableBlocks:  true,
	}, raw, db, log.New())
	require.NoError(t, err)
	defer sentinel.Stop()

	require.NoError(t, sentinel.Start())
	h := sentinel.host

	listenAddrHost1 := "/ip4/127.0.0.1/tcp/3202"
	host1, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost1))
	require.NoError(t, err)

	err = h.Connect(ctx, peer.AddrInfo{
		ID:    host1.ID(),
		Addrs: host1.Addrs(),
	})
	require.NoError(t, err)

	stream, err := host1.NewStream(ctx, h.ID(), protocol.ID(communication.BeaconBlocksByRangeProtocolV2))
	require.NoError(t, err)

	req := &cltypes.BeaconBlocksByRangeRequest{
		StartSlot: blocks[0].Block.Slot,
		Count:     6,
	}

	if err := ssz_snappy.EncodeAndWrite(stream, req); err != nil {
		return
	}

	code := make([]byte, 1)
	_, err = stream.Read(code)
	require.NoError(t, err)
	require.Equal(t, code[0], uint8(0))

	var w bytes.Buffer
	_, err = io.Copy(&w, stream)
	require.NoError(t, err)

	responsePacket := make([]*cltypes.SignedBeaconBlock, 0)

	r := bytes.NewReader(w.Bytes())
	for i := 0; i < len(blocks); i++ {
		forkDigest := make([]byte, 4)
		if _, err := r.Read(forkDigest); err != nil {
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
		}

		// Read varint for length of message.
		encodedLn, _, err := ssz_snappy.ReadUvarint(r)
		require.NoError(t, err)

		// Read bytes using snappy into a new raw buffer of side encodedLn.
		raw := make([]byte, encodedLn)
		sr := snappy.NewReader(r)
		bytesRead := 0
		for bytesRead < int(encodedLn) {
			n, err := sr.Read(raw[bytesRead:])
			require.NoError(t, err)
			bytesRead += n
		}
		// Fork digests
		respForkDigest := binary.BigEndian.Uint32(forkDigest)
		require.NoError(t, err)

		version, err := fork.ForkDigestVersion(utils.Uint32ToBytes4(respForkDigest), beaconConfig, genesisConfig.GenesisValidatorRoot)
		require.NoError(t, err)

		responseChunk := cltypes.NewSignedBeaconBlock(beaconConfig)

		require.NoError(t, responseChunk.DecodeSSZ(raw, int(version)))

		responsePacket = append(responsePacket, responseChunk)
		// TODO(issues/5884): figure out why there is this extra byte.
		r.ReadByte()
	}
	require.Equal(t, len(responsePacket), len(blocks))
	for i := 0; i < len(blocks); i++ {
		root1, err := responsePacket[i].HashSSZ()
		require.NoError(t, err)

		root2, err := blocks[i].HashSSZ()
		require.NoError(t, err)

		require.Equal(t, root1, root2)
	}

}

func TestSentinelBlocksByRoots(t *testing.T) {
	listenAddrHost := "127.0.0.1"

	ctx := context.Background()
	db, blocks, f, _, _ := loadChain(t)
	raw := persistence.NewAferoRawBlockSaver(f, &clparams.MainnetBeaconConfig)
	genesisConfig, networkConfig, beaconConfig := clparams.GetConfigsByNetwork(clparams.MainnetNetwork)
	sentinel, err := New(ctx, &SentinelConfig{
		NetworkConfig: networkConfig,
		BeaconConfig:  beaconConfig,
		GenesisConfig: genesisConfig,
		IpAddr:        listenAddrHost,
		Port:          7070,
		EnableBlocks:  true,
	}, raw, db, log.New())
	require.NoError(t, err)
	defer sentinel.Stop()

	require.NoError(t, sentinel.Start())
	h := sentinel.host

	listenAddrHost1 := "/ip4/127.0.0.1/tcp/5021"
	host1, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost1))
	require.NoError(t, err)

	err = h.Connect(ctx, peer.AddrInfo{
		ID:    host1.ID(),
		Addrs: host1.Addrs(),
	})
	require.NoError(t, err)

	stream, err := host1.NewStream(ctx, h.ID(), protocol.ID(communication.BeaconBlocksByRootProtocolV2))
	require.NoError(t, err)

	req := solid.NewHashList(1232)
	rt, err := blocks[0].Block.HashSSZ()
	require.NoError(t, err)

	req.Append(rt)
	rt, err = blocks[1].Block.HashSSZ()
	require.NoError(t, err)
	req.Append(rt)

	if err := ssz_snappy.EncodeAndWrite(stream, req); err != nil {
		return
	}

	code := make([]byte, 1)
	_, err = stream.Read(code)
	require.NoError(t, err)
	require.Equal(t, code[0], uint8(0))

	var w bytes.Buffer
	_, err = io.Copy(&w, stream)
	require.NoError(t, err)

	responsePacket := make([]*cltypes.SignedBeaconBlock, 0)

	r := bytes.NewReader(w.Bytes())
	for i := 0; i < len(blocks); i++ {
		forkDigest := make([]byte, 4)
		if _, err := r.Read(forkDigest); err != nil {
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
		}

		// Read varint for length of message.
		encodedLn, _, err := ssz_snappy.ReadUvarint(r)
		require.NoError(t, err)

		// Read bytes using snappy into a new raw buffer of side encodedLn.
		raw := make([]byte, encodedLn)
		sr := snappy.NewReader(r)
		bytesRead := 0
		for bytesRead < int(encodedLn) {
			n, err := sr.Read(raw[bytesRead:])
			require.NoError(t, err)
			bytesRead += n
		}
		// Fork digests
		respForkDigest := binary.BigEndian.Uint32(forkDigest)
		require.NoError(t, err)

		version, err := fork.ForkDigestVersion(utils.Uint32ToBytes4(respForkDigest), beaconConfig, genesisConfig.GenesisValidatorRoot)
		require.NoError(t, err)

		responseChunk := cltypes.NewSignedBeaconBlock(beaconConfig)

		require.NoError(t, responseChunk.DecodeSSZ(raw, int(version)))

		responsePacket = append(responsePacket, responseChunk)
		// TODO(issues/5884): figure out why there is this extra byte.
		r.ReadByte()
	}

	require.Equal(t, len(responsePacket), len(blocks))
	for i := 0; i < len(responsePacket); i++ {
		root1, err := responsePacket[i].HashSSZ()
		require.NoError(t, err)

		root2, err := blocks[i].HashSSZ()
		require.NoError(t, err)

		require.Equal(t, root1, root2)
	}
}

func TestSentinelStatusRequest(t *testing.T) {
	t.Skip("TODO: fix me")
	listenAddrHost := "127.0.0.1"

	ctx := context.Background()
	db, blocks, f, _, _ := loadChain(t)
	raw := persistence.NewAferoRawBlockSaver(f, &clparams.MainnetBeaconConfig)
	genesisConfig, networkConfig, beaconConfig := clparams.GetConfigsByNetwork(clparams.MainnetNetwork)
	sentinel, err := New(ctx, &SentinelConfig{
		NetworkConfig: networkConfig,
		BeaconConfig:  beaconConfig,
		GenesisConfig: genesisConfig,
		IpAddr:        listenAddrHost,
		Port:          7070,
		EnableBlocks:  true,
	}, raw, db, log.New())
	require.NoError(t, err)
	defer sentinel.Stop()

	require.NoError(t, sentinel.Start())
	h := sentinel.host

	listenAddrHost1 := "/ip4/127.0.0.1/tcp/5001"
	host1, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost1))
	require.NoError(t, err)

	err = h.Connect(ctx, peer.AddrInfo{
		ID:    host1.ID(),
		Addrs: host1.Addrs(),
	})
	require.NoError(t, err)
	req := &cltypes.Status{
		HeadRoot: blocks[0].Block.ParentRoot,
		HeadSlot: 1234,
	}
	sentinel.SetStatus(req)
	stream, err := host1.NewStream(ctx, h.ID(), protocol.ID(communication.StatusProtocolV1))
	require.NoError(t, err)

	if err := ssz_snappy.EncodeAndWrite(stream, req); err != nil {
		return
	}

	code := make([]byte, 1)
	_, err = stream.Read(code)
	require.NoError(t, err)
	require.Equal(t, code[0], uint8(0))

	resp := &cltypes.Status{}
	if err := ssz_snappy.DecodeAndReadNoForkDigest(stream, resp, 0); err != nil {
		return
	}
	require.NoError(t, err)

	require.Equal(t, resp, req)
}
