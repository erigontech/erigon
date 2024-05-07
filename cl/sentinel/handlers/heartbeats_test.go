package handlers

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
	"github.com/ledgerwatch/erigon/cl/sentinel/communication"
	"github.com/ledgerwatch/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/ledgerwatch/erigon/cl/sentinel/handshake"
	"github.com/ledgerwatch/erigon/cl/sentinel/peers"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/ledgerwatch/erigon/p2p/enr"
	"github.com/ledgerwatch/log/v3"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/stretchr/testify/require"
)

var (
	attnetsTestVal  = [8]byte{1, 5, 6}
	syncnetsTestVal = [1]byte{56}
)

func newkey() *ecdsa.PrivateKey {
	key, err := crypto.GenerateKey()
	if err != nil {
		panic("couldn't generate key: " + err.Error())
	}
	return key
}

func testLocalNode() *enode.LocalNode {
	db, err := enode.OpenDB(context.TODO(), "", "", log.Root())
	if err != nil {
		panic(err)
	}
	ln := enode.NewLocalNode(db, newkey(), log.Root())
	ln.Set(enr.WithEntry("attnets", attnetsTestVal))
	ln.Set(enr.WithEntry("syncnets", syncnetsTestVal))
	return ln
}

func TestPing(t *testing.T) {
	ctx := context.Background()

	listenAddrHost := "/ip4/127.0.0.1/tcp/4501"
	host, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost))
	require.NoError(t, err)

	listenAddrHost1 := "/ip4/127.0.0.1/tcp/4503"
	host1, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost1))
	require.NoError(t, err)

	err = host.Connect(ctx, peer.AddrInfo{
		ID:    host1.ID(),
		Addrs: host1.Addrs(),
	})
	require.NoError(t, err)

	peersPool := peers.NewPool()
	beaconDB, indiciesDB := setupStore(t)

	f := forkchoice.NewForkChoiceStorageMock()

	genesisCfg, _, beaconCfg := clparams.GetConfigsByNetwork(1)
	c := NewConsensusHandlers(
		ctx,
		beaconDB,
		indiciesDB,
		host,
		peersPool,
		&clparams.NetworkConfig{},
		testLocalNode(),
		beaconCfg,
		genesisCfg,
		nil, f, nil, true,
	)
	c.Start()

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.PingProtocolV1))
	require.NoError(t, err)

	_, err = stream.Write(nil)
	require.NoError(t, err)

	firstByte := make([]byte, 1)
	_, err = stream.Read(firstByte)
	require.NoError(t, err)
	require.Equal(t, firstByte[0], byte(0))

	p := &cltypes.Ping{}

	err = ssz_snappy.DecodeAndReadNoForkDigest(stream, p, clparams.Phase0Version)
	require.NoError(t, err)
}

func TestGoodbye(t *testing.T) {
	ctx := context.Background()

	listenAddrHost := "/ip4/127.0.0.1/tcp/4509"
	host, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost))
	require.NoError(t, err)

	listenAddrHost1 := "/ip4/127.0.0.1/tcp/4512"
	host1, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost1))
	require.NoError(t, err)

	err = host.Connect(ctx, peer.AddrInfo{
		ID:    host1.ID(),
		Addrs: host1.Addrs(),
	})
	require.NoError(t, err)

	peersPool := peers.NewPool()
	beaconDB, indiciesDB := setupStore(t)

	f := forkchoice.NewForkChoiceStorageMock()

	genesisCfg, _, beaconCfg := clparams.GetConfigsByNetwork(1)
	c := NewConsensusHandlers(
		ctx,
		beaconDB,
		indiciesDB,
		host,
		peersPool,
		&clparams.NetworkConfig{},
		testLocalNode(),
		beaconCfg,
		genesisCfg,
		nil, f, nil, true,
	)
	c.Start()

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.GoodbyeProtocolV1))
	require.NoError(t, err)

	req := &cltypes.Ping{}
	var reqBuf bytes.Buffer
	if err := ssz_snappy.EncodeAndWrite(&reqBuf, req); err != nil {
		return
	}

	_, err = stream.Write(reqBuf.Bytes())
	require.NoError(t, err)

	firstByte := make([]byte, 1)
	_, err = stream.Read(firstByte)
	require.NoError(t, err)
	require.Equal(t, firstByte[0], byte(0))

	p := &cltypes.Ping{}

	err = ssz_snappy.DecodeAndReadNoForkDigest(stream, p, clparams.Phase0Version)
	require.NoError(t, err)
}

func TestMetadataV2(t *testing.T) {
	ctx := context.Background()

	listenAddrHost := "/ip4/127.0.0.1/tcp/2509"
	host, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost))
	require.NoError(t, err)

	listenAddrHost1 := "/ip4/127.0.0.1/tcp/7510"
	host1, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost1))
	require.NoError(t, err)

	err = host.Connect(ctx, peer.AddrInfo{
		ID:    host1.ID(),
		Addrs: host1.Addrs(),
	})
	require.NoError(t, err)

	peersPool := peers.NewPool()
	beaconDB, indiciesDB := setupStore(t)

	f := forkchoice.NewForkChoiceStorageMock()

	nc := clparams.NetworkConfigs[clparams.MainnetNetwork]
	genesisCfg, _, beaconCfg := clparams.GetConfigsByNetwork(1)
	c := NewConsensusHandlers(
		ctx,
		beaconDB,
		indiciesDB,
		host,
		peersPool,
		&nc,
		testLocalNode(),
		beaconCfg,
		genesisCfg,
		nil, f, nil, true,
	)
	c.Start()

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.MetadataProtocolV2))
	require.NoError(t, err)

	_, err = stream.Write(nil)
	require.NoError(t, err)

	firstByte := make([]byte, 1)
	_, err = stream.Read(firstByte)
	require.NoError(t, err)
	require.Equal(t, firstByte[0], byte(0))

	p := &cltypes.Metadata{}

	err = ssz_snappy.DecodeAndReadNoForkDigest(stream, p, clparams.Phase0Version)
	require.NoError(t, err)

	require.Equal(t, attnetsTestVal, p.Attnets)
	require.Equal(t, &syncnetsTestVal, p.Syncnets)
}

func TestMetadataV1(t *testing.T) {
	ctx := context.Background()

	listenAddrHost := "/ip4/127.0.0.1/tcp/4519"
	host, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost))
	require.NoError(t, err)

	listenAddrHost1 := "/ip4/127.0.0.1/tcp/4578"
	host1, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost1))
	require.NoError(t, err)

	err = host.Connect(ctx, peer.AddrInfo{
		ID:    host1.ID(),
		Addrs: host1.Addrs(),
	})
	require.NoError(t, err)

	peersPool := peers.NewPool()
	beaconDB, indiciesDB := setupStore(t)

	f := forkchoice.NewForkChoiceStorageMock()

	nc := clparams.NetworkConfigs[clparams.MainnetNetwork]
	genesisCfg, _, beaconCfg := clparams.GetConfigsByNetwork(1)
	c := NewConsensusHandlers(
		ctx,
		beaconDB,
		indiciesDB,
		host,
		peersPool,
		&nc,
		testLocalNode(),
		beaconCfg,
		genesisCfg,
		nil, f, nil, true,
	)
	c.Start()

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.MetadataProtocolV1))
	require.NoError(t, err)

	_, err = stream.Write(nil)
	require.NoError(t, err)

	firstByte := make([]byte, 1)
	_, err = stream.Read(firstByte)
	require.NoError(t, err)
	require.Equal(t, firstByte[0], byte(0))

	p := &cltypes.Metadata{}

	err = ssz_snappy.DecodeAndReadNoForkDigest(stream, p, clparams.Phase0Version)
	require.NoError(t, err)

	require.Equal(t, attnetsTestVal, p.Attnets)
}

func TestStatus(t *testing.T) {
	ctx := context.Background()

	listenAddrHost := "/ip4/127.0.0.1/tcp/1519"
	host, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost))
	require.NoError(t, err)

	listenAddrHost1 := "/ip4/127.0.0.1/tcp/4518"
	host1, err := libp2p.New(libp2p.ListenAddrStrings(listenAddrHost1))
	require.NoError(t, err)

	err = host.Connect(ctx, peer.AddrInfo{
		ID:    host1.ID(),
		Addrs: host1.Addrs(),
	})
	require.NoError(t, err)

	peersPool := peers.NewPool()
	beaconDB, indiciesDB := setupStore(t)

	f := forkchoice.NewForkChoiceStorageMock()

	hs := handshake.New(ctx, &clparams.GenesisConfig{}, &clparams.MainnetBeaconConfig, nil)
	s := &cltypes.Status{
		FinalizedRoot:  libcommon.Hash{1, 2, 4},
		HeadRoot:       libcommon.Hash{1, 2, 4},
		FinalizedEpoch: 1,
		HeadSlot:       1,
	}
	hs.SetStatus(s)
	nc := clparams.NetworkConfigs[clparams.MainnetNetwork]
	genesisCfg, _, beaconCfg := clparams.GetConfigsByNetwork(1)
	c := NewConsensusHandlers(
		ctx,
		beaconDB,
		indiciesDB,
		host,
		peersPool,
		&nc,
		testLocalNode(),
		beaconCfg,
		genesisCfg,
		hs, f, nil, true,
	)
	c.Start()

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.StatusProtocolV1))
	require.NoError(t, err)

	_, err = stream.Write(nil)
	require.NoError(t, err)

	firstByte := make([]byte, 1)
	_, err = stream.Read(firstByte)
	require.NoError(t, err)
	require.Equal(t, firstByte[0], byte(0))

	p := &cltypes.Status{}

	err = ssz_snappy.DecodeAndReadNoForkDigest(stream, p, clparams.Phase0Version)
	require.NoError(t, err)

	require.Equal(t, s, p)
}
