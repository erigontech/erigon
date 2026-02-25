// Copyright 2024 The Erigon Authors
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

package sentinel

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/antiquary"
	antiquarytests "github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/clparams/initial_state"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	peerdasstatemock "github.com/erigontech/erigon/cl/das/state/mock_services"
	"github.com/erigontech/erigon/cl/p2p"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/sentinel/communication"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

// noErr panics if err is non-nil. Used with retryTestFunc so recover() can catch it.
func noErr(err error) {
	if err != nil {
		panic(err)
	}
}

// assertPanic panics with a formatted message if cond is false.
func assertPanic(cond bool, format string, args ...any) {
	if !cond {
		panic(fmt.Sprintf(format, args...))
	}
}

// retryTestFunc retries fn up to maxRetries times if it panics (e.g. from noErr/assertPanic).
// This works around transient libp2p races where protocol negotiation fails with
// "failed to negotiate protocol: stream reset" on macOS CI runners.
func retryTestFunc(t *testing.T, maxRetries int, fn func()) {
	t.Helper()
	for attempt := 1; attempt <= maxRetries; attempt++ {
		failed := false
		func() {
			defer func() {
				if r := recover(); r != nil {
					failed = true
					t.Logf("attempt %d/%d failed: %v", attempt, maxRetries, r)
				}
			}()
			fn()
		}()
		if !failed {
			return
		}
		time.Sleep(time.Second)
		if attempt == maxRetries {
			// Last attempt — run without recovery so it properly fails the test
			fn()
		}
	}
}

func getEthClock(t *testing.T) eth_clock.EthereumClock {
	s, err := initial_state.GetGenesisState(chainspec.MainnetChainID)
	noErr(err)
	return eth_clock.NewEthereumClock(s.GenesisTime(), s.GenesisValidatorsRoot(), s.BeaconConfig())
}

func loadChain(t *testing.T) (db kv.RwDB, blocks []*cltypes.SignedBeaconBlock, preState, postState *state.CachingBeaconState, reader *antiquarytests.MockBlockReader) {
	blocks, preState, postState = antiquarytests.GetPhase0Random()
	db = memdb.NewTestDB(t, dbcfg.ChainDB)
	reader = antiquarytests.LoadChain(blocks, postState, db, t)

	sn := synced_data.NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	noErr(sn.OnHeadState(postState))

	ctx := context.Background()
	vt := state_accessors.NewStaticValidatorTable()
	a := antiquary.NewAntiquary(ctx, nil, preState, vt, &clparams.MainnetBeaconConfig, datadir.New(t.TempDir()), nil, db, nil, nil, reader, sn, log.New(), true, true, false, false, nil)
	noErr(a.IncrementBeaconState(ctx, blocks[len(blocks)-1].Block.Slot+33))
	return
}

func newTestP2PManager(t *testing.T, ethClock eth_clock.EthereumClock) p2p.P2PManager {
	networkConfig, beaconConfig := clparams.GetConfigsByNetwork(chainspec.MainnetChainID)
	pm, err := p2p.NewP2Pmanager(context.Background(), &p2p.P2PConfig{
		NetworkConfig: networkConfig,
		BeaconConfig:  beaconConfig,
		IpAddr:        "127.0.0.1",
		Port:          0,
		TCPPort:       0,
		NoDiscovery:   true,
		MaxPeerCount:  100,
	}, log.New(), ethClock)
	noErr(err)
	t.Cleanup(func() { pm.Host().Close() })
	return pm
}

func newTestSentinel(t *testing.T, ethClock eth_clock.EthereumClock, reader freezeblocks.BeaconSnapshotReader, db kv.RoDB, mockPeerDasStateReader *peerdasstatemock.MockPeerDasStateReader) *Sentinel {
	networkConfig, beaconConfig := clparams.GetConfigsByNetwork(chainspec.MainnetChainID)
	pm := newTestP2PManager(t, ethClock)
	sent, err := New(context.Background(), &SentinelConfig{
		NetworkConfig: networkConfig,
		BeaconConfig:  beaconConfig,
		EnableBlocks:  true,
		MaxPeerCount:  100,
	}, ethClock, reader, nil, db, log.New(), &mock_services.ForkChoiceStorageMock{}, nil, mockPeerDasStateReader, pm)
	noErr(err)
	t.Cleanup(func() { sent.Stop() })

	_, err = sent.Start()
	noErr(err)
	return sent
}

func newMockPeerDasStateReader(t *testing.T) *peerdasstatemock.MockPeerDasStateReader {
	ctrl := gomock.NewController(t)
	m := peerdasstatemock.NewMockPeerDasStateReader(ctrl)
	m.EXPECT().GetEarliestAvailableSlot().Return(uint64(0)).AnyTimes()
	m.EXPECT().GetRealCgc().Return(uint64(0)).AnyTimes()
	m.EXPECT().GetAdvertisedCgc().Return(uint64(0)).AnyTimes()
	return m
}

func testSentinelBlocksByRange(t *testing.T) {
	ethClock := getEthClock(t)
	ctx := context.Background()
	db, blocks, _, _, reader := loadChain(t)
	_, beaconConfig := clparams.GetConfigsByNetwork(chainspec.MainnetChainID)

	sent := newTestSentinel(t, ethClock, reader, db, newMockPeerDasStateReader(t))
	h := sent.Host()

	host1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	noErr(err)
	defer host1.Close()

	noErr(h.Connect(ctx, peer.AddrInfo{ID: host1.ID(), Addrs: host1.Addrs()}))

	stream, err := host1.NewStream(ctx, h.ID(), protocol.ID(communication.BeaconBlocksByRangeProtocolV2))
	noErr(err)
	defer stream.Close()

	req := &cltypes.BeaconBlocksByRangeRequest{
		StartSlot: blocks[0].Block.Slot,
		Count:     6,
	}
	noErr(ssz_snappy.EncodeAndWrite(stream, req))

	code := make([]byte, 1)
	_, err = stream.Read(code)
	noErr(err)
	assertPanic(code[0] == uint8(0), "expected code[0]=0, got %d", code[0])

	var w bytes.Buffer
	_, err = io.Copy(&w, stream)
	noErr(err)

	responsePacket := make([]*cltypes.SignedBeaconBlock, 0)
	r := bytes.NewReader(w.Bytes())
	for i := 0; i < len(blocks); i++ {
		forkDigest := make([]byte, 4)
		if _, err := r.Read(forkDigest); err != nil {
			if err == io.EOF {
				break
			}
			noErr(err)
		}

		encodedLn, _, err := ssz_snappy.ReadUvarint(r)
		noErr(err)

		raw := make([]byte, encodedLn)
		sr := snappy.NewReader(r)
		bytesRead := 0
		for bytesRead < int(encodedLn) {
			n, err := sr.Read(raw[bytesRead:])
			noErr(err)
			bytesRead += n
		}

		version, err := ethClock.StateVersionByForkDigest(utils.Uint32ToBytes4(binary.BigEndian.Uint32(forkDigest)))
		noErr(err)

		responseChunk := cltypes.NewSignedBeaconBlock(beaconConfig, clparams.DenebVersion)
		noErr(responseChunk.DecodeSSZ(raw, int(version)))

		responsePacket = append(responsePacket, responseChunk)
		r.ReadByte()
	}
	assertPanic(len(blocks) == len(responsePacket), "expected %d blocks, got %d", len(blocks), len(responsePacket))
	for i := 0; i < len(blocks); i++ {
		root1, err := responsePacket[i].HashSSZ()
		noErr(err)
		root2, err := blocks[i].HashSSZ()
		noErr(err)
		assertPanic(root1 == root2, "block %d root mismatch: %x != %x", i, root1, root2)
	}
}

func testSentinelBlocksByRoots(t *testing.T) {
	ctx := context.Background()
	db, blocks, _, _, reader := loadChain(t)
	ethClock := getEthClock(t)
	_, beaconConfig := clparams.GetConfigsByNetwork(chainspec.MainnetChainID)

	sent := newTestSentinel(t, ethClock, reader, db, newMockPeerDasStateReader(t))
	h := sent.Host()

	host1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	noErr(err)
	defer host1.Close()

	noErr(h.Connect(ctx, peer.AddrInfo{ID: host1.ID(), Addrs: host1.Addrs()}))

	stream, err := host1.NewStream(ctx, h.ID(), protocol.ID(communication.BeaconBlocksByRootProtocolV2))
	noErr(err)
	defer stream.Close()

	req := solid.NewHashList(1232)
	rt, err := blocks[0].Block.HashSSZ()
	noErr(err)
	req.Append(rt)
	rt, err = blocks[1].Block.HashSSZ()
	noErr(err)
	req.Append(rt)

	noErr(ssz_snappy.EncodeAndWrite(stream, req))

	code := make([]byte, 1)
	_, err = stream.Read(code)
	noErr(err)
	assertPanic(code[0] == uint8(0), "expected code[0]=0, got %d", code[0])

	var w bytes.Buffer
	_, err = io.Copy(&w, stream)
	noErr(err)

	responsePacket := make([]*cltypes.SignedBeaconBlock, 0)
	r := bytes.NewReader(w.Bytes())
	for i := 0; i < len(blocks); i++ {
		forkDigest := make([]byte, 4)
		if _, err := r.Read(forkDigest); err != nil {
			if err == io.EOF {
				break
			}
			noErr(err)
		}

		encodedLn, _, err := ssz_snappy.ReadUvarint(r)
		noErr(err)

		raw := make([]byte, encodedLn)
		sr := snappy.NewReader(r)
		bytesRead := 0
		for bytesRead < int(encodedLn) {
			n, err := sr.Read(raw[bytesRead:])
			noErr(err)
			bytesRead += n
		}

		version, err := ethClock.StateVersionByForkDigest(utils.Uint32ToBytes4(binary.BigEndian.Uint32(forkDigest)))
		noErr(err)

		responseChunk := cltypes.NewSignedBeaconBlock(beaconConfig, clparams.DenebVersion)
		noErr(responseChunk.DecodeSSZ(raw, int(version)))

		responsePacket = append(responsePacket, responseChunk)
		r.ReadByte()
	}
	assertPanic(len(blocks) == len(responsePacket), "expected %d blocks, got %d", len(blocks), len(responsePacket))
	for i := 0; i < len(responsePacket); i++ {
		root1, err := responsePacket[i].HashSSZ()
		noErr(err)
		root2, err := blocks[i].HashSSZ()
		noErr(err)
		assertPanic(root1 == root2, "block %d root mismatch: %x != %x", i, root1, root2)
	}
}

func testSentinelStatusRequest(t *testing.T) {
	ctx := context.Background()
	db, blocks, _, _, reader := loadChain(t)
	ethClock := getEthClock(t)

	sent := newTestSentinel(t, ethClock, reader, db, newMockPeerDasStateReader(t))
	h := sent.Host()

	host1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	noErr(err)
	defer host1.Close()

	noErr(h.Connect(ctx, peer.AddrInfo{ID: host1.ID(), Addrs: host1.Addrs()}))

	req := &cltypes.Status{
		HeadRoot:       common.Hash(blocks[0].Block.ParentRoot),
		HeadSlot:       1234,
		FinalizedRoot:  common.Hash{},
		FinalizedEpoch: 0,
	}
	sent.SetStatus(req)

	stream, err := host1.NewStream(ctx, h.ID(), protocol.ID(communication.StatusProtocolV1))
	noErr(err)
	defer stream.Close()

	noErr(ssz_snappy.EncodeAndWrite(stream, req))

	code := make([]byte, 1)
	_, err = stream.Read(code)
	noErr(err)
	assertPanic(code[0] == uint8(0), "expected code[0]=0, got %d", code[0])

	resp := &cltypes.Status{}
	noErr(ssz_snappy.DecodeAndReadNoForkDigest(stream, resp, 0))

	assertPanic(req.HeadRoot == resp.HeadRoot, "HeadRoot mismatch: %v != %v", req.HeadRoot, resp.HeadRoot)
	assertPanic(req.HeadSlot == resp.HeadSlot, "HeadSlot mismatch: %v != %v", req.HeadSlot, resp.HeadSlot)
	assertPanic(req.FinalizedRoot == resp.FinalizedRoot, "FinalizedRoot mismatch: %v != %v", req.FinalizedRoot, resp.FinalizedRoot)
	assertPanic(req.FinalizedEpoch == resp.FinalizedEpoch, "FinalizedEpoch mismatch: %v != %v", req.FinalizedEpoch, resp.FinalizedEpoch)
}

func TestSentinelBlocksByRange(t *testing.T) {
	retryTestFunc(t, 3, func() { testSentinelBlocksByRange(t) })
}

func TestSentinelBlocksByRoots(t *testing.T) {
	retryTestFunc(t, 3, func() { testSentinelBlocksByRoots(t) })
}

func TestSentinelStatusRequest(t *testing.T) {
	retryTestFunc(t, 3, func() { testSentinelStatusRequest(t) })
}
