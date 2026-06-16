// Copyright 2026 The Erigon Authors
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

package handlers

import (
	"bytes"
	"context"
	"io"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/sentinel/communication"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/sentinel/peers"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
)

var initDataColumnSidecarTestConfig sync.Once

func TestDataColumnSidecarsByRootMissingRootReturnsResourceUnavailable(t *testing.T) {
	ctx, host, host1, beaconCfg := setupDataColumnSidecarHandlerTest(t)

	req := solid.NewDynamicListSSZ[*cltypes.DataColumnsByRootIdentifier](1)
	id := &cltypes.DataColumnsByRootIdentifier{
		BlockRoot: common.Hash{0x42},
		Columns:   solid.NewUint64ListSSZ(int(beaconCfg.NumberOfColumns)),
	}
	id.Columns.Append(0)
	req.Append(id)

	var reqBuf bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&reqBuf, req))

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.DataColumnSidecarsByRootProtocolV1))
	require.NoError(t, err)
	t.Cleanup(func() { stream.Close() })

	expectDataColumnSidecarResourceUnavailable(t, stream, reqBuf.Bytes())
}

func TestDataColumnSidecarsByRootBeforeFuluReturnsResourceUnavailable(t *testing.T) {
	ctx, host, host1, beaconCfg := setupDataColumnSidecarHandlerTestWithFuluForkEpoch(t, math.MaxUint64)

	req := solid.NewDynamicListSSZ[*cltypes.DataColumnsByRootIdentifier](1)
	id := &cltypes.DataColumnsByRootIdentifier{
		BlockRoot: common.Hash{0x42},
		Columns:   solid.NewUint64ListSSZ(int(beaconCfg.NumberOfColumns)),
	}
	id.Columns.Append(0)
	req.Append(id)

	var reqBuf bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&reqBuf, req))

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.DataColumnSidecarsByRootProtocolV1))
	require.NoError(t, err)
	t.Cleanup(func() { stream.Close() })

	expectDataColumnSidecarResourceUnavailable(t, stream, reqBuf.Bytes())
}

func TestDataColumnSidecarsByRootBeforeFuluInvalidColumnReturnsInvalidRequest(t *testing.T) {
	ctx, host, host1, beaconCfg := setupDataColumnSidecarHandlerTestWithFuluForkEpoch(t, math.MaxUint64)

	req := solid.NewDynamicListSSZ[*cltypes.DataColumnsByRootIdentifier](1)
	id := &cltypes.DataColumnsByRootIdentifier{
		BlockRoot: common.Hash{0x42},
		Columns:   solid.NewUint64ListSSZ(int(beaconCfg.NumberOfColumns) + 1),
	}
	id.Columns.Append(beaconCfg.NumberOfColumns)
	req.Append(id)

	var reqBuf bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&reqBuf, req))

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.DataColumnSidecarsByRootProtocolV1))
	require.NoError(t, err)
	t.Cleanup(func() { stream.Close() })

	expectDataColumnSidecarResponsePrefix(t, stream, reqBuf.Bytes(), InvalidRequestPrefix)
}

func TestDataColumnSidecarsByRootFoundSidecarReturnsSuccessWithoutTrailingResourceUnavailable(t *testing.T) {
	ctx, host, host1, beaconCfg, indiciesDB, columnStorage := setupDataColumnSidecarHandlerTestWithStore(t, 0)

	tx, err := indiciesDB.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	blocks := populateDatabaseWithBlocks(t, tests.NewMockBlockReader(), tx, getEthClock(t).GetCurrentSlot(), 0)
	require.NoError(t, tx.Commit())

	header := blocks[0].SignedBeaconBlockHeader()
	slot := header.Header.Slot
	blockRoot, err := header.Header.HashSSZ()
	require.NoError(t, err)
	columnIndex := uint64(0)
	sidecar := cltypes.NewDataColumnSidecar()
	sidecar.Index = columnIndex
	sidecar.SignedBlockHeader.Header.Slot = slot
	require.NoError(t, columnStorage.WriteColumnSidecars(ctx, blockRoot, int64(columnIndex), sidecar))

	req := solid.NewDynamicListSSZ[*cltypes.DataColumnsByRootIdentifier](1)
	id := &cltypes.DataColumnsByRootIdentifier{
		BlockRoot: blockRoot,
		Columns:   solid.NewUint64ListSSZ(int(beaconCfg.NumberOfColumns)),
	}
	id.Columns.Append(columnIndex)
	req.Append(id)

	var reqBuf bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&reqBuf, req))

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.DataColumnSidecarsByRootProtocolV1))
	require.NoError(t, err)
	t.Cleanup(func() { stream.Close() })
	require.NoError(t, stream.SetDeadline(time.Now().Add(5*time.Second)))

	_, err = stream.Write(reqBuf.Bytes())
	require.NoError(t, err)

	firstByte := make([]byte, 1)
	_, err = io.ReadFull(stream, firstByte)
	require.NoError(t, err)
	require.Equal(t, byte(SuccessfulResponsePrefix), firstByte[0])

	forkDigest := make([]byte, 4)
	_, err = io.ReadFull(stream, forkDigest)
	require.NoError(t, err)

	got := cltypes.NewDataColumnSidecar()
	require.NoError(t, ssz_snappy.DecodeAndReadNoForkDigest(stream, got, clparams.FuluVersion))
	require.Equal(t, columnIndex, got.Index)

	require.NoError(t, stream.SetReadDeadline(time.Now().Add(200*time.Millisecond)))
	buf := make([]byte, 1)
	n, err := stream.Read(buf)
	require.Zero(t, n)
	require.Error(t, err)
}

func TestDataColumnSidecarsByRootEmptyRequestReturnsEmptySuccess(t *testing.T) {
	ctx, host, host1, _ := setupDataColumnSidecarHandlerTest(t)

	req := solid.NewDynamicListSSZ[*cltypes.DataColumnsByRootIdentifier](1)

	var reqBuf bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&reqBuf, req))

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.DataColumnSidecarsByRootProtocolV1))
	require.NoError(t, err)
	t.Cleanup(func() { stream.Close() })

	expectDataColumnSidecarEmptyResponse(t, stream, reqBuf.Bytes())
}

func TestDataColumnSidecarsByRootEmptyColumnsReturnsEmptySuccess(t *testing.T) {
	ctx, host, host1, beaconCfg := setupDataColumnSidecarHandlerTest(t)

	req := solid.NewDynamicListSSZ[*cltypes.DataColumnsByRootIdentifier](1)
	req.Append(&cltypes.DataColumnsByRootIdentifier{
		BlockRoot: common.Hash{0x42},
		Columns:   solid.NewUint64ListSSZ(int(beaconCfg.NumberOfColumns)),
	})

	var reqBuf bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&reqBuf, req))

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.DataColumnSidecarsByRootProtocolV1))
	require.NoError(t, err)
	t.Cleanup(func() { stream.Close() })

	expectDataColumnSidecarEmptyResponse(t, stream, reqBuf.Bytes())
}

func TestDataColumnSidecarsByRootInvalidColumnReturnsInvalidRequest(t *testing.T) {
	ctx, host, host1, beaconCfg := setupDataColumnSidecarHandlerTest(t)

	req := solid.NewDynamicListSSZ[*cltypes.DataColumnsByRootIdentifier](1)
	id := &cltypes.DataColumnsByRootIdentifier{
		BlockRoot: common.Hash{0x42},
		Columns:   solid.NewUint64ListSSZ(int(beaconCfg.NumberOfColumns) + 1),
	}
	id.Columns.Append(beaconCfg.NumberOfColumns)
	req.Append(id)

	var reqBuf bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&reqBuf, req))

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.DataColumnSidecarsByRootProtocolV1))
	require.NoError(t, err)
	t.Cleanup(func() { stream.Close() })

	expectDataColumnSidecarResponsePrefix(t, stream, reqBuf.Bytes(), InvalidRequestPrefix)
}

func TestDataColumnSidecarsByRootTooManyColumnsReturnsInvalidRequest(t *testing.T) {
	ctx, host, host1, beaconCfg := setupDataColumnSidecarHandlerTest(t)

	req := solid.NewDynamicListSSZ[*cltypes.DataColumnsByRootIdentifier](1)
	id := &cltypes.DataColumnsByRootIdentifier{
		BlockRoot: common.Hash{0x42},
		Columns:   solid.NewUint64ListSSZ(int(beaconCfg.NumberOfColumns) + 1),
	}
	for range beaconCfg.NumberOfColumns + 1 {
		id.Columns.Append(0)
	}
	req.Append(id)

	var reqBuf bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&reqBuf, req))

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.DataColumnSidecarsByRootProtocolV1))
	require.NoError(t, err)
	t.Cleanup(func() { stream.Close() })

	expectDataColumnSidecarResponsePrefix(t, stream, reqBuf.Bytes(), InvalidRequestPrefix)
}

func TestDataColumnSidecarsByRangeZeroCountReturnsEmptySuccess(t *testing.T) {
	ctx, host, host1, beaconCfg := setupDataColumnSidecarHandlerTest(t)

	req := &cltypes.ColumnSidecarsByRangeRequest{
		StartSlot: 0,
		Count:     0,
		Columns:   solid.NewUint64ListSSZ(int(beaconCfg.NumberOfColumns)),
	}
	req.Columns.Append(0)

	var reqBuf bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&reqBuf, req))

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.DataColumnSidecarsByRangeProtocolV1))
	require.NoError(t, err)
	t.Cleanup(func() { stream.Close() })

	expectDataColumnSidecarEmptyResponse(t, stream, reqBuf.Bytes())
}

func TestDataColumnSidecarsByRangeEmptyColumnsReturnsEmptySuccess(t *testing.T) {
	ctx, host, host1, beaconCfg := setupDataColumnSidecarHandlerTest(t)

	req := &cltypes.ColumnSidecarsByRangeRequest{
		StartSlot: 0,
		Count:     1,
		Columns:   solid.NewUint64ListSSZ(int(beaconCfg.NumberOfColumns)),
	}

	var reqBuf bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&reqBuf, req))

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.DataColumnSidecarsByRangeProtocolV1))
	require.NoError(t, err)
	t.Cleanup(func() { stream.Close() })

	expectDataColumnSidecarEmptyResponse(t, stream, reqBuf.Bytes())
}

func TestDataColumnSidecarsByRangeMissingSidecarsReturnsEmptySuccess(t *testing.T) {
	ctx, host, host1, beaconCfg := setupDataColumnSidecarHandlerTest(t)

	req := &cltypes.ColumnSidecarsByRangeRequest{
		StartSlot: 0,
		Count:     1,
		Columns:   solid.NewUint64ListSSZ(int(beaconCfg.NumberOfColumns)),
	}
	req.Columns.Append(0)

	var reqBuf bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&reqBuf, req))

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.DataColumnSidecarsByRangeProtocolV1))
	require.NoError(t, err)
	t.Cleanup(func() { stream.Close() })

	expectDataColumnSidecarEmptyResponse(t, stream, reqBuf.Bytes())
}

func TestDataColumnSidecarsByRangeFutureRangeReturnsEmptySuccess(t *testing.T) {
	ctx, host, host1, beaconCfg := setupDataColumnSidecarHandlerTest(t)

	req := &cltypes.ColumnSidecarsByRangeRequest{
		StartSlot: getEthClock(t).GetCurrentSlot() + 1,
		Count:     1,
		Columns:   solid.NewUint64ListSSZ(int(beaconCfg.NumberOfColumns)),
	}
	req.Columns.Append(0)

	var reqBuf bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&reqBuf, req))

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.DataColumnSidecarsByRangeProtocolV1))
	require.NoError(t, err)
	t.Cleanup(func() { stream.Close() })

	expectDataColumnSidecarEmptyResponse(t, stream, reqBuf.Bytes())
}

func TestDataColumnSidecarsByRangeBeforeFuluReturnsEmptySuccess(t *testing.T) {
	ctx, host, host1, beaconCfg := setupDataColumnSidecarHandlerTestWithFuluForkEpoch(t, 1)

	req := &cltypes.ColumnSidecarsByRangeRequest{
		StartSlot: 0,
		Count:     beaconCfg.SlotsPerEpoch,
		Columns:   solid.NewUint64ListSSZ(int(beaconCfg.NumberOfColumns)),
	}
	req.Columns.Append(0)

	var reqBuf bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&reqBuf, req))

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.DataColumnSidecarsByRangeProtocolV1))
	require.NoError(t, err)
	t.Cleanup(func() { stream.Close() })

	expectDataColumnSidecarEmptyResponse(t, stream, reqBuf.Bytes())
}

func TestDataColumnSidecarsByRangeInvalidColumnReturnsInvalidRequest(t *testing.T) {
	ctx, host, host1, beaconCfg := setupDataColumnSidecarHandlerTest(t)

	req := &cltypes.ColumnSidecarsByRangeRequest{
		StartSlot: 0,
		Count:     1,
		Columns:   solid.NewUint64ListSSZ(int(beaconCfg.NumberOfColumns) + 1),
	}
	req.Columns.Append(beaconCfg.NumberOfColumns)

	var reqBuf bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&reqBuf, req))

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.DataColumnSidecarsByRangeProtocolV1))
	require.NoError(t, err)
	t.Cleanup(func() { stream.Close() })

	expectDataColumnSidecarResponsePrefix(t, stream, reqBuf.Bytes(), InvalidRequestPrefix)
}

func TestDataColumnSidecarsByRangeTooManyColumnsReturnsInvalidRequest(t *testing.T) {
	ctx, host, host1, beaconCfg := setupDataColumnSidecarHandlerTest(t)

	req := &cltypes.ColumnSidecarsByRangeRequest{
		StartSlot: 0,
		Count:     1,
		Columns:   solid.NewUint64ListSSZ(int(beaconCfg.NumberOfColumns) + 1),
	}
	for range beaconCfg.NumberOfColumns + 1 {
		req.Columns.Append(0)
	}

	var reqBuf bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&reqBuf, req))

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.DataColumnSidecarsByRangeProtocolV1))
	require.NoError(t, err)
	t.Cleanup(func() { stream.Close() })

	expectDataColumnSidecarResponsePrefix(t, stream, reqBuf.Bytes(), InvalidRequestPrefix)
}

func TestDataColumnSidecarsByRangeOverflowReturnsInvalidRequest(t *testing.T) {
	ctx, host, host1, beaconCfg := setupDataColumnSidecarHandlerTest(t)

	req := &cltypes.ColumnSidecarsByRangeRequest{
		StartSlot: math.MaxUint64,
		Count:     1,
		Columns:   solid.NewUint64ListSSZ(int(beaconCfg.NumberOfColumns)),
	}
	req.Columns.Append(0)

	var reqBuf bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&reqBuf, req))

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.DataColumnSidecarsByRangeProtocolV1))
	require.NoError(t, err)
	t.Cleanup(func() { stream.Close() })

	expectDataColumnSidecarResponsePrefix(t, stream, reqBuf.Bytes(), InvalidRequestPrefix)
}

func TestDataColumnSidecarsByRangeTooLargeReturnsInvalidRequest(t *testing.T) {
	ctx, host, host1, beaconCfg := setupDataColumnSidecarHandlerTest(t)

	req := &cltypes.ColumnSidecarsByRangeRequest{
		StartSlot: 0,
		Count:     beaconCfg.MinEpochsForDataColumnSidecarsRequests*beaconCfg.SlotsPerEpoch + 1,
		Columns:   solid.NewUint64ListSSZ(int(beaconCfg.NumberOfColumns)),
	}
	req.Columns.Append(0)

	var reqBuf bytes.Buffer
	require.NoError(t, ssz_snappy.EncodeAndWrite(&reqBuf, req))

	stream, err := host1.NewStream(ctx, host.ID(), protocol.ID(communication.DataColumnSidecarsByRangeProtocolV1))
	require.NoError(t, err)
	t.Cleanup(func() { stream.Close() })

	expectDataColumnSidecarResponsePrefix(t, stream, reqBuf.Bytes(), InvalidRequestPrefix)
}

func setupDataColumnSidecarHandlerTest(t *testing.T) (context.Context, host.Host, host.Host, *clparams.BeaconChainConfig) {
	ctx, server, client, beaconCfg, _, _ := setupDataColumnSidecarHandlerTestWithStore(t, 0)
	return ctx, server, client, beaconCfg
}

func setupDataColumnSidecarHandlerTestWithFuluForkEpoch(t *testing.T, fuluForkEpoch uint64) (context.Context, host.Host, host.Host, *clparams.BeaconChainConfig) {
	ctx, server, client, beaconCfg, _, _ := setupDataColumnSidecarHandlerTestWithStore(t, fuluForkEpoch)
	return ctx, server, client, beaconCfg
}

func setupDataColumnSidecarHandlerTestWithStore(t *testing.T, fuluForkEpoch uint64) (context.Context, host.Host, host.Host, *clparams.BeaconChainConfig, kv.RwDB, blob_storage.DataColumnStorage) {
	t.Helper()

	ctx := context.Background()

	server, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })

	client, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	require.NoError(t, server.Connect(ctx, peer.AddrInfo{
		ID:    client.ID(),
		Addrs: client.Addrs(),
	}))

	_, indiciesDB := setupStore(t)
	t.Cleanup(func() { indiciesDB.Close() })

	ethClock := getEthClock(t)
	_, mainnetCfg := clparams.GetConfigsByNetwork(1)
	initDataColumnSidecarTestConfig.Do(func() {
		if clparams.GetBeaconConfig() == nil {
			clparams.InitGlobalStaticConfig(mainnetCfg, &clparams.CaplinConfig{})
		}
	})
	beaconCfg := *mainnetCfg
	beaconCfg.FuluForkEpoch = fuluForkEpoch
	columnStorage := blob_storage.NewDataColumnStore(afero.NewMemMapFs(), 1000, &beaconCfg, ethClock, beaconevents.NewEventEmitter())

	c := NewConsensusHandlers(
		ctx,
		tests.NewMockBlockReader(),
		indiciesDB,
		server,
		peers.NewPool(server),
		&clparams.NetworkConfig{},
		nil,
		&beaconCfg,
		ethClock,
		nil, &mock_services.ForkChoiceStorageMock{}, nil, columnStorage, nil, true,
	)
	c.Start()

	return ctx, server, client, &beaconCfg, indiciesDB, columnStorage
}

func expectDataColumnSidecarResourceUnavailable(t *testing.T, stream network.Stream, reqData []byte) {
	t.Helper()

	expectDataColumnSidecarResponsePrefix(t, stream, reqData, ResourceUnavailablePrefix)
}

func expectDataColumnSidecarResponsePrefix(t *testing.T, stream network.Stream, reqData []byte, prefix byte) {
	t.Helper()

	require.NoError(t, stream.SetDeadline(time.Now().Add(5*time.Second)))

	_, err := stream.Write(reqData)
	require.NoError(t, err)

	firstByte := make([]byte, 1)
	_, err = io.ReadFull(stream, firstByte)
	require.NoError(t, err)
	require.Equal(t, prefix, firstByte[0])
}

func expectDataColumnSidecarEmptyResponse(t *testing.T, stream network.Stream, reqData []byte) {
	t.Helper()

	require.NoError(t, stream.SetDeadline(time.Now().Add(5*time.Second)))

	_, err := stream.Write(reqData)
	require.NoError(t, err)

	require.NoError(t, stream.SetReadDeadline(time.Now().Add(200*time.Millisecond)))
	buf := make([]byte, 1)
	n, err := stream.Read(buf)
	require.Zero(t, n)
	require.Error(t, err)
}
