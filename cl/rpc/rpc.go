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

package rpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/sentinel/communication"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/utils/eth_clock"

	"github.com/c2h5oh/datasize"
	"github.com/golang/snappy"
	"go.uber.org/zap/buffer"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces"
	sentinel "github.com/erigontech/erigon-lib/gointerfaces/sentinelproto"
	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/utils"
)

const maxMessageLength = 18 * datasize.MB

// BeaconRpcP2P represents a beacon chain RPC client.
type BeaconRpcP2P struct {
	// ctx is the context for the RPC client.
	ctx context.Context
	// sentinel is a client for sending and receiving messages to and from a beacon chain node.
	sentinel sentinel.SentinelClient
	// beaconConfig is the configuration for the beacon chain.
	beaconConfig *clparams.BeaconChainConfig
	// ethClock handles all time-related operations.
	ethClock eth_clock.EthereumClock

	columnDataPeers *columnDataPeers
}

// NewBeaconRpcP2P creates a new BeaconRpcP2P struct and returns a pointer to it.
// It takes a context, a sentinel.Sent
func NewBeaconRpcP2P(ctx context.Context, sentinel sentinel.SentinelClient, beaconConfig *clparams.BeaconChainConfig, ethClock eth_clock.EthereumClock, beaconState *state.CachingBeaconState) *BeaconRpcP2P {
	rpc := &BeaconRpcP2P{
		ctx:          ctx,
		sentinel:     sentinel,
		beaconConfig: beaconConfig,
		ethClock:     ethClock,
	}
	rpc.columnDataPeers = newColumnPeers(
		sentinel,
		beaconConfig,
		ethClock,
		beaconState,
	)
	return rpc
}

func (b *BeaconRpcP2P) sendBlocksRequest(ctx context.Context, topic string, reqData []byte) ([]*cltypes.SignedBeaconBlock, string, error) {
	responses, pid, err := b.sendRequest(ctx, topic, reqData)
	if err != nil {
		return nil, pid, err
	}

	responsePacket := []*cltypes.SignedBeaconBlock{}
	for _, data := range responses {
		responseChunk := cltypes.NewSignedBeaconBlock(b.beaconConfig, data.version)
		if err := responseChunk.DecodeSSZ(data.raw, int(data.version)); err != nil {
			return nil, pid, err
		}
		responsePacket = append(responsePacket, responseChunk)
	}

	return responsePacket, pid, nil
}

func (b *BeaconRpcP2P) sendBlobsSidecar(ctx context.Context, topic string, reqData []byte, count uint64) ([]*cltypes.BlobSidecar, string, error) {
	responses, pid, err := b.sendRequest(ctx, topic, reqData)
	if err != nil {
		return nil, pid, err
	}

	responsePacket := []*cltypes.BlobSidecar{}
	for _, data := range responses {
		responseChunk := &cltypes.BlobSidecar{}
		if err := responseChunk.DecodeSSZ(data.raw, int(data.version)); err != nil {
			return nil, pid, err
		}
		responsePacket = append(responsePacket, responseChunk)
	}

	return responsePacket, pid, nil
}

func (b *BeaconRpcP2P) SendColumnSidecarsByRootIdentifierReq(
	ctx context.Context,
	req *solid.ListSSZ[*cltypes.DataColumnsByRootIdentifier],
) ([]*cltypes.DataColumnSidecar, string, error) {
	filteredReq, pid, _, err := b.columnDataPeers.pickPeerRoundRobin(ctx, req)
	if err != nil {
		return nil, pid, err
	}

	var buffer buffer.Buffer
	if err := ssz_snappy.EncodeAndWrite(&buffer, filteredReq); err != nil {
		return nil, "", err
	}

	data := common.CopyBytes(buffer.Bytes())
	responsePacket, pid, err := b.sendRequestWithPeer(ctx, communication.DataColumnSidecarsByRootProtocolV1, data, pid)
	if err != nil {
		return nil, pid, err
	}

	ColumnSidecars := []*cltypes.DataColumnSidecar{}
	for _, data := range responsePacket {
		columnSidecar := &cltypes.DataColumnSidecar{}
		if err := columnSidecar.DecodeSSZ(data.raw, int(data.version)); err != nil {
			return nil, pid, err
		}
		ColumnSidecars = append(ColumnSidecars, columnSidecar)
	}

	return ColumnSidecars, pid, nil
}

func (b *BeaconRpcP2P) SendColumnSidecarsByRangeReqV1(
	ctx context.Context,
	start, count uint64,
	columns []uint64,
) ([]*cltypes.DataColumnSidecar, string, error) {
	req := &cltypes.ColumnSidecarsByRangeRequest{
		StartSlot: start,
		Count:     count,
		Columns:   solid.NewUint64ListSSZ(int(b.beaconConfig.NumberOfColumns)),
	}
	for _, column := range columns {
		req.Columns.Append(column)
	}
	var buffer buffer.Buffer
	if err := ssz_snappy.EncodeAndWrite(&buffer, req); err != nil {
		return nil, "", err
	}

	responsePacket, pid, err := b.sendRequest(ctx, communication.DataColumnSidecarsByRangeProtocolV1, buffer.Bytes())
	if err != nil {
		return nil, pid, err
	}

	ColumnSidecars := []*cltypes.DataColumnSidecar{}
	for _, data := range responsePacket {
		columnSidecar := &cltypes.DataColumnSidecar{}
		if err := columnSidecar.DecodeSSZ(data.raw, int(data.version)); err != nil {
			return nil, pid, err
		}
		ColumnSidecars = append(ColumnSidecars, columnSidecar)
	}
	return ColumnSidecars, pid, nil
}

// SendBeaconBlocksByRangeReq retrieves blocks range from beacon chain.
func (b *BeaconRpcP2P) SendBlobsSidecarByIdentifierReq(ctx context.Context, req *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error) {
	var buffer buffer.Buffer
	if err := ssz_snappy.EncodeAndWrite(&buffer, req); err != nil {
		return nil, "", err
	}

	data := common.CopyBytes(buffer.Bytes())
	blobs, pid, err := b.sendBlobsSidecar(ctx, communication.BlobSidecarByRootProtocolV1, data, uint64(req.Len()))
	if err != nil {
		if strings.Contains(err.Error(), "invalid request") {
			b.BanPeer(pid)
		}
		return nil, pid, err
	}
	return blobs, pid, nil
}

// SendBeaconBlocksByRangeReq retrieves blocks range from beacon chain.
func (b *BeaconRpcP2P) SendBlobsSidecarByRangerReq(ctx context.Context, start, count uint64) ([]*cltypes.BlobSidecar, string, error) {
	var buffer buffer.Buffer
	if err := ssz_snappy.EncodeAndWrite(&buffer, &cltypes.BlobsByRangeRequest{
		StartSlot: start,
		Count:     count,
	}); err != nil {
		return nil, "", err
	}

	data := common.CopyBytes(buffer.Bytes())
	return b.sendBlobsSidecar(ctx, communication.BlobSidecarByRangeProtocolV1, data, count*b.beaconConfig.MaxBlobsPerBlock)
}

// SendBeaconBlocksByRangeReq retrieves blocks range from beacon chain.
func (b *BeaconRpcP2P) SendBeaconBlocksByRangeReq(ctx context.Context, start, count uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
	req := &cltypes.BeaconBlocksByRangeRequest{
		StartSlot: start,
		Count:     count,
		Step:      1, // deprecated, and must be set to 1.
	}
	var buffer buffer.Buffer
	if err := ssz_snappy.EncodeAndWrite(&buffer, req); err != nil {
		return nil, "", err
	}

	data := common.CopyBytes(buffer.Bytes())
	return b.sendBlocksRequest(ctx, communication.BeaconBlocksByRangeProtocolV2, data)
}

// SendBeaconBlocksByRootReq retrieves blocks by root from beacon chain.
func (b *BeaconRpcP2P) SendBeaconBlocksByRootReq(ctx context.Context, roots [][32]byte) ([]*cltypes.SignedBeaconBlock, string, error) {
	var req solid.HashListSSZ = solid.NewHashList(69696969) // The number is used for hashing, it is innofensive here.
	for _, root := range roots {
		req.Append(root)
	}
	var buffer buffer.Buffer
	if err := ssz_snappy.EncodeAndWrite(&buffer, req); err != nil {
		return nil, "", err
	}
	data := common.CopyBytes(buffer.Bytes())
	return b.sendBlocksRequest(ctx, communication.BeaconBlocksByRootProtocolV2, data)
}

// Peers retrieves peer count.
func (b *BeaconRpcP2P) Peers() (uint64, error) {
	amount, err := b.sentinel.GetPeers(b.ctx, &sentinel.EmptyMessage{})
	if err != nil {
		return 0, err
	}
	return amount.Active, nil
}

func (b *BeaconRpcP2P) SetStatus(finalizedRoot common.Hash, finalizedEpoch uint64, headRoot common.Hash, headSlot uint64) error {
	forkDigest, err := b.ethClock.CurrentForkDigest()
	if err != nil {
		return err
	}
	_, err = b.sentinel.SetStatus(b.ctx, &sentinel.Status{
		ForkDigest:     utils.Bytes4ToUint32(forkDigest),
		FinalizedRoot:  gointerfaces.ConvertHashToH256(finalizedRoot),
		FinalizedEpoch: finalizedEpoch,
		HeadRoot:       gointerfaces.ConvertHashToH256(headRoot),
		HeadSlot:       headSlot,
	})
	return err
}

func (b *BeaconRpcP2P) BanPeer(pid string) {
	b.sentinel.BanPeer(b.ctx, &sentinel.Peer{Pid: pid})
}

// responseData is a helper struct to store the version and the raw data of the response for each data container.
type responseData struct {
	version clparams.StateVersion
	raw     []byte
}

// parseResponseData parses the response data from a sentinel message and returns the parsed response data.
func (b *BeaconRpcP2P) parseResponseData(message *sentinel.ResponseData) ([]responseData, string, error) {
	if message.Error {
		rd := snappy.NewReader(bytes.NewBuffer(message.Data))
		errBytes, _ := io.ReadAll(rd)
		log.Trace("received range req error", "err", string(errBytes), "raw", string(message.Data))
		return nil, message.Peer.Pid, nil
	}

	responsePacket := []responseData{}
	r := bytes.NewReader(message.Data)
	for {
		forkDigest := make([]byte, 4)
		if n, err := r.Read(forkDigest); err != nil {
			if err == io.EOF {
				break
			}
			return nil, message.Peer.Pid, err
		} else if n == 0 {
			break
		}

		// Read varint for length of message.
		encodedLn, _, err := ssz_snappy.ReadUvarint(r)
		if err != nil {
			return nil, message.Peer.Pid, fmt.Errorf("sendRequest failed. Unable to read varint from message prefix: %w", err)
		}
		// Sanity check for message size.
		if encodedLn > uint64(maxMessageLength) {
			return nil, message.Peer.Pid, errors.New("received message too big")
		}

		// Read bytes using snappy into a new raw buffer of side encodedLn.
		raw := make([]byte, encodedLn)
		sr := snappy.NewReader(r)
		bytesRead := 0
		for bytesRead < int(encodedLn) {
			n, err := sr.Read(raw[bytesRead:])
			if err != nil {
				return nil, message.Peer.Pid, fmt.Errorf("read error: %w", err)
			}
			bytesRead += n
		}
		// Fork digests
		respForkDigest := binary.BigEndian.Uint32(forkDigest)
		if respForkDigest == 0 {
			return nil, message.Peer.Pid, errors.New("null fork digest")
		}

		version, err := b.ethClock.StateVersionByForkDigest(utils.Uint32ToBytes4(respForkDigest))
		if err != nil {
			return nil, message.Peer.Pid, err
		}
		responsePacket = append(responsePacket, responseData{
			version: version,
			raw:     raw,
		})

		// read next result byte
		if _, err := r.ReadByte(); err == io.EOF {
			break
		} else if err != nil {
			log.Debug("failed to read byte", "err", err)
			return nil, message.Peer.Pid, err
		}
	}
	return responsePacket, message.Peer.Pid, nil
}

// sendRequest sends a request to the sentinel and helps with decoding the response.
func (b *BeaconRpcP2P) sendRequest(
	ctx context.Context,
	topic string,
	reqPayload []byte,
) ([]responseData, string, error) {
	ctx, cn := context.WithTimeout(ctx, time.Second*2)
	defer cn()
	message, err := b.sentinel.SendRequest(ctx, &sentinel.RequestData{
		Data:  reqPayload,
		Topic: topic,
	})
	if err != nil {
		return nil, "", err
	}
	return b.parseResponseData(message)
}

func (b *BeaconRpcP2P) sendRequestWithPeer(
	ctx context.Context,
	topic string,
	reqPayload []byte,
	peerId string,
) ([]responseData, string, error) {
	ctx, cn := context.WithTimeout(ctx, time.Second*2)
	defer cn()
	message, err := b.sentinel.SendPeerRequest(ctx, &sentinel.RequestDataWithPeer{
		Pid:   peerId,
		Data:  reqPayload,
		Topic: topic,
	})
	if err != nil {
		return nil, "", err
	}
	return b.parseResponseData(message)
}
