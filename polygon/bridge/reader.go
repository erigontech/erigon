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

package bridge

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/u256"
	"github.com/erigontech/erigon-lib/gointerfaces"
	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

type Reader struct {
	store              Store
	logger             log.Logger
	stateClientAddress common.Address
}

type ReaderConfig struct {
	Store                        Store
	Logger                       log.Logger
	StateReceiverContractAddress common.Address
	RoTxLimit                    int64
}

func AssembleReader(ctx context.Context, config ReaderConfig) (*Reader, error) {
	reader := NewReader(config.Store, config.Logger, config.StateReceiverContractAddress)

	err := reader.Prepare(ctx)
	if err != nil {
		return nil, err
	}

	return reader, nil
}

func NewReader(store Store, logger log.Logger, stateReceiverContractAddress common.Address) *Reader {
	return &Reader{
		store:              store,
		logger:             logger,
		stateClientAddress: stateReceiverContractAddress,
	}
}

func (r *Reader) Prepare(ctx context.Context) error {
	return r.store.Prepare(ctx)
}

func (r *Reader) EventsWithinTime(ctx context.Context, timeFrom, timeTo time.Time) ([]*types.Message, error) {
	events, ids, err := r.store.EventsByTimeframe(ctx, uint64(timeFrom.Unix()), uint64(timeTo.Unix()))
	if err != nil {
		return nil, err
	}

	eventsRaw := make([]*types.Message, 0, len(events))

	if len(events) > 0 && dbg.Enabled(ctx) {
		r.logger.Debug(
			bridgeLogPrefix("events for time range"),
			"timeFrom", timeFrom.Unix(),
			"timeTo", timeTo.Unix(),
			"start", ids[0],
			"end", ids[len(ids)-1],
			"len", len(events),
		)
	}

	// convert to message
	for _, event := range events {
		msg := types.NewMessage(
			state.SystemAddress,
			&r.stateClientAddress,
			0, u256.Num0,
			core.SysCallGasLimit,
			u256.Num0,
			nil, nil,
			event, nil, false,
			true,
			nil,
		)

		eventsRaw = append(eventsRaw, msg)
	}

	return eventsRaw, nil
}

// Events returns all sync events at blockNum
func (r *Reader) Events(ctx context.Context, blockHash common.Hash, blockNum uint64) ([]*types.Message, error) {
	events, err := r.store.EventsByBlock(ctx, blockHash, blockNum)
	if err != nil {
		return nil, err
	}

	if len(events) > 0 && dbg.Enabled(ctx) {
		r.logger.Debug(bridgeLogPrefix("events for block"), "block", blockNum, "len", len(events))
	}

	eventsRaw := make([]*types.Message, 0, len(events))

	// convert to message
	for _, event := range events {
		msg := types.NewMessage(
			state.SystemAddress,
			&r.stateClientAddress,
			0, u256.Num0,
			core.SysCallGasLimit,
			u256.Num0,
			nil, nil,
			event, nil, false,
			true,
			nil,
		)

		eventsRaw = append(eventsRaw, msg)
	}

	return eventsRaw, nil
}

func (r *Reader) EventTxnLookup(ctx context.Context, borTxHash common.Hash) (uint64, bool, error) {
	return r.store.EventTxnToBlockNum(ctx, borTxHash)
}

func (r *Reader) Close() {
	r.store.Close()
}

type RemoteReader struct {
	client  remote.BridgeBackendClient
	logger  log.Logger
	version gointerfaces.Version
}

func NewRemoteReader(client remote.BridgeBackendClient) *RemoteReader {
	return &RemoteReader{
		client:  client,
		logger:  log.New("remote_service", "bridge"),
		version: gointerfaces.VersionFromProto(APIVersion),
	}
}

func (r *RemoteReader) Events(ctx context.Context, blockHash common.Hash, blockNum uint64) ([]*types.Message, error) {
	reply, err := r.client.BorEvents(ctx, &remote.BorEventsRequest{
		BlockNum:  blockNum,
		BlockHash: gointerfaces.ConvertHashToH256(blockHash)})
	if err != nil {
		return nil, err
	}
	if reply == nil {
		return nil, nil
	}

	stateReceiverContractAddress := common.HexToAddress(reply.StateReceiverContractAddress)
	result := make([]*types.Message, len(reply.EventRlps))
	for i, event := range reply.EventRlps {
		result[i] = messageFromData(stateReceiverContractAddress, event)
	}

	return result, nil
}

func (r *RemoteReader) EventTxnLookup(ctx context.Context, borTxHash common.Hash) (uint64, bool, error) {
	reply, err := r.client.BorTxnLookup(ctx, &remote.BorTxnLookupRequest{BorTxHash: gointerfaces.ConvertHashToH256(borTxHash)})
	if err != nil {
		return 0, false, err
	}
	if reply == nil {
		return 0, false, nil
	}

	return reply.BlockNumber, reply.Present, nil
}

func (r *RemoteReader) Close() {
	// no-op as there is no attached store
}

func (r *RemoteReader) EnsureVersionCompatibility() bool {
	versionReply, err := r.client.Version(context.Background(), &emptypb.Empty{}, grpc.WaitForReady(true))
	if err != nil {
		r.logger.Error("getting Version", "err", err)
		return false
	}
	if !gointerfaces.EnsureVersion(r.version, versionReply) {
		r.logger.Error("incompatible interface versions", "client", r.version.String(),
			"server", fmt.Sprintf("%d.%d.%d", versionReply.Major, versionReply.Minor, versionReply.Patch))
		return false
	}
	r.logger.Info("interfaces compatible", "client", r.version.String(),
		"server", fmt.Sprintf("%d.%d.%d", versionReply.Major, versionReply.Minor, versionReply.Patch))
	return true
}

func messageFromData(to common.Address, data []byte) *types.Message {
	msg := types.NewMessage(
		state.SystemAddress,
		&to,
		0, u256.Num0,
		core.SysCallGasLimit,
		u256.Num0,
		nil, nil,
		data, nil, false,
		true,
		nil,
	)

	return msg
}

// NewStateSyncEventMessages creates a corresponding message that can be passed to EVM for multiple state sync events
func NewStateSyncEventMessages(stateSyncEvents []rlp.RawValue, stateReceiverContract *common.Address, gasLimit uint64) []*types.Message {
	msgs := make([]*types.Message, len(stateSyncEvents))
	for i, event := range stateSyncEvents {
		msg := types.NewMessage(
			state.SystemAddress, // from
			stateReceiverContract,
			0,         // nonce
			u256.Num0, // amount
			gasLimit,
			u256.Num0, // gasPrice
			nil,       // feeCap
			nil,       // tip
			event,
			nil,   // accessList
			false, // checkNonce
			true,  // isFree
			nil,   // maxFeePerBlobGas
		)

		msgs[i] = msg
	}

	return msgs
}
