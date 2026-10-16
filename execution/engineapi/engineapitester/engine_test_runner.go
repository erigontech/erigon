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

package engineapitester

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/engineapi"
	"github.com/erigontech/erigon/execution/engineapi/engine_block_downloader"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/execution/tests/testutil"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/rpc"
)

// An EngineTest is a blockchain_tests_engine fixture, a BlockTest whose chain
// is delivered through the engine API.
type EngineTest struct {
	testutil.BlockTest
	payloads []EngineXTestNewPayload
}

// UnmarshalJSON implements json.Unmarshaler interface.
func (et *EngineTest) UnmarshalJSON(in []byte) error {
	if err := et.BlockTest.UnmarshalJSON(in); err != nil {
		return err
	}
	var fixture struct {
		Payloads []EngineXTestNewPayload `json:"engineNewPayloads"`
	}
	if err := json.Unmarshal(in, &fixture); err != nil {
		return err
	}
	et.payloads = fixture.Payloads
	return nil
}

// RunCLI calls an engine server's handlers in process, at the versions the
// fixture names, on a fresh execution module.
func (et *EngineTest) RunCLI() error {
	et.Rejections = []testutil.Rejection{}
	return et.RunCLIWith(et.deliver)
}

func (et *EngineTest) deliver(m *execmoduletester.ExecModuleTester) error {
	// With no peers, a payload whose parent is unknown is answered SYNCING.
	noPeers := p2p.NewBackwardBlockDownloader(m.Log, nil, p2p.NewPeerTracker(m.Log, nil), m.Dirs.Tmp)
	srv := engineapi.NewEngineServer(
		m.Log,
		m.ChainConfig,
		m.ExecModule,
		engine_block_downloader.NewEngineBlockDownloader(m.Ctx, m.Log, m.ExecModule, m.BlockReader, m.DB, m.ChainConfig, ethconfig.Defaults.Sync, noPeers),
		false, /* caplin */
		false, /* internalCL */
		false, /* proposing */
		true,  /* consuming */
		nil,   /* txPool */
		nil,   /* blobGetter */
		ethconfig.Defaults.FcuTimeout,
		ethconfig.Defaults.MaxReorgDepth,
	)
	if len(et.payloads) > 0 {
		if err := forkchoiceUpdated(m.Ctx, srv, m.Genesis.Hash(), et.payloads[0].FcuVersion); err != nil {
			return fmt.Errorf("forkchoice update to genesis: %w", err)
		}
	}
	chain := chainreader.NewChainReaderEth1(m.ChainConfig, m.ExecModule, 0)
	for i, payload := range et.payloads {
		reject := func(hash *common.Hash, err string) {
			et.Rejections = append(et.Rejections, testutil.Rejection{Index: i, Hash: hash, Error: err})
		}
		if err := sendPayload(m.Ctx, srv, chain, payload, reject); err != nil {
			return fmt.Errorf("payload %d: %w", i, err)
		}
	}
	m.ExecModule.WaitIdle(m.Ctx)
	return nil
}

// sendPayload calls engine_newPayloadV<n> and, for a valid payload,
// engine_forkchoiceUpdatedV<n>.
func sendPayload(ctx context.Context, srv *engineapi.EngineServer, chain chainreader.ChainReaderWriterEth1, p EngineXTestNewPayload, reject func(hash *common.Hash, err string)) error {
	var (
		payload           enginetypes.ExecutionPayload
		blobHashes        []common.Hash
		parentBeaconRoot  common.Hash
		executionRequests []hexutil.Bytes
	)
	targets := []any{&payload, &blobHashes, &parentBeaconRoot, &executionRequests}
	for i, param := range p.Params {
		if i >= len(targets) {
			return fmt.Errorf("unexpected param %d", i)
		}
		if err := json.Unmarshal(param, targets[i]); err != nil {
			reject(nil, err.Error())
			return fmt.Errorf("decode param %d: %w", i, err)
		}
	}

	parentUnknown := func() bool { return chain.GetHeaderByHash(ctx, payload.ParentHash) == nil }
	status, err := retrySyncing(ctx, parentUnknown, func() (*enginetypes.PayloadStatus, error) {
		switch p.NewPayloadVersion {
		case "1":
			return srv.NewPayloadV1(ctx, &payload)
		case "2":
			return srv.NewPayloadV2(ctx, &payload)
		case "3":
			return srv.NewPayloadV3(ctx, &payload, blobHashes, &parentBeaconRoot)
		case "4":
			return srv.NewPayloadV4(ctx, &payload, blobHashes, &parentBeaconRoot, executionRequests)
		case "5":
			return srv.NewPayloadV5(ctx, &payload, blobHashes, &parentBeaconRoot, executionRequests)
		default:
			return nil, fmt.Errorf("unsupported newPayload version %q", p.NewPayloadVersion)
		}
	})
	switch {
	case err != nil:
		reject(&payload.BlockHash, rpcErrorString(err))
	case status.Status != enginetypes.ValidStatus:
		reject(&payload.BlockHash, statusErrorString(status))
	}
	expectInvalid := p.ValidationError != "" || p.ErrorCode != ""
	switch {
	case err != nil && expectInvalid:
		return nil
	case err != nil:
		return err
	case status.Status != enginetypes.ValidStatus && expectInvalid:
		return nil
	case status.Status != enginetypes.ValidStatus:
		return statusError("payload", status)
	case expectInvalid:
		return fmt.Errorf("payload is valid, expected %s%s", p.ValidationError, p.ErrorCode)
	}
	return forkchoiceUpdated(ctx, srv, payload.BlockHash, p.FcuVersion)
}

func forkchoiceUpdated(ctx context.Context, srv *engineapi.EngineServer, head common.Hash, version string) error {
	state := &enginetypes.ForkChoiceState{HeadHash: head}
	status, err := retrySyncing(ctx, nil, func() (*enginetypes.PayloadStatus, error) {
		var (
			r   *enginetypes.ForkChoiceUpdatedResponse
			err error
		)
		switch version {
		case "1":
			r, err = srv.ForkchoiceUpdatedV1(ctx, state, nil)
		case "2":
			r, err = srv.ForkchoiceUpdatedV2(ctx, state, nil)
		case "3":
			r, err = srv.ForkchoiceUpdatedV3(ctx, state, nil)
		case "4":
			r, err = srv.ForkchoiceUpdatedV4(ctx, state, nil, nil)
		default:
			return nil, fmt.Errorf("unsupported forkchoiceUpdated version %q", version)
		}
		if err != nil {
			return nil, err
		}
		return r.PayloadStatus, nil
	})
	if err != nil {
		return err
	}
	if status.Status != enginetypes.ValidStatus {
		return statusError("forkchoice", status)
	}
	return nil
}

// rpcErrorString renders err as a JSON-RPC client would see it: code, message
// and any data.
func rpcErrorString(err error) string {
	jsonErr := rpc.NewJsonErrorFromErr(err).(interface {
		rpc.Error
		rpc.DataError
	})
	s := fmt.Sprintf("%d: %s", jsonErr.ErrorCode(), err.Error())
	switch data := jsonErr.ErrorData().(type) {
	case nil:
	case string:
		s += ": " + data
	default:
		if encoded, jsonErr := json.Marshal(data); jsonErr == nil {
			s += ": " + string(encoded)
		} else {
			s += fmt.Sprintf(": %v", data)
		}
	}
	return s
}

func statusErrorString(status *enginetypes.PayloadStatus) string {
	if status.ValidationError == nil || status.ValidationError.Error() == nil {
		return string(status.Status)
	}
	return status.ValidationError.Error().Error()
}

func statusError(call string, status *enginetypes.PayloadStatus) error {
	if status.ValidationError == nil || status.ValidationError.Error() == nil {
		return fmt.Errorf("%s status %s", call, status.Status)
	}
	return fmt.Errorf("%s status %s: %w", call, status.Status, status.ValidationError.Error())
}

// retrySyncing repeats call while the server answers SYNCING because it is
// still busy with the previous request, or until stuck reports true.
func retrySyncing(ctx context.Context, stuck func() bool, call func() (*enginetypes.PayloadStatus, error)) (*enginetypes.PayloadStatus, error) {
	for {
		status, err := call()
		if err != nil || status.Status != enginetypes.SyncingStatus {
			return status, err
		}
		if stuck != nil && stuck() {
			return status, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}
