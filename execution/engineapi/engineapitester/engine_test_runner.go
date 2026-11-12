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
	"errors"
	"fmt"
	"time"

	"github.com/erigontech/erigon/common"
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
	// SyncingTimeout is how long a call may keep answering SYNCING before the
	// fixture fails.
	SyncingTimeout time.Duration
	payloads       []EngineXTestNewPayload
}

// UnmarshalJSON implements json.Unmarshaler interface.
func (et *EngineTest) UnmarshalJSON(in []byte) error {
	if err := et.BlockTest.UnmarshalJSON(in); err != nil {
		return err
	}
	var fixture struct {
		Payloads *[]EngineXTestNewPayload `json:"engineNewPayloads"`
	}
	if err := json.Unmarshal(in, &fixture); err != nil {
		return err
	}
	if fixture.Payloads == nil {
		return errors.New("fixture has no engineNewPayloads")
	}
	et.payloads = *fixture.Payloads
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
	// Calls go through the RPC handler, which decodes and counts the params as
	// it does for a CL.
	rpcSrv := rpc.NewServer(1, false, false, false, m.Log, 0)
	defer rpcSrv.Stop()
	if err := rpcSrv.RegisterAPI(srv.API()); err != nil {
		return err
	}
	client := rpc.DialInProc(rpcSrv, m.Log)
	defer client.Close()
	if len(et.payloads) > 0 {
		if err := forkchoiceUpdated(m.Ctx, client, m.Genesis.Hash(), et.payloads[0].FcuVersion, et.SyncingTimeout); err != nil {
			return fmt.Errorf("forkchoice update to genesis: %w", err)
		}
	}
	chain := chainreader.NewChainReaderEth1(m.ChainConfig, m.ExecModule, 0)
	for i, payload := range et.payloads {
		if err := et.sendPayload(m.Ctx, client, chain, i, payload); err != nil {
			return fmt.Errorf("payload %d: %w", i, err)
		}
	}
	m.ExecModule.WaitIdle(m.Ctx)
	return nil
}

// sendPayload calls engine_newPayloadV<n> with the fixture's payload i and,
// for a valid payload, engine_forkchoiceUpdatedV<n>.
func (et *EngineTest) sendPayload(ctx context.Context, client *rpc.Client, chain chainreader.ChainReaderWriterEth1, i int, p EngineXTestNewPayload) error {
	var payload struct {
		BlockHash  common.Hash `json:"blockHash"`
		ParentHash common.Hash `json:"parentHash"`
	}
	var hash *common.Hash
	if len(p.Params) > 0 && json.Unmarshal(p.Params[0], &payload) == nil {
		hash = &payload.BlockHash
	}
	params := make([]any, len(p.Params))
	for i, param := range p.Params {
		params[i] = param
	}

	parentUnknown := func() bool { return chain.GetHeaderByHash(ctx, payload.ParentHash) == nil }
	status, err := retrySyncing(ctx, et.SyncingTimeout, parentUnknown, func() (*enginetypes.PayloadStatus, error) {
		var status enginetypes.PayloadStatus
		if err := client.CallContext(ctx, &status, "engine_newPayloadV"+p.NewPayloadVersion, params...); err != nil {
			return nil, err
		}
		return &status, nil
	})
	if errors.Is(err, errStillSyncing) {
		return err
	}
	var rpcErr rpc.Error
	isRPCErr := errors.As(err, &rpcErr)
	switch {
	case isRPCErr:
		et.Reject(i, hash, rpcErrorString(rpcErr))
	case err == nil && status.Status == enginetypes.InvalidStatus:
		et.Reject(i, hash, statusErrorString(status))
	}
	expectInvalid := p.ValidationError != "" || p.ErrorCode != ""
	switch {
	case isRPCErr && expectInvalid:
		return nil
	case err != nil:
		return err
	case status.Status == enginetypes.InvalidStatus && expectInvalid:
		return nil
	case status.Status != enginetypes.ValidStatus:
		return statusError("payload", status)
	case expectInvalid:
		return fmt.Errorf("payload is valid, expected %s%s", p.ValidationError, p.ErrorCode)
	}
	return forkchoiceUpdated(ctx, client, payload.BlockHash, p.FcuVersion, et.SyncingTimeout)
}

func forkchoiceUpdated(ctx context.Context, client *rpc.Client, head common.Hash, version string, timeout time.Duration) error {
	state := &enginetypes.ForkChoiceState{HeadHash: head}
	status, err := retrySyncing(ctx, timeout, nil, func() (*enginetypes.PayloadStatus, error) {
		var r enginetypes.ForkChoiceUpdatedResponse
		if err := client.CallContext(ctx, &r, "engine_forkchoiceUpdatedV"+version, state); err != nil {
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

// rpcErrorString renders err as a JSON-RPC client sees it: code, message and
// any data.
func rpcErrorString(err rpc.Error) string {
	s := fmt.Sprintf("%d: %s", err.ErrorCode(), err.Error())
	var dataErr rpc.DataError
	if !errors.As(err, &dataErr) {
		return s
	}
	switch data := dataErr.ErrorData().(type) {
	case nil:
	case string:
		s += ": " + data
	default:
		// The data was decoded from JSON, so it encodes again.
		if encoded, err := json.Marshal(data); err == nil {
			s += ": " + string(encoded)
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

var errStillSyncing = errors.New("still SYNCING")

// retrySyncing repeats call while the server answers SYNCING because it is
// still busy with the previous request, or until stuck reports true. A server
// still answering SYNCING after timeout fails with errStillSyncing.
func retrySyncing(ctx context.Context, timeout time.Duration, stuck func() bool, call func() (*enginetypes.PayloadStatus, error)) (*enginetypes.PayloadStatus, error) {
	deadline := time.After(timeout)
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
		case <-deadline:
			return nil, fmt.Errorf("%w after %s", errStillSyncing, timeout)
		case <-time.After(50 * time.Millisecond):
		}
	}
}
