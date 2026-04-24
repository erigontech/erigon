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

package engineapi

// testing_api.go implements the testing_ RPC namespace, specifically testing_buildBlockV1.
// Enable via --http.api=...,testing (e.g. --http.api eth,erigon,testing).
// This namespace MUST NOT be enabled on production networks.

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	execctx "github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/executionproto"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/txnprovider"
)

// directServerAccessor is implemented by direct.ExecutionClientDirect, which wraps the real
// ExecModule.  We use a two-step assertion (client → server → concrete method) to avoid
// importing execution/builder inside node/direct (which would introduce an import cycle).
type directServerAccessor interface {
	Server() executionproto.ExecutionServer
}

// TestingAPI is the interface for the testing_ RPC namespace.
type TestingAPI interface {
	// BuildBlockV1 synchronously builds and returns an execution payload given a parent hash,
	// payload attributes, an optional transaction list, and optional extra data.
	// Unlike the two-phase forkchoiceUpdated+getPayload flow this call blocks until the block
	// is fully assembled and returns the result in a single response.
	//
	// transactions: nil  → draw from mempool (normal builder behaviour)
	//               []   → build an empty block (mempool bypassed, no txs)
	//               [...] → build a block containing exactly these transactions (strict nonce check)
	BuildBlockV1(ctx context.Context, parentHash common.Hash, payloadAttributes *engine_types.PayloadAttributes, transactions *[]hexutil.Bytes, extraData *hexutil.Bytes) (*engine_types.GetPayloadResponse, error)
}

// blockParamAssembler is implemented by execmodule.ExecModule.  We reach it via the two-step
// assertion executionService → directServerAccessor.Server() → blockParamAssembler.
type blockParamAssembler interface {
	AssembleBlockWithParams(ctx context.Context, params *builder.Parameters) (payloadID uint64, busy bool, err error)
}

// resolveBlockParamAssembler extracts a blockParamAssembler from the execution service,
// using the directServerAccessor indirection to avoid circular imports.
func resolveBlockParamAssembler(svc executionproto.ExecutionClient) (blockParamAssembler, bool) {
	dsa, ok := svc.(directServerAccessor)
	if !ok {
		return nil, false
	}
	bpa, ok := dsa.Server().(blockParamAssembler)
	return bpa, ok
}

// testingImpl is the concrete implementation of TestingAPI.
type testingImpl struct {
	server *EngineServer
	logger log.Logger
	db     kv.TemporalRoDB
}

// NewTestingImpl returns a new TestingAPI implementation wrapping the given EngineServer.
func NewTestingImpl(server *EngineServer, logger log.Logger, db kv.TemporalRoDB) TestingAPI {
	return &testingImpl{server: server, logger: logger, db: db}
}

// NewTestingRPCEntry returns the rpc.API descriptor for the testing_ namespace.
func NewTestingRPCEntry(server *EngineServer, logger log.Logger, db kv.TemporalRoDB) rpc.API {
	return rpc.API{
		Namespace: "testing",
		Public:    false,
		Service:   TestingAPI(NewTestingImpl(server, logger, db)),
		Version:   "1.0",
	}
}

// decodeTxnProvider decodes raw transactions into a TxnProvider.
// Returns nil if transactions is nil (mempool path).
// Opens a single temporal DB transaction for all nonce lookups to avoid per-sender overhead.
func (t *testingImpl) decodeTxnProvider(ctx context.Context, transactions *[]hexutil.Bytes, blockNumber, timestamp uint64) (txnprovider.TxnProvider, error) {
	if transactions == nil {
		return nil, nil
	}

	var reader *state.ReaderV3
	if t.db != nil {
		dbTx, err := t.db.BeginTemporalRo(ctx)
		if err != nil {
			return nil, fmt.Errorf("testing_buildBlockV1: could not begin temporal transaction: %w", err)
		}
		defer dbTx.Rollback()
		sd, err := execctx.NewSharedDomains(ctx, dbTx, t.logger)
		if err != nil {
			return nil, fmt.Errorf("testing_buildBlockV1: NewSharedDomains error: %w", err)
		}
		defer sd.Close()
		reader = state.NewReaderV3(sd.AsGetter(dbTx))
	}

	decoded := make([]types.Transaction, 0, len(*transactions))
	signer := types.MakeSigner(t.server.config, blockNumber, timestamp)
	expectedNonce := make(map[accounts.Address]uint64, len(*transactions))
	for i, rawTx := range *transactions {
		tx, err := types.DecodeTransaction(rawTx)
		if err != nil {
			return nil, &rpc.InvalidParamsError{Message: fmt.Sprintf("transaction %d: decode error: %v", i, err)}
		}
		sender, err := signer.Sender(tx)
		if err != nil {
			return nil, &rpc.InvalidParamsError{Message: fmt.Sprintf("transaction %d: cannot recover sender: %v", i, err)}
		}
		tx.SetSender(sender)
		if _, seen := expectedNonce[sender]; !seen {
			var stateNonce uint64
			if reader != nil {
				acc, err := reader.ReadAccountData(accounts.InternAddress(sender.Value()))
				if err != nil {
					return nil, fmt.Errorf("testing_buildBlockV1: ReadAccountData error: %w", err)
				}
				if acc != nil {
					stateNonce = acc.Nonce
				}
			}
			expectedNonce[sender] = stateNonce
		}
		want := expectedNonce[sender]
		got := tx.GetNonce()
		if got > want {
			return nil, &rpc.CustomError{Code: rpc.ErrCodeDefault, Message: fmt.Sprintf("nonce too high: address %v, tx: %d state: %d", sender.Value(), got, want)}
		}
		if got < want {
			return nil, &rpc.CustomError{Code: rpc.ErrCodeDefault, Message: fmt.Sprintf("nonce too low: address %v, tx: %d state: %d", sender.Value(), got, want)}
		}
		expectedNonce[sender]++
		decoded = append(decoded, tx)
	}
	return &staticTxnProvider{txns: decoded}, nil
}

// BuildBlockV1 implements TestingAPI.
func (t *testingImpl) BuildBlockV1(
	ctx context.Context,
	parentHash common.Hash,
	payloadAttributes *engine_types.PayloadAttributes,
	transactions *[]hexutil.Bytes,
	extraData *hexutil.Bytes,
) (*engine_types.GetPayloadResponse, error) {
	if payloadAttributes == nil {
		return nil, &rpc.InvalidParamsError{Message: "payloadAttributes must not be null"}
	}

	// Validate parent block exists.
	parentHeader := t.server.chainRW.GetHeaderByHash(ctx, parentHash)
	if parentHeader == nil {
		return nil, &rpc.InvalidParamsError{Message: "unknown parent hash"}
	}

	timestamp := uint64(payloadAttributes.Timestamp)

	// Timestamp must be strictly greater than parent.
	if parentHeader.Time >= timestamp {
		return nil, &rpc.InvalidParamsError{Message: "payload timestamp must be greater than parent block timestamp"}
	}

	// Validate withdrawals presence.
	if err := t.server.checkWithdrawalsPresence(timestamp, payloadAttributes.Withdrawals); err != nil {
		return nil, err
	}

	// Determine version from timestamp for proper fork handling.
	version := clparams.BellatrixVersion
	switch {
	case t.server.config.IsAmsterdam(timestamp):
		version = clparams.GloasVersion
	case t.server.config.IsOsaka(timestamp):
		version = clparams.FuluVersion
	case t.server.config.IsPrague(timestamp):
		version = clparams.ElectraVersion
	case t.server.config.IsCancun(timestamp):
		version = clparams.DenebVersion
	case t.server.config.IsShanghai(timestamp):
		version = clparams.CapellaVersion
	}

	// Validate parentBeaconBlockRoot presence for Cancun+.
	if version >= clparams.DenebVersion && payloadAttributes.ParentBeaconBlockRoot == nil {
		return nil, &rpc.InvalidParamsError{Message: "parentBeaconBlockRoot required for Cancun and later forks"}
	}
	if version < clparams.DenebVersion && payloadAttributes.ParentBeaconBlockRoot != nil {
		return nil, &rpc.InvalidParamsError{Message: "parentBeaconBlockRoot not supported before Cancun"}
	}

	customProvider, err := t.decodeTxnProvider(ctx, transactions, parentHeader.Number.Uint64()+1, timestamp)
	if err != nil {
		return nil, err
	}

	// Both steps share a single slot-duration budget so the total wall-clock
	// time of BuildBlockV1 is bounded to one slot (e.g. 12 s), not two.
	// Each step acquires the lock independently, matching production behaviour
	// where ForkChoiceUpdated and GetPayload are separate RPC calls.
	deadline := time.Now().Add(time.Duration(t.server.config.SecondsPerSlot()) * time.Second)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}

	// Step 1: AssembleBlock.
	var payloadID uint64
	execBusy, err := func() (bool, error) {
		t.server.lock.Lock()
		defer t.server.lock.Unlock()

		if customProvider != nil {
			// Explicit tx list path: use AssembleBlockWithParams to pass CustomTxnProvider.
			bpa, ok := resolveBlockParamAssembler(t.server.executionService)
			if !ok {
				return false, errors.New("execution service does not support explicit transaction list")
			}
			params := &builder.Parameters{
				ParentHash:            parentHash,
				Timestamp:             timestamp,
				PrevRandao:            payloadAttributes.PrevRandao,
				SuggestedFeeRecipient: payloadAttributes.SuggestedFeeRecipient,
				SlotNumber:            (*uint64)(payloadAttributes.SlotNumber),
				CustomTxnProvider:     customProvider,
			}
			if version >= clparams.CapellaVersion {
				params.Withdrawals = payloadAttributes.Withdrawals
			}
			if version >= clparams.DenebVersion {
				params.ParentBeaconBlockRoot = payloadAttributes.ParentBeaconBlockRoot
			}
			var id uint64
			var busy bool
			var err error
			busy, err = waitForResponse(time.Until(deadline), func() (bool, error) {
				id, busy, err = bpa.AssembleBlockWithParams(ctx, params)
				return busy, err
			})
			payloadID = id
			return busy, err
		}

		// Mempool / empty tx path: use the proto-based AssembleBlock.
		req := &executionproto.AssembleBlockRequest{
			ParentHash:            gointerfaces.ConvertHashToH256(parentHash),
			Timestamp:             timestamp,
			PrevRandao:            gointerfaces.ConvertHashToH256(payloadAttributes.PrevRandao),
			SuggestedFeeRecipient: gointerfaces.ConvertAddressToH160(payloadAttributes.SuggestedFeeRecipient),
			SlotNumber:            (*uint64)(payloadAttributes.SlotNumber),
		}
		if version >= clparams.CapellaVersion {
			req.Withdrawals = engine_types.ConvertWithdrawalsToRpc(payloadAttributes.Withdrawals)
		}
		if version >= clparams.DenebVersion {
			req.ParentBeaconBlockRoot = gointerfaces.ConvertHashToH256(*payloadAttributes.ParentBeaconBlockRoot)
		}
		var resp *executionproto.AssembleBlockResponse
		busy, err := waitForResponse(time.Until(deadline), func() (bool, error) {
			resp, err = t.server.executionService.AssembleBlock(ctx, req)
			if err != nil {
				return false, err
			}
			return resp.Busy, nil
		})
		if resp != nil {
			payloadID = resp.Id
		}
		return busy, err
	}()
	if err != nil {
		return nil, err
	}
	if execBusy {
		return nil, errors.New("execution service is busy, cannot build block")
	}

	// Step 2: GetAssembledBlock (proto-based).
	getResp, execBusy, err := func() (*executionproto.GetAssembledBlockResponse, bool, error) {
		t.server.lock.Lock()
		defer t.server.lock.Unlock()

		var resp *executionproto.GetAssembledBlockResponse
		busy, err := waitForResponse(time.Until(deadline), func() (bool, error) {
			resp, err = t.server.executionService.GetAssembledBlock(ctx, &executionproto.GetAssembledBlockRequest{
				Id: payloadID,
			})
			if err != nil {
				return false, err
			}
			return resp.Busy, nil
		})
		return resp, busy, err
	}()
	if err != nil {
		return nil, err
	}
	if execBusy {
		return nil, errors.New("execution service is busy retrieving assembled block")
	}
	if getResp.Data == nil {
		return nil, errors.New("no assembled block data available for payload ID")
	}

	data := getResp.Data

	// Build execution requests for Prague+.
	var executionRequests []hexutil.Bytes
	if version >= clparams.ElectraVersion {
		executionRequests = make([]hexutil.Bytes, 0)
		if data.Requests != nil {
			for _, r := range data.Requests.Requests {
				executionRequests = append(executionRequests, r)
			}
		}
	}

	response := &engine_types.GetPayloadResponse{
		ExecutionPayload:      engine_types.ConvertPayloadFromRpc(data.ExecutionPayload),
		BlockValue:            (*hexutil.Big)(gointerfaces.ConvertH256ToUint256Int(data.BlockValue).ToBig()),
		BlobsBundle:           engine_types.ConvertBlobsFromRpc(data.BlobsBundle),
		ExecutionRequests:     executionRequests,
		ShouldOverrideBuilder: false,
	}

	if extraData != nil {
		response.ExecutionPayload.ExtraData = *extraData
	}

	return response, nil
}

// staticTxnProvider is a TxnProvider that yields a fixed transaction list exactly once,
// then returns nil on every subsequent call. Used only by the testing_ namespace.
type staticTxnProvider struct {
	txns []types.Transaction
	done atomic.Bool
}

func (s *staticTxnProvider) ProvideTxns(_ context.Context, _ ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	if !s.done.CompareAndSwap(false, true) {
		return nil, nil
	}
	txns := s.txns
	s.txns = nil // release for GC after handing off
	return txns, nil
}
