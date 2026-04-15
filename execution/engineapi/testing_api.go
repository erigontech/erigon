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
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/txnprovider"
)

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

// accountNonceGetter is a narrow interface for looking up the current state nonce of an address.
// It is intentionally separate from ExecutionModule to avoid forcing every stub/mock to carry
// a method that is only used by the testing_ namespace.
type accountNonceGetter interface {
	GetAccountNonce(ctx context.Context, address common.Address) (uint64, error)
}

// testingImpl is the concrete implementation of TestingAPI.
type testingImpl struct {
	server      *EngineServer
	nonceGetter accountNonceGetter // nil if executionService does not implement accountNonceGetter
}

// NewTestingImpl returns a new TestingAPI implementation wrapping the given EngineServer.
func NewTestingImpl(server *EngineServer) TestingAPI {
	ng, _ := server.executionService.(accountNonceGetter)
	return &testingImpl{server: server, nonceGetter: ng}
}

// NewTestingRPCEntry returns the rpc.API descriptor for the testing_ namespace.
func NewTestingRPCEntry(server *EngineServer) rpc.API {
	return rpc.API{
		Namespace: "testing",
		Public:    false,
		Service:   TestingAPI(NewTestingImpl(server)),
		Version:   "1.0",
	}
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

	var customProvider txnprovider.TxnProvider
	if transactions != nil {
		decoded := make([]types.Transaction, 0, len(*transactions))
		if len(*transactions) > 0 {
			signer := types.MakeSigner(t.server.config, parentHeader.Number.Uint64()+1, timestamp)
			// Track expected next nonce per sender to support sequential multi-tx lists.
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
					if t.nonceGetter != nil {
						stateNonce, err = t.nonceGetter.GetAccountNonce(ctx, sender.Value())
						if err != nil {
							return nil, err
						}
					}
					expectedNonce[sender] = stateNonce
				}
				want := expectedNonce[sender]
				got := tx.GetNonce()
				if got > want {
					return nil, &rpc.CustomError{
						Code:    rpc.ErrCodeDefault,
						Message: fmt.Sprintf("nonce too high: address %v, tx: %d state: %d", sender.Value(), got, want),
					}
				}
				if got < want {
					return nil, &rpc.CustomError{
						Code:    rpc.ErrCodeDefault,
						Message: fmt.Sprintf("nonce too low: address %v, tx: %d state: %d", sender.Value(), got, want),
					}
				}
				expectedNonce[sender]++
				decoded = append(decoded, tx)
			}
		}
		customProvider = &staticTxnProvider{txns: decoded}
	}

	// Build the AssembleBlock parameters (mirrors forkchoiceUpdated logic).
	assembleParams := &builder.Parameters{
		ParentHash:            parentHash,
		Timestamp:             timestamp,
		PrevRandao:            payloadAttributes.PrevRandao,
		SuggestedFeeRecipient: payloadAttributes.SuggestedFeeRecipient,
		SlotNumber:            (*uint64)(payloadAttributes.SlotNumber),
		CustomTxnProvider:        customProvider,
	}
	if version >= clparams.CapellaVersion {
		assembleParams.Withdrawals = payloadAttributes.Withdrawals
	}
	if version >= clparams.DenebVersion {
		assembleParams.ParentBeaconBlockRoot = payloadAttributes.ParentBeaconBlockRoot
	}

	// Both steps share a single slot-duration budget so the total wall-clock
	// time of BuildBlockV1 is bounded to one slot (e.g. 12 s), not two.
	// Each step acquires the lock independently, matching production behaviour
	// where ForkChoiceUpdated and GetPayload are separate RPC calls.
	deadline := time.Now().Add(time.Duration(t.server.config.SecondsPerSlot()) * time.Second)

	var payloadID uint64
	execBusy, err := func() (bool, error) {
		t.server.lock.Lock()
		defer t.server.lock.Unlock()

		var assembled execmodule.AssembleBlockResult
		var err error
		busy, err := waitForResponse(time.Until(deadline), func() (bool, error) {
			assembled, err = t.server.executionService.AssembleBlock(ctx, assembleParams)
			if err != nil {
				return false, err
			}
			return assembled.Busy, nil
		})
		payloadID = assembled.PayloadID
		return busy, err
	}()
	if err != nil {
		return nil, err
	}
	if execBusy {
		return nil, errors.New("execution service is busy, cannot build block")
	}

	var assembled execmodule.AssembledBlockResult
	execBusy, err = func() (bool, error) {
		t.server.lock.Lock()
		defer t.server.lock.Unlock()

		var err error
		busy, err := waitForResponse(time.Until(deadline), func() (bool, error) {
			assembled, err = t.server.executionService.GetAssembledBlock(ctx, payloadID)
			if err != nil {
				return false, err
			}
			return assembled.Busy, nil
		})
		return busy, err
	}()
	if err != nil {
		return nil, err
	}
	if execBusy {
		return nil, errors.New("execution service is busy retrieving assembled block")
	}
	if assembled.Block == nil {
		return nil, errors.New("no assembled block data available for payload ID")
	}

	response, err := assembledBlockToPayloadResponse(assembled.Block, assembled.BlockValue, version)
	if err != nil {
		return nil, err
	}
	response.ShouldOverrideBuilder = false
	// Return blockValue=0, matching Geth's BuildTestingPayload behaviour.
	response.BlockValue = new(hexutil.Big)
	// Strip BAL (EIP-7928, Amsterdam+) and/or override extraData, recomputing blockHash.
	// BAL is an Erigon-specific field unknown to Geth: if present it is excluded from the
	// payload so the blockHash matches what a Geth-compatible client would compute.
	// The nil-check is implicitly fork-gated: the builder only populates BlockAccessList
	// for Amsterdam+ blocks, so it is nil for all earlier forks.
	if response.ExecutionPayload.BlockAccessList != nil || extraData != nil {
		h := types.CopyHeader(assembled.Block.Block.Header())
		h.BlockAccessListHash = nil
		if extraData != nil {
			h.Extra = *extraData
			response.ExecutionPayload.ExtraData = *extraData
		}
		response.ExecutionPayload.BlockHash = h.Hash()
		response.ExecutionPayload.BlockAccessList = nil
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
	return s.txns, nil
}
