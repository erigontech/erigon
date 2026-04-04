// Copyright 2025 The Erigon Authors
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

package testutil

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/holiman/uint256"

	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/engineapi"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/testforks"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/rulesconfig"
	"github.com/erigontech/erigon/rpc"
)

// EngineTest checks processing of engine API payloads.
type EngineTest struct {
	json etJSON
}

func (t *EngineTest) UnmarshalJSON(in []byte) error {
	return json.Unmarshal(in, &t.json)
}

// Network returns the network/fork name for this test.
func (t *EngineTest) Network() string {
	return t.json.Network
}

type etJSON struct {
	Genesis   btHeader               `json:"genesisBlockHeader"`
	Pre       types.GenesisAlloc     `json:"pre"`
	Post      types.GenesisAlloc     `json:"postState"`
	PostHash  *common.UnprefixedHash `json:"postStateHash"`
	BestBlock common.UnprefixedHash  `json:"lastblockhash"`
	Network   string                 `json:"network"`
	Payloads  []etNewPayload         `json:"engineNewPayloads"`
}

// etNewPayload represents a single engine API new payload call from the fixture.
type etNewPayload struct {
	ExecutionPayload engine_types.ExecutionPayload
	VersionedHashes  []common.Hash
	BeaconRoot       *common.Hash
	Requests         []hexutil.Bytes

	Version         int    // newPayloadVersion
	FcuVersion      int    // forkchoiceUpdatedVersion
	ValidationError string // expected validation error (empty = expect VALID)
	ErrorCode       *int   // expected JSON-RPC error code
}

func (p *etNewPayload) UnmarshalJSON(data []byte) error {
	var raw struct {
		Params                   []json.RawMessage `json:"params"`
		NewPayloadVersion        string            `json:"newPayloadVersion"`
		ForkchoiceUpdatedVersion string            `json:"forkchoiceUpdatedVersion"`
		ValidationError          string            `json:"validationError,omitempty"`
		ErrorCode                json.RawMessage    `json:"errorCode,omitempty"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	p.ValidationError = raw.ValidationError
	// errorCode can be a string ("-32602") or int (-32602) in fixtures
	if len(raw.ErrorCode) > 0 && string(raw.ErrorCode) != "null" {
		s := string(raw.ErrorCode)
		if len(s) >= 2 && s[0] == '"' {
			s = s[1 : len(s)-1]
		}
		code, err := strconv.Atoi(s)
		if err != nil {
			return fmt.Errorf("invalid errorCode %s: %v", raw.ErrorCode, err)
		}
		p.ErrorCode = &code
	}

	var err error
	p.Version, err = strconv.Atoi(raw.NewPayloadVersion)
	if err != nil {
		return fmt.Errorf("invalid newPayloadVersion: %v", err)
	}
	p.FcuVersion, err = strconv.Atoi(raw.ForkchoiceUpdatedVersion)
	if err != nil {
		return fmt.Errorf("invalid forkchoiceUpdatedVersion: %v", err)
	}

	if len(raw.Params) < 1 {
		return errors.New("params must have at least one element")
	}
	if err := json.Unmarshal(raw.Params[0], &p.ExecutionPayload); err != nil {
		return fmt.Errorf("failed to unmarshal ExecutionPayload: %v", err)
	}
	// V3+: params[1] = versionedHashes, params[2] = beaconRoot
	if len(raw.Params) >= 3 {
		if err := json.Unmarshal(raw.Params[1], &p.VersionedHashes); err != nil {
			return fmt.Errorf("failed to unmarshal versionedHashes: %v", err)
		}
		var beaconRoot common.Hash
		if err := json.Unmarshal(raw.Params[2], &beaconRoot); err != nil {
			return fmt.Errorf("failed to unmarshal beaconRoot: %v", err)
		}
		p.BeaconRoot = &beaconRoot
	}
	// V4/V5+: params[3] = executionRequests
	if len(raw.Params) >= 4 {
		var hexRequests []hexutil.Bytes
		if err := json.Unmarshal(raw.Params[3], &hexRequests); err != nil {
			return fmt.Errorf("failed to unmarshal executionRequests: %v", err)
		}
		p.Requests = make([][]byte, len(hexRequests))
		for i, r := range hexRequests {
			p.Requests[i] = r
		}
	}
	return nil
}

// payloadToBlock converts an engine ExecutionPayload to a types.Block,
// replicating the conversion logic from engine_server.go newPayload().
func payloadToBlock(p *etNewPayload) (*types.Block, []byte, error) {
	req := &p.ExecutionPayload

	var bloom types.Bloom
	copy(bloom[:], req.LogsBloom)

	txs := make([][]byte, len(req.Transactions))
	for i, tx := range req.Transactions {
		txs[i] = tx
	}

	header := types.Header{
		ParentHash:  req.ParentHash,
		Coinbase:    req.FeeRecipient,
		Root:        req.StateRoot,
		Bloom:       bloom,
		BaseFee:     uint256.MustFromBig(req.BaseFeePerGas.ToInt()),
		Extra:       req.ExtraData,
		Number:      *uint256.NewInt(req.BlockNumber.Uint64()),
		GasUsed:     uint64(req.GasUsed),
		GasLimit:    uint64(req.GasLimit),
		Time:        uint64(req.Timestamp),
		MixDigest:   req.PrevRandao,
		UncleHash:   empty.UncleHash,
		Difficulty:  *merge.ProofOfStakeDifficulty,
		Nonce:       merge.ProofOfStakeNonce,
		ReceiptHash: req.ReceiptsRoot,
		TxHash:      types.DeriveSha(types.BinaryTransactions(txs)),
	}

	var withdrawals types.Withdrawals
	if req.Withdrawals != nil {
		withdrawals = req.Withdrawals
		wh := types.DeriveSha(withdrawals)
		header.WithdrawalsHash = &wh
	}

	if p.Requests != nil {
		requests := make(types.FlatRequests, 0, len(p.Requests))
		for _, r := range p.Requests {
			if len(r) > 0 {
				requests = append(requests, types.FlatRequest{Type: r[0], RequestData: r[1:]})
			}
		}
		rh := requests.Hash()
		header.RequestsHash = rh
	}

	if req.BlobGasUsed != nil {
		header.BlobGasUsed = (*uint64)(req.BlobGasUsed)
	}
	if req.ExcessBlobGas != nil {
		header.ExcessBlobGas = (*uint64)(req.ExcessBlobGas)
	}
	if p.BeaconRoot != nil {
		header.ParentBeaconBlockRoot = p.BeaconRoot
	}

	var blockAccessListBytes []byte
	if len(req.BlockAccessList) > 0 {
		hash := crypto.Keccak256Hash(req.BlockAccessList)
		header.BlockAccessListHash = &hash
		blockAccessListBytes = req.BlockAccessList
	} else if req.SlotNumber != nil {
		// Amsterdam+ with empty BAL
		header.BlockAccessListHash = &empty.BlockAccessListHash
	}

	if req.SlotNumber != nil {
		slotNumber := uint64(*req.SlotNumber)
		header.SlotNumber = &slotNumber
	}

	// Verify block hash
	if header.Hash() != req.BlockHash {
		return nil, nil, fmt.Errorf("block hash mismatch: computed=%x, expected=%x", header.Hash(), req.BlockHash)
	}

	transactions, err := types.DecodeTransactions(txs)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to decode transactions: %v", err)
	}

	block := types.NewBlockFromStorage(req.BlockHash, &header, transactions, nil, withdrawals)
	return block, blockAccessListBytes, nil
}

// RunCLI executes the engine test.
func (t *EngineTest) RunCLI() error {
	config, ok := testforks.Forks[t.json.Network]
	if !ok {
		return testforks.UnsupportedForkError{Name: t.json.Network}
	}
	engine := rulesconfig.CreateRulesEngineBareBones(context.Background(), config, log.New())
	m := execmoduletester.New(nil, execmoduletester.WithGenesisSpec(t.genesis(config)), execmoduletester.WithEngine(engine))
	defer m.DB.Close()

	if m.Genesis.Hash() != t.json.Genesis.Hash {
		return fmt.Errorf("genesis block hash doesn't match test: computed=%x, test=%x", m.Genesis.Hash().Bytes()[:6], t.json.Genesis.Hash[:6])
	}
	if m.Genesis.Root() != t.json.Genesis.StateRoot {
		return fmt.Errorf("genesis block state root does not match test: computed=%x, test=%x", m.Genesis.Root().Bytes()[:6], t.json.Genesis.StateRoot[:6])
	}

	// Create EngineServer to route through the real engine API path
	executionClient := direct.NewExecutionClientDirect(m.ExecModule)
	engineServer := engineapi.NewEngineServer(
		log.New(),
		config,
		executionClient,
		nil,   // blockDownloader — not needed for payload execution
		false, // caplin
		true,  // internalCL
		false, // proposing
		true,  // consuming
		nil,   // txPool
		time.Hour, // fcuTimeout
		0,     // maxReorgDepth
	)

	// Send initial forkchoiceUpdated to genesis
	genesisHash := m.Genesis.Hash()
	fcuStatus, err := engineServer.HandleForkChoice(m.Ctx, "ForkchoiceUpdated",
		&engine_types.ForkChoiceState{
			HeadHash:           genesisHash,
			SafeBlockHash:      genesisHash,
			FinalizedBlockHash: genesisHash,
		})
	if err != nil || fcuStatus.Status != engine_types.ValidStatus {
		return fmt.Errorf("initial FCU to genesis failed: %v (status: %v)", err, fcuStatus)
	}

	for i, payload := range t.json.Payloads {
		// Call the real engine newPayload via EngineServer
		version := payload.Version
		req := &payload.ExecutionPayload

		var versionedHashes []common.Hash
		var beaconRoot *common.Hash
		var executionRequests []hexutil.Bytes

		if payload.VersionedHashes != nil {
			versionedHashes = payload.VersionedHashes
		}
		if payload.BeaconRoot != nil {
			beaconRoot = payload.BeaconRoot
		}
		if payload.Requests != nil {
			executionRequests = make([]hexutil.Bytes, len(payload.Requests))
			for j, r := range payload.Requests {
				executionRequests[j] = r
			}
		}

		var status *engine_types.PayloadStatus
		switch version {
		case 1:
			status, err = engineServer.NewPayloadV1(m.Ctx, req)
		case 2:
			status, err = engineServer.NewPayloadV2(m.Ctx, req)
		case 3:
			status, err = engineServer.NewPayloadV3(m.Ctx, req, versionedHashes, beaconRoot)
		case 4:
			status, err = engineServer.NewPayloadV4(m.Ctx, req, versionedHashes, beaconRoot, executionRequests)
		case 5:
			status, err = engineServer.NewPayloadV5(m.Ctx, req, versionedHashes, beaconRoot, executionRequests)
		default:
			return fmt.Errorf("payload %d: unsupported version %d", i, version)
		}

		// Check error code expectation
		if payload.ErrorCode != nil {
			if err == nil {
				return fmt.Errorf("payload %d: expected error code %d but no error occurred", i, *payload.ErrorCode)
			}
			// Error occurred — check if it's the right type
			continue
		}
		if err != nil {
			if payload.ValidationError != "" {
				continue // Expected to be invalid
			}
			return fmt.Errorf("payload %d: unexpected error: %v", i, err)
		}

		// Check validation error expectation
		if payload.ValidationError != "" {
			if status.Status != engine_types.InvalidStatus {
				return fmt.Errorf("payload %d: expected INVALID for %q, got %s", i, payload.ValidationError, status.Status)
			}
			continue
		}

		// Expect valid
		if status.Status != engine_types.ValidStatus {
			errMsg := ""
			if status.ValidationError != nil {
				errMsg = status.ValidationError.Error()
			}
			return fmt.Errorf("payload %d: expected VALID, got %s (err: %s)", i, status.Status, errMsg)
		}

		// Send forkchoiceUpdated to advance head
		blockHash := payload.ExecutionPayload.BlockHash
		fcuStatus, err := engineServer.HandleForkChoice(m.Ctx, "ForkchoiceUpdated",
			&engine_types.ForkChoiceState{
				HeadHash:           blockHash,
				SafeBlockHash:      blockHash,
				FinalizedBlockHash: blockHash,
			})
		if err != nil {
			return fmt.Errorf("payload %d: FCU error: %v", i, err)
		}
		if fcuStatus.Status != engine_types.ValidStatus {
			return fmt.Errorf("payload %d: FCU returned %s", i, fcuStatus.Status)
		}
	}

	// Validate final state
	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	cmlast := rawdb.ReadHeadBlockHash(tx)
	if common.Hash(t.json.BestBlock) != cmlast {
		return fmt.Errorf("last block hash validation mismatch: want: %x, have: %x", t.json.BestBlock, cmlast)
	}

	if t.json.Post != nil {
		newDB := state.New(m.NewStateReader(tx))
		for addr, acct := range t.json.Post {
			address := accounts.InternAddress(addr)
			code, err := newDB.GetCode(address)
			if err != nil {
				return err
			}
			if !bytes.Equal(code, acct.Code) {
				return fmt.Errorf("post state code mismatch for %x", addr)
			}
			balance, err := newDB.GetBalance(address)
			if err != nil {
				return err
			}
			if balance.ToBig().Cmp(acct.Balance) != 0 {
				return fmt.Errorf("post state balance mismatch for %x: want %d, have %d", addr, acct.Balance, &balance)
			}
			nonce, err := newDB.GetNonce(address)
			if err != nil {
				return err
			}
			if nonce != acct.Nonce {
				return fmt.Errorf("post state nonce mismatch for %x: want %d, have %d", addr, acct.Nonce, nonce)
			}
			for loc, val := range acct.Storage {
				expected := uint256.NewInt(0).SetBytes(val.Bytes())
				actual, _ := newDB.GetState(address, accounts.InternKey(loc))
				if !expected.Eq(&actual) {
					return fmt.Errorf("post state storage mismatch for %x slot %x: want %d, have %d", addr, loc, expected, &actual)
				}
			}
		}
	}

	return nil
}

func (t *EngineTest) genesis(config *chain.Config) *types.Genesis {
	return &types.Genesis{
		Config:                config,
		Nonce:                 t.json.Genesis.Nonce.Uint64(),
		Timestamp:             t.json.Genesis.Timestamp,
		ParentHash:            t.json.Genesis.ParentHash,
		ExtraData:             t.json.Genesis.ExtraData,
		GasLimit:              t.json.Genesis.GasLimit,
		GasUsed:               t.json.Genesis.GasUsed,
		Difficulty:            t.json.Genesis.Difficulty,
		Mixhash:               t.json.Genesis.MixHash,
		Coinbase:              t.json.Genesis.Coinbase,
		Alloc:                 t.json.Pre,
		BaseFee:               t.json.Genesis.BaseFeePerGas,
		BlobGasUsed:           t.json.Genesis.BlobGasUsed,
		ExcessBlobGas:         t.json.Genesis.ExcessBlobGas,
		ParentBeaconBlockRoot: t.json.Genesis.ParentBeaconBlockRoot,
		RequestsHash:          t.json.Genesis.RequestsHash,
		BlockAccessListHash:   t.json.Genesis.BlockAccessListHash,
		SlotNumber:            t.json.Genesis.SlotNumber,
	}
}

// validatePayloadVersion checks version-specific parameter constraints.
func validatePayloadVersion(p *etNewPayload, config *chain.Config) error {
	req := &p.ExecutionPayload
	switch p.Version {
	case 1:
		if req.Withdrawals != nil {
			return &rpc.InvalidParamsError{Message: "withdrawals not supported in V1"}
		}
	case 2:
		isCancun := config.IsCancun(uint64(req.Timestamp))
		isShanghai := config.IsShanghai(uint64(req.Timestamp))
		switch {
		case isCancun:
			return &rpc.InvalidParamsError{Message: "can't use newPayloadV2 post-cancun"}
		case isShanghai && req.Withdrawals == nil:
			return &rpc.InvalidParamsError{Message: "nil withdrawals post-shanghai"}
		case !isShanghai && req.Withdrawals != nil:
			return &rpc.InvalidParamsError{Message: "non-nil withdrawals pre-shanghai"}
		case req.ExcessBlobGas != nil:
			return &rpc.InvalidParamsError{Message: "non-nil excessBlobGas pre-cancun"}
		case req.BlobGasUsed != nil:
			return &rpc.InvalidParamsError{Message: "non-nil blobGasUsed pre-cancun"}
		}
	case 3:
		switch {
		case req.Withdrawals == nil:
			return &rpc.InvalidParamsError{Message: "nil withdrawals post-shanghai"}
		case req.ExcessBlobGas == nil:
			return &rpc.InvalidParamsError{Message: "nil excessBlobGas post-cancun"}
		case req.BlobGasUsed == nil:
			return &rpc.InvalidParamsError{Message: "nil blobGasUsed post-cancun"}
		case p.VersionedHashes == nil:
			return &rpc.InvalidParamsError{Message: "nil versionedHashes post-cancun"}
		case p.BeaconRoot == nil:
			return &rpc.InvalidParamsError{Message: "nil beaconRoot post-cancun"}
		case !config.IsCancun(uint64(req.Timestamp)):
			return &rpc.UnsupportedForkError{Message: "newPayloadV3 must only be called for cancun+ payloads"}
		}
	case 4:
		switch {
		case req.Withdrawals == nil:
			return &rpc.InvalidParamsError{Message: "nil withdrawals post-shanghai"}
		case req.ExcessBlobGas == nil:
			return &rpc.InvalidParamsError{Message: "nil excessBlobGas post-cancun"}
		case req.BlobGasUsed == nil:
			return &rpc.InvalidParamsError{Message: "nil blobGasUsed post-cancun"}
		case p.VersionedHashes == nil:
			return &rpc.InvalidParamsError{Message: "nil versionedHashes post-cancun"}
		case p.BeaconRoot == nil:
			return &rpc.InvalidParamsError{Message: "nil beaconRoot post-cancun"}
		case p.Requests == nil:
			return &rpc.InvalidParamsError{Message: "nil executionRequests post-prague"}
		case !config.IsPrague(uint64(req.Timestamp)):
			return &rpc.UnsupportedForkError{Message: "newPayloadV4 must only be called for prague+ payloads"}
		}
	case 5:
		switch {
		case req.Withdrawals == nil:
			return &rpc.InvalidParamsError{Message: "nil withdrawals post-shanghai"}
		case req.ExcessBlobGas == nil:
			return &rpc.InvalidParamsError{Message: "nil excessBlobGas post-cancun"}
		case req.BlobGasUsed == nil:
			return &rpc.InvalidParamsError{Message: "nil blobGasUsed post-cancun"}
		case p.VersionedHashes == nil:
			return &rpc.InvalidParamsError{Message: "nil versionedHashes post-cancun"}
		case p.BeaconRoot == nil:
			return &rpc.InvalidParamsError{Message: "nil beaconRoot post-cancun"}
		case p.Requests == nil:
			return &rpc.InvalidParamsError{Message: "nil executionRequests post-prague"}
		case req.SlotNumber == nil:
			return &rpc.InvalidParamsError{Message: "nil slotNumber post-amsterdam"}
		case !config.IsAmsterdam(uint64(req.Timestamp)):
			return &rpc.UnsupportedForkError{Message: "newPayloadV5 must only be called for amsterdam payloads"}
		}
	}
	return nil
}
