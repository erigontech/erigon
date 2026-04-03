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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/tests/testforks"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
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
		if err := json.Unmarshal(raw.Params[3], &p.Requests); err != nil {
			return fmt.Errorf("failed to unmarshal executionRequests: %v", err)
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

	for i, payload := range t.json.Payloads {
		// Version validation
		if err := validatePayloadVersion(&payload, config); err != nil {
			if payload.ErrorCode != nil {
				var rpcErr *rpc.InvalidParamsError
				var unsupportedErr *rpc.UnsupportedForkError
				if errors.As(err, &rpcErr) || errors.As(err, &unsupportedErr) {
					continue // Expected error
				}
			}
			if payload.ValidationError != "" {
				continue // Expected to be invalid
			}
			return fmt.Errorf("payload %d: %v", i, err)
		}

		// Convert payload to block
		block, balBytes, err := payloadToBlock(&payload)
		if err != nil {
			if payload.ValidationError != "" {
				continue // Expected to be invalid
			}
			return fmt.Errorf("payload %d: failed to convert payload to block: %v", i, err)
		}

		// Insert block
		chain := &blockgen.ChainPack{
			Blocks:           []*types.Block{block},
			Headers:          []*types.Header{block.HeaderNoCopy()},
			TopBlock:         block,
			BlockAccessLists: [][]byte{balBytes},
		}
		if err := m.InsertChain(chain); err != nil {
			if payload.ValidationError != "" {
				continue // Expected to be invalid
			}
			return fmt.Errorf("payload %d: block insertion failed: %v", i, err)
		}
		// If we expected this payload to be invalid but it succeeded
		if payload.ValidationError != "" {
			return fmt.Errorf("payload %d: expected validation error %q but block was accepted", i, payload.ValidationError)
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
