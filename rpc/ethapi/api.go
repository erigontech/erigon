// Copyright 2015 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package ethapi

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/execution/abi"
	"github.com/erigontech/erigon/execution/tracing/tracers/logger"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

// CallArgs represents the arguments for a call.
type CallArgs struct {
	From                 *common.Address           `json:"from"`
	To                   *common.Address           `json:"to"`
	Gas                  *hexutil.Uint64           `json:"gas"`
	GasPrice             *hexutil.U256             `json:"gasPrice"`
	MaxPriorityFeePerGas *hexutil.U256             `json:"maxPriorityFeePerGas"`
	MaxFeePerGas         *hexutil.U256             `json:"maxFeePerGas"`
	MaxFeePerBlobGas     *hexutil.U256             `json:"maxFeePerBlobGas"`
	Value                *hexutil.U256             `json:"value"`
	Nonce                *hexutil.Uint64           `json:"nonce"`
	Data                 *hexutil.Bytes            `json:"data"`
	Input                *hexutil.Bytes            `json:"input"`
	AccessList           *types.AccessList         `json:"accessList"`
	ChainID              *hexutil.U256             `json:"chainId,omitempty"`
	BlobVersionedHashes  []common.Hash             `json:"blobVersionedHashes,omitempty"`
	AuthorizationList    []types.JsonAuthorization `json:"authorizationList"`
}

func (args *CallArgs) FromOrEmpty() accounts.Address {
	return args.from()
}

// from retrieves the transaction sender address.
func (args *CallArgs) from() accounts.Address {
	if args.From == nil {
		return accounts.ZeroAddress
	}
	return accounts.InternAddress(*args.From)
}

// ToMessage converts CallArgs to the Message type used by the core evm
func (args *CallArgs) ToMessage(globalGasCap uint64, baseFee *uint256.Int) (*types.Message, error) {
	// Reject invalid combinations of pre- and post-1559 fee styles
	if args.GasPrice != nil && (args.MaxFeePerGas != nil || args.MaxPriorityFeePerGas != nil) {
		return nil, errors.New("both gasPrice and (maxFeePerGas or maxPriorityFeePerGas) specified")
	}
	// Set sender address or use zero address if none specified.
	addr := args.from()

	// Set default gas & gas price if none were set
	gas := globalGasCap
	if gas == 0 {
		gas = uint64(math.MaxUint64 / 2)
	}
	if args.Gas != nil {
		gas = uint64(*args.Gas)
	}
	if globalGasCap != 0 && globalGasCap < gas {
		log.Warn("Caller gas above allowance, capping", "requested", gas, "cap", globalGasCap)
		gas = globalGasCap
	}

	var (
		gasPrice         *uint256.Int
		gasFeeCap        *uint256.Int
		gasTipCap        *uint256.Int
		maxFeePerBlobGas *uint256.Int
	)
	if baseFee == nil {
		// If there's no basefee, then it must be a non-1559 execution
		gasPrice = new(uint256.Int)
		if args.GasPrice != nil {
			gasPrice.Set((*uint256.Int)(args.GasPrice))
		}
		gasFeeCap, gasTipCap = gasPrice, gasPrice
	} else {
		// A basefee is provided, necessitating 1559-type execution
		if args.GasPrice != nil {
			// User specified the legacy gas field, convert to 1559 gas typing
			gasPrice = new(uint256.Int).Set((*uint256.Int)(args.GasPrice))
			gasFeeCap, gasTipCap = gasPrice, gasPrice
		} else {
			// User specified 1559 gas fields (or none), use those
			gasFeeCap = new(uint256.Int)
			if args.MaxFeePerGas != nil {
				gasFeeCap.Set((*uint256.Int)(args.MaxFeePerGas))
			}
			gasTipCap = new(uint256.Int)
			if args.MaxPriorityFeePerGas != nil {
				gasTipCap.Set((*uint256.Int)(args.MaxPriorityFeePerGas))
			}
			// Backfill the legacy gasPrice for EVM execution, unless we're all zeroes
			gasPrice = new(uint256.Int)
			if !gasFeeCap.IsZero() || !gasTipCap.IsZero() {
				min := u256.Min(*new(uint256.Int).Add(gasTipCap, baseFee), *gasFeeCap)
				gasPrice = &min
			}
		}
		if args.MaxFeePerBlobGas != nil {
			maxFeePerBlobGas = new(uint256.Int).Set((*uint256.Int)(args.MaxFeePerBlobGas))
		}
	}

	value := new(uint256.Int)
	if args.Value != nil {
		value.Set((*uint256.Int)(args.Value))
	}
	var data []byte
	if args.Input != nil {
		data = *args.Input
	} else if args.Data != nil {
		data = *args.Data
	}
	var accessList types.AccessList
	if args.AccessList != nil {
		accessList = *args.AccessList
	}
	var nonce uint64
	if args.Nonce != nil {
		nonce = args.Nonce.Uint64()
	}

	var to = accounts.NilAddress
	if args.To != nil {
		to = accounts.InternAddress(*args.To)
	}

	msg := types.NewMessage(addr, to, nonce, value, gas, gasPrice, gasFeeCap, gasTipCap, data, accessList, false /* checkNonce */, false /* checkTransaction */, false /* checkGas */, false /* isFree */, maxFeePerBlobGas)

	if args.BlobVersionedHashes != nil {
		msg.SetBlobVersionedHashes(args.BlobVersionedHashes)
	}

	if args.AuthorizationList != nil {
		authorizations := make([]types.Authorization, len(args.AuthorizationList))
		for i := range args.AuthorizationList {
			var err error
			authorizations[i], err = args.AuthorizationList[i].ToAuthorization()
			if err != nil {
				return nil, err
			}
		}
		msg.SetAuthorizations(authorizations)
	}

	return msg, nil
}

// ToTransaction converts CallArgs to the Transaction type used by the core evm
func (args *CallArgs) ToTransaction(globalGasCap uint64, baseFee *uint256.Int) (types.Transaction, error) {
	var chainID uint256.Int
	if args.ChainID != nil {
		chainID = uint256.Int(*args.ChainID)
	}

	msg, err := args.ToMessage(globalGasCap, baseFee)
	if err != nil {
		return nil, err
	}

	var tx types.Transaction
	switch {
	case args.AuthorizationList != nil:
		al := types.AccessList{}
		if args.AccessList != nil {
			al = *args.AccessList
		}
		authorizations := make([]types.Authorization, 0)
		if args.AuthorizationList != nil {
			authorizations = make([]types.Authorization, len(args.AuthorizationList))
			for i := range args.AuthorizationList {
				authorizations[i], err = args.AuthorizationList[i].ToAuthorization()
				if err != nil {
					return nil, err
				}
			}
		}
		tx = &types.SetCodeTransaction{
			DynamicFeeTransaction: types.DynamicFeeTransaction{
				CommonTx: types.CommonTx{
					Nonce:    msg.Nonce(),
					GasLimit: msg.Gas(),
					To:       args.To,
					Value:    *msg.Value(),
					Data:     msg.Data(),
				},
				ChainID:    chainID,
				FeeCap:     *msg.FeeCap(),
				TipCap:     *msg.TipCap(),
				AccessList: al,
			},
			Authorizations: authorizations,
		}
	case args.BlobVersionedHashes != nil:
		al := types.AccessList{}
		if args.AccessList != nil {
			al = *args.AccessList
		}
		var maxFeePerBlobGas uint256.Int
		if args.MaxFeePerBlobGas != nil {
			maxFeePerBlobGas = uint256.Int(*args.MaxFeePerBlobGas)
		}
		tx = &types.BlobTx{
			DynamicFeeTransaction: types.DynamicFeeTransaction{
				CommonTx: types.CommonTx{
					Nonce:    msg.Nonce(),
					GasLimit: msg.Gas(),
					To:       args.To,
					Value:    *msg.Value(),
					Data:     msg.Data(),
				},
				ChainID:    chainID,
				FeeCap:     *msg.FeeCap(),
				TipCap:     *msg.TipCap(),
				AccessList: al,
			},
			MaxFeePerBlobGas:    maxFeePerBlobGas,
			BlobVersionedHashes: args.BlobVersionedHashes,
		}
	case args.MaxFeePerGas != nil:
		al := types.AccessList{}
		if args.AccessList != nil {
			al = *args.AccessList
		}
		tx = &types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{
				Nonce:    msg.Nonce(),
				GasLimit: msg.Gas(),
				To:       args.To,
				Value:    *msg.Value(),
				Data:     msg.Data(),
			},
			ChainID:    chainID,
			FeeCap:     *msg.FeeCap(),
			TipCap:     *msg.TipCap(),
			AccessList: al,
		}
	// Unlike Geth, an explicit accessList with gasPrice produces type 1 rather than dropping the list.
	case args.AccessList != nil:
		al := types.AccessList{}
		if args.AccessList != nil {
			al = *args.AccessList
		}
		tx = &types.AccessListTx{
			LegacyTx: types.LegacyTx{
				CommonTx: types.CommonTx{
					Nonce:    msg.Nonce(),
					GasLimit: msg.Gas(),
					To:       args.To,
					Value:    *msg.Value(),
					Data:     msg.Data(),
				},
				GasPrice: *msg.GasPrice(),
			},
			ChainID:    chainID,
			AccessList: al,
		}
	default:
		tx = &types.LegacyTx{
			CommonTx: types.CommonTx{
				Nonce:    msg.Nonce(),
				GasLimit: msg.Gas(),
				To:       args.To,
				Value:    *msg.Value(),
				Data:     msg.Data(),
			},
			GasPrice: *msg.GasPrice(),
		}
	}
	return tx, nil
}

// Account indicates the overriding fields of account during the execution of
// a message call.
// Note, state and stateDiff can't be specified at the same time. If state is
// set, message execution will only use the data in the given state. Otherwise
// if statDiff is set, all diff will be applied first and then execute the call
// message.
type Account struct {
	Nonce            *hexutil.Uint64              `json:"nonce"`
	Code             *hexutil.Bytes               `json:"code"`
	Balance          **hexutil.Big                `json:"balance"`
	State            *map[common.Hash]common.Hash `json:"state"`
	StateDiff        *map[common.Hash]common.Hash `json:"stateDiff"`
	MovePrecompileTo *common.Address              `json:"movePrecompileToAddress"`
}

func NewRevertError(result *evmtypes.ExecutionResult) *RevertError {
	reason, errUnpack := abi.UnpackRevert(result.Revert())
	err := errors.New("execution reverted")
	if errUnpack == nil {
		err = fmt.Errorf("execution reverted: %v", reason)
	}
	return &RevertError{
		error:  err,
		reason: hexutil.Encode(result.Revert()),
	}
}

// RevertError is an API error that encompassas an EVM revertal with JSON error
// code and a binary data blob.
type RevertError struct {
	error
	reason string // revert reason hex encoded
}

// ErrorCode returns the JSON error code for a revertal.
// See: https://eips.ethereum.org/EIPS/eip-1474#json-rpc
func (e *RevertError) ErrorCode() int {
	return 3
}

// ErrorData returns the hex encoded revert reason.
func (e *RevertError) ErrorData() any {
	return e.reason
}

// ExecutionResult groups all structured logs emitted by the EVM
// while replaying a transaction in debug mode as well as transaction
// execution status, the amount of gas used and the return value
type ExecutionResult struct {
	Gas         uint64         `json:"gas"`
	Failed      bool           `json:"failed"`
	ReturnValue string         `json:"returnValue"`
	StructLogs  []StructLogRes `json:"structLogs"`
}

// StructLogRes stores a structured log emitted by the EVM while replaying a
// transaction in debug mode
type StructLogRes = logger.StructLogRes

// FormatLogs formats EVM returned structured logs for json output
func FormatLogs(logs []logger.StructLog) []StructLogRes {
	return logger.FormatLogs(logs)
}

// RPCMarshalHeader converts the given header to the RPC output .
func RPCMarshalHeader(head *types.Header) map[string]any {
	result := map[string]any{
		"number":           (*hexutil.Big)(head.Number.ToBig()),
		"hash":             head.Hash(),
		"parentHash":       head.ParentHash,
		"nonce":            head.Nonce,
		"mixHash":          head.MixDigest,
		"sha3Uncles":       head.UncleHash,
		"logsBloom":        head.Bloom,
		"stateRoot":        head.Root,
		"miner":            head.Coinbase,
		"difficulty":       (*hexutil.Big)(head.Difficulty.ToBig()),
		"extraData":        hexutil.Bytes(head.Extra),
		"size":             hexutil.Uint64(head.Size()),
		"gasLimit":         hexutil.Uint64(head.GasLimit),
		"gasUsed":          hexutil.Uint64(head.GasUsed),
		"timestamp":        hexutil.Uint64(head.Time),
		"transactionsRoot": head.TxHash,
		"receiptsRoot":     head.ReceiptHash,
	}
	if head.BaseFee != nil {
		result["baseFeePerGas"] = (*hexutil.Big)(head.BaseFee.ToBig())
	}
	if head.WithdrawalsHash != nil {
		result["withdrawalsRoot"] = head.WithdrawalsHash
	}
	if head.BlobGasUsed != nil {
		result["blobGasUsed"] = (*hexutil.Uint64)(head.BlobGasUsed)
	}
	if head.ExcessBlobGas != nil {
		result["excessBlobGas"] = (*hexutil.Uint64)(head.ExcessBlobGas)
	}
	if head.ParentBeaconBlockRoot != nil {
		result["parentBeaconBlockRoot"] = head.ParentBeaconBlockRoot
	}
	if head.RequestsHash != nil {
		result["requestsHash"] = head.RequestsHash
	}
	if head.BlockAccessListHash != nil {
		result["blockAccessListHash"] = head.BlockAccessListHash
	}
	if head.SlotNumber != nil {
		result["slotNumber"] = (*hexutil.Uint64)(head.SlotNumber)
	}

	// For Gnosis only
	if head.AuRaSeal != nil {
		result["auraSeal"] = hexutil.Bytes(head.AuRaSeal)
		result["auraStep"] = (hexutil.Uint64)(head.AuRaStep)
	}

	return result
}

// RPCMarshalBlock converts the given block to the RPC output which depends on fullTx. If inclTx is true transactions are
// returned. When fullTx is true the returned block contains full transaction details, otherwise it will only contain
// transaction hashes.
func RPCMarshalBlockDeprecated(block *types.Block, inclTx bool, fullTx bool) (map[string]any, error) {
	fields := RPCMarshalHeader(block.Header())
	fields["size"] = hexutil.Uint64(block.Size())
	if _, ok := fields["transactions"]; !ok {
		fields["transactions"] = make([]any, 0)
	}

	if inclTx {
		formatTx := func(tx types.Transaction, index int) (any, error) {
			return tx.Hash(), nil
		}
		if fullTx {
			formatTx = func(tx types.Transaction, index int) (any, error) {
				return newRPCTransactionFromBlockAndTxGivenIndex(block, tx, uint64(index)), nil
			}
		}
		txs := block.Transactions()
		transactions := make([]any, len(txs))
		var err error
		for i, txn := range txs {
			if transactions[i], err = formatTx(txn, i); err != nil {
				return nil, err
			}
		}

		fields["transactions"] = transactions
	}
	uncles := block.Uncles()
	uncleHashes := make([]common.Hash, len(uncles))
	for i, uncle := range uncles {
		uncleHashes[i] = uncle.Hash()
	}
	fields["uncles"] = uncleHashes

	if block.Withdrawals() != nil {
		fields["withdrawals"] = block.Withdrawals()
	}

	return fields, nil
}

// SignTransactionResult represents a RLP-encoded transaction paired with its JSON form.
type SignTransactionResult struct {
	Raw hexutil.Bytes   `json:"raw"`
	Tx  *RPCTransaction `json:"tx"`
}

func (r SignTransactionResult) MarshalJSON() ([]byte, error) {
	if r.Tx == nil {
		return nil, errors.New("nil transaction")
	}
	type plain struct {
		Raw hexutil.Bytes   `json:"raw"`
		Tx  json.RawMessage `json:"tx"`
	}
	txBytes, err := json.Marshal(r.Tx)
	if err != nil {
		return nil, err
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(txBytes, &m); err != nil {
		return nil, err
	}
	for _, k := range []string{"blockHash", "blockNumber", "blockTimestamp", "transactionIndex", "from"} {
		delete(m, k)
	}
	nullVal := json.RawMessage("null")
	for _, k := range []string{"gasPrice", "maxFeePerGas", "maxPriorityFeePerGas"} {
		if _, ok := m[k]; !ok {
			m[k] = nullVal
		}
	}
	zeroHex := json.RawMessage(`"0x0"`)
	for _, k := range []string{"v", "r", "s"} {
		if v, ok := m[k]; !ok || string(v) == "null" {
			m[k] = zeroHex
		}
	}
	stripped, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return json.Marshal(plain{Raw: r.Raw, Tx: stripped})
}

// RPCTransaction represents a transaction that will serialize to the RPC representation of a transaction.
// Numeric fields may alias the source transaction; they are read-only after construction.
type RPCTransaction struct {
	BlockHash            *common.Hash               `json:"blockHash"`
	BlockNumber          *hexutil.U256              `json:"blockNumber"`
	BlockTimestamp       *hexutil.Uint64            `json:"blockTimestamp"`
	From                 common.Address             `json:"from"`
	Gas                  hexutil.Uint64             `json:"gas"`
	GasPrice             *hexutil.U256              `json:"gasPrice,omitempty"`
	MaxPriorityFeePerGas *hexutil.U256              `json:"maxPriorityFeePerGas,omitempty"`
	MaxFeePerGas         *hexutil.U256              `json:"maxFeePerGas,omitempty"`
	Hash                 common.Hash                `json:"hash"`
	Input                hexutil.Bytes              `json:"input"`
	Nonce                hexutil.Uint64             `json:"nonce"`
	To                   *common.Address            `json:"to"`
	TransactionIndex     *hexutil.Uint64            `json:"transactionIndex"`
	Value                *hexutil.U256              `json:"value"`
	Type                 hexutil.Uint64             `json:"type"`
	Accesses             *types.AccessList          `json:"accessList,omitempty"`
	ChainID              *hexutil.U256              `json:"chainId,omitempty"`
	MaxFeePerBlobGas     *hexutil.U256              `json:"maxFeePerBlobGas,omitempty"`
	BlobVersionedHashes  []common.Hash              `json:"blobVersionedHashes,omitempty"`
	Authorizations       *[]types.JsonAuthorization `json:"authorizationList,omitempty"`
	V                    *hexutil.U256              `json:"v"`
	YParity              *hexutil.U256              `json:"yParity,omitempty"`
	R                    *hexutil.U256              `json:"r"`
	S                    *hexutil.U256              `json:"s"`
}

// NewRPCTransaction returns a transaction that will serialize to the RPC
// representation, with the given location metadata set (if available).
func NewRPCTransaction(txn types.Transaction, blockHash common.Hash, blockTime uint64, blockNumber uint64, index uint64, baseFee *uint256.Int) *RPCTransaction {
	// Determine the signer. For replay-protected transactions, use the most permissive
	// signer, because we assume that signers are backwards-compatible with old
	// transactions. For non-protected transactions, the homestead signer is used
	// because the return value of ChainId is zero for those transactions.
	chainId := new(uint256.Int)
	result := &RPCTransaction{
		Type:  hexutil.Uint64(txn.Type()),
		Gas:   hexutil.Uint64(txn.GetGasLimit()),
		Hash:  txn.Hash(),
		Input: hexutil.Bytes(txn.GetData()),
		Nonce: hexutil.Uint64(txn.GetNonce()),
		To:    txn.GetTo(),
		Value: (*hexutil.U256)(txn.GetValue()),
	}
	if t, ok := txn.(*types.BlobTxWrapper); ok {
		txn = &t.Tx
	}

	v, r, s := txn.RawSignatureValues()
	// For LegacyTx, v=r=s=0 means an unsigned system/protocol transaction (e.g. EIP-4788);
	// match geth which returns null for these. For typed transactions (EIP-1559, EIP-2930…),
	// v=0 is valid yParity=0 and must serialise as "0x0" — do not suppress it.
	if txn.Type() != types.LegacyTxType || !v.IsZero() || !r.IsZero() || !s.IsZero() {
		result.V = (*hexutil.U256)(v)
		result.R = (*hexutil.U256)(r)
		result.S = (*hexutil.U256)(s)
	}

	if txn.Type() == types.LegacyTxType {
		if !v.IsZero() { // skip chain id derivation in case of call simulation (where v,r,s are zero)
			var err error
			chainId, err = types.DeriveChainId(v)
			// if a legacy transaction has an EIP-155 chain id, include it explicitly, otherwise chain id is not included
			if err != nil {
				log.Warn("[rpc] chain id derivation", "err", err)
			} else if !chainId.IsZero() {
				result.ChainID = (*hexutil.U256)(chainId)
			}
		}
		result.GasPrice = (*hexutil.U256)(txn.GetTipCap())
	} else {
		chainId = txn.GetChainID()
		result.ChainID = (*hexutil.U256)(chainId)
		result.YParity = (*hexutil.U256)(v)
		acl := txn.GetAccessList()
		result.Accesses = &acl

		if txn.Type() == types.AccessListTxType {
			result.GasPrice = (*hexutil.U256)(txn.GetTipCap())
		} else {
			result.GasPrice = computeGasPrice(txn, blockHash, baseFee)
			result.MaxPriorityFeePerGas = (*hexutil.U256)(txn.GetTipCap())
			result.MaxFeePerGas = (*hexutil.U256)(txn.GetFeeCap())
		}

		if txn.Type() == types.BlobTxType {
			blobTx := txn.(*types.BlobTx)
			result.MaxFeePerBlobGas = (*hexutil.U256)(&blobTx.MaxFeePerBlobGas)
			result.BlobVersionedHashes = blobTx.BlobVersionedHashes
		} else if txn.Type() == types.SetCodeTxType {
			setCodeTx := txn.(*types.SetCodeTransaction)
			auths := setCodeTx.GetAuthorizations()
			ats := make([]types.JsonAuthorization, len(auths))
			for i := range auths {
				ats[i] = types.JsonAuthorization{}.FromAuthorization(auths[i])
			}
			result.Authorizations = &ats
		}
	}

	signer := types.LatestSignerForChainID(chainId)
	from, err := txn.Sender(*signer)
	if err != nil {
		log.Warn("sender recovery", "err", err)
	} else {
		result.From = from.Value()
	}

	if blockHash != (common.Hash{}) {
		result.BlockHash = &blockHash
		result.BlockNumber = (*hexutil.U256)(uint256.NewInt(blockNumber))
		result.BlockTimestamp = (*hexutil.Uint64)(&blockTime)
		result.TransactionIndex = (*hexutil.Uint64)(&index)
	}
	return result
}

func computeGasPrice(txn types.Transaction, _ common.Hash, baseFee *uint256.Int) *hexutil.U256 {
	if baseFee != nil {
		// price = min(tip + baseFee, gasFeeCap)
		price := u256.Min(u256.Add(*txn.GetTipCap(), *baseFee), *txn.GetFeeCap())
		return (*hexutil.U256)(&price)
	}
	return nil
}

// newRPCTransactionFromBlockAndTxGivenIndex returns a transaction that will serialize to the RPC representation.
func newRPCTransactionFromBlockAndTxGivenIndex(b *types.Block, txn types.Transaction, index uint64) *RPCTransaction {
	return NewRPCTransaction(txn, b.Hash(), b.Time(), b.NumberU64(), index, b.BaseFee())
}
