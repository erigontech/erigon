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
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"

	"github.com/holiman/uint256"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/accounts/abi"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/eth/tracers/logger"
)

// CallArgs represents the arguments for a call.
type CallArgs struct {
	From                 *libcommon.Address `json:"from"`
	To                   *libcommon.Address `json:"to"`
	Gas                  *hexutil.Uint64    `json:"gas"`
	GasPrice             *hexutil.Big       `json:"gasPrice"`
	MaxPriorityFeePerGas *hexutil.Big       `json:"maxPriorityFeePerGas"`
	MaxFeePerGas         *hexutil.Big       `json:"maxFeePerGas"`
	MaxFeePerBlobGas     *hexutil.Big       `json:"maxFeePerBlobGas"`
	Value                *hexutil.Big       `json:"value"`
	Nonce                *hexutil.Uint64    `json:"nonce"`
	Data                 *hexutility.Bytes  `json:"data"`
	Input                *hexutility.Bytes  `json:"input"`
	AccessList           *types.AccessList  `json:"accessList"`
	ChainID              *hexutil.Big       `json:"chainId,omitempty"`
}

// from retrieves the transaction sender address.
func (arg *CallArgs) from() libcommon.Address {
	if arg.From == nil {
		return libcommon.Address{}
	}
	return *arg.From
}

// ToMessage converts CallArgs to the Message type used by the core evm
func (args *CallArgs) ToMessage(globalGasCap uint64, baseFee *uint256.Int) (types.Message, error) {
	// Reject invalid combinations of pre- and post-1559 fee styles
	if args.GasPrice != nil && (args.MaxFeePerGas != nil || args.MaxPriorityFeePerGas != nil) {
		return types.Message{}, errors.New("both gasPrice and (maxFeePerGas or maxPriorityFeePerGas) specified")
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
			overflow := gasPrice.SetFromBig(args.GasPrice.ToInt())
			if overflow {
				return types.Message{}, errors.New("args.GasPrice higher than 2^256-1")
			}
		}
		gasFeeCap, gasTipCap = gasPrice, gasPrice
	} else {
		// A basefee is provided, necessitating 1559-type execution
		if args.GasPrice != nil {
			// User specified the legacy gas field, convert to 1559 gas typing
			gasPrice = new(uint256.Int)
			overflow := gasPrice.SetFromBig(args.GasPrice.ToInt())
			if overflow {
				return types.Message{}, errors.New("args.GasPrice higher than 2^256-1")
			}
			gasFeeCap, gasTipCap = gasPrice, gasPrice
		} else {
			// User specified 1559 gas fields (or none), use those
			gasFeeCap = new(uint256.Int)
			if args.MaxFeePerGas != nil {
				overflow := gasFeeCap.SetFromBig(args.MaxFeePerGas.ToInt())
				if overflow {
					return types.Message{}, errors.New("args.GasPrice higher than 2^256-1")
				}
			}
			gasTipCap = new(uint256.Int)
			if args.MaxPriorityFeePerGas != nil {
				overflow := gasTipCap.SetFromBig(args.MaxPriorityFeePerGas.ToInt())
				if overflow {
					return types.Message{}, errors.New("args.GasPrice higher than 2^256-1")
				}
			}
			// Backfill the legacy gasPrice for EVM execution, unless we're all zeroes
			gasPrice = new(uint256.Int)
			if !gasFeeCap.IsZero() || !gasTipCap.IsZero() {
				gasPrice = math.U256Min(new(uint256.Int).Add(gasTipCap, baseFee), gasFeeCap)
			}
		}
		if args.MaxFeePerBlobGas != nil {
			blobFee, overflow := uint256.FromBig(args.MaxFeePerBlobGas.ToInt())
			if overflow {
				return types.Message{}, errors.New("args.MaxFeePerBlobGas higher than 2^256-1")
			}
			maxFeePerBlobGas = blobFee
		}
	}

	value := new(uint256.Int)
	if args.Value != nil {
		overflow := value.SetFromBig(args.Value.ToInt())
		if overflow {
			return types.Message{}, errors.New("args.Value higher than 2^256-1")
		}
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

	msg := types.NewMessage(addr, args.To, 0, value, gas, gasPrice, gasFeeCap, gasTipCap, data, accessList, false /* checkNonce */, false /* isFree */, maxFeePerBlobGas)
	return msg, nil
}

// account indicates the overriding fields of account during the execution of
// a message call.
// Note, state and stateDiff can't be specified at the same time. If state is
// set, message execution will only use the data in the given state. Otherwise
// if statDiff is set, all diff will be applied first and then execute the call
// message.
type Account struct {
	Nonce     *hexutil.Uint64                    `json:"nonce"`
	Code      *hexutility.Bytes                  `json:"code"`
	Balance   **hexutil.Big                      `json:"balance"`
	State     *map[libcommon.Hash]libcommon.Hash `json:"state"`
	StateDiff *map[libcommon.Hash]libcommon.Hash `json:"stateDiff"`
}

func NewRevertError(result *evmtypes.ExecutionResult) *RevertError {
	reason, errUnpack := abi.UnpackRevert(result.Revert())
	err := errors.New("execution reverted")
	if errUnpack == nil {
		err = fmt.Errorf("execution reverted: %v", reason)
	}
	return &RevertError{
		error:  err,
		reason: hexutility.Encode(result.Revert()),
	}
}

// RevertError is an API error that encompassas an EVM revertal with JSON error
// code and a binary data blob.
type RevertError struct {
	error
	reason string // revert reason hex encoded
}

// ErrorCode returns the JSON error code for a revertal.
// See: https://github.com/ethereum/wiki/wiki/JSON-RPC-Error-Codes-Improvement-Proposal
func (e *RevertError) ErrorCode() int {
	return 3
}

// ErrorData returns the hex encoded revert reason.
func (e *RevertError) ErrorData() interface{} {
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
type StructLogRes struct {
	Pc      uint64             `json:"pc"`
	Op      string             `json:"op"`
	Gas     uint64             `json:"gas"`
	GasCost uint64             `json:"gasCost"`
	Depth   int                `json:"depth"`
	Error   error              `json:"error,omitempty"`
	Stack   *[]string          `json:"stack,omitempty"`
	Memory  *[]string          `json:"memory,omitempty"`
	Storage *map[string]string `json:"storage,omitempty"`
}

// FormatLogs formats EVM returned structured logs for json output
func FormatLogs(logs []logger.StructLog) []StructLogRes {
	formatted := make([]StructLogRes, len(logs))
	for index, trace := range logs {
		formatted[index] = StructLogRes{
			Pc:      trace.Pc,
			Op:      trace.Op.String(),
			Gas:     trace.Gas,
			GasCost: trace.GasCost,
			Depth:   trace.Depth,
			Error:   trace.Err,
		}
		if trace.Stack != nil {
			stack := make([]string, len(trace.Stack))
			for i, stackValue := range trace.Stack {
				stack[i] = hex.EncodeToString(math.PaddedBigBytes(stackValue, 32))
			}
			formatted[index].Stack = &stack
		}
		if trace.Memory != nil {
			memory := make([]string, 0, (len(trace.Memory)+31)/32)
			for i := 0; i+32 <= len(trace.Memory); i += 32 {
				memory = append(memory, hex.EncodeToString(trace.Memory[i:i+32]))
			}
			formatted[index].Memory = &memory
		}
		if trace.Storage != nil {
			storage := make(map[string]string)
			for i, storageValue := range trace.Storage {
				storage[fmt.Sprintf("%x", i)] = fmt.Sprintf("%x", storageValue)
			}
			formatted[index].Storage = &storage
		}
	}
	return formatted
}

// RPCMarshalHeader converts the given header to the RPC output .
func RPCMarshalHeader(head *types.Header) map[string]interface{} {
	result := map[string]interface{}{
		"number":           (*hexutil.Big)(head.Number),
		"hash":             head.Hash(),
		"parentHash":       head.ParentHash,
		"nonce":            head.Nonce,
		"mixHash":          head.MixDigest,
		"sha3Uncles":       head.UncleHash,
		"logsBloom":        head.Bloom,
		"stateRoot":        head.Root,
		"miner":            head.Coinbase,
		"difficulty":       (*hexutil.Big)(head.Difficulty),
		"extraData":        hexutility.Bytes(head.Extra),
		"size":             hexutil.Uint64(head.Size()),
		"gasLimit":         hexutil.Uint64(head.GasLimit),
		"gasUsed":          hexutil.Uint64(head.GasUsed),
		"timestamp":        hexutil.Uint64(head.Time),
		"transactionsRoot": head.TxHash,
		"receiptsRoot":     head.ReceiptHash,
	}
	if head.BaseFee != nil {
		result["baseFeePerGas"] = (*hexutil.Big)(head.BaseFee)
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

	return result
}

// RPCMarshalBlock converts the given block to the RPC output which depends on fullTx. If inclTx is true transactions are
// returned. When fullTx is true the returned block contains full transaction details, otherwise it will only contain
// transaction hashes.
func RPCMarshalBlockDeprecated(block *types.Block, inclTx bool, fullTx bool) (map[string]interface{}, error) {
	return RPCMarshalBlockExDeprecated(block, inclTx, fullTx, nil, libcommon.Hash{})
}

func RPCMarshalBlockExDeprecated(block *types.Block, inclTx bool, fullTx bool, borTx types.Transaction, borTxHash libcommon.Hash) (map[string]interface{}, error) {
	fields := RPCMarshalHeader(block.Header())
	fields["size"] = hexutil.Uint64(block.Size())
	if _, ok := fields["transactions"]; !ok {
		fields["transactions"] = make([]interface{}, 0)
	}

	if inclTx {
		formatTx := func(tx types.Transaction, index int) (interface{}, error) {
			return tx.Hash(), nil
		}
		if fullTx {
			formatTx = func(tx types.Transaction, index int) (interface{}, error) {
				return newRPCTransactionFromBlockAndTxGivenIndex(block, tx, uint64(index)), nil
			}
		}
		txs := block.Transactions()
		transactions := make([]interface{}, len(txs), len(txs)+1)
		var err error
		for i, txn := range txs {
			if transactions[i], err = formatTx(txn, i); err != nil {
				return nil, err
			}
		}

		if borTx != nil {
			if fullTx {
				transactions = append(transactions, NewRPCBorTransaction(borTx, borTxHash, block.Hash(), block.NumberU64(), uint64(len(txs)), nil /* chainID */))
			} else {
				transactions = append(transactions, borTxHash)
			}
		}

		fields["transactions"] = transactions
	}
	uncles := block.Uncles()
	uncleHashes := make([]libcommon.Hash, len(uncles))
	for i, uncle := range uncles {
		uncleHashes[i] = uncle.Hash()
	}
	fields["uncles"] = uncleHashes

	if block.Withdrawals() != nil {
		fields["withdrawals"] = block.Withdrawals()
	}

	return fields, nil
}

// RPCTransaction represents a transaction that will serialize to the RPC representation of a transaction
type RPCTransaction struct {
	BlockHash           *libcommon.Hash            `json:"blockHash"`
	BlockNumber         *hexutil.Big               `json:"blockNumber"`
	From                libcommon.Address          `json:"from"`
	Gas                 hexutil.Uint64             `json:"gas"`
	GasPrice            *hexutil.Big               `json:"gasPrice,omitempty"`
	Tip                 *hexutil.Big               `json:"maxPriorityFeePerGas,omitempty"`
	FeeCap              *hexutil.Big               `json:"maxFeePerGas,omitempty"`
	Hash                libcommon.Hash             `json:"hash"`
	Input               hexutility.Bytes           `json:"input"`
	Nonce               hexutil.Uint64             `json:"nonce"`
	To                  *libcommon.Address         `json:"to"`
	TransactionIndex    *hexutil.Uint64            `json:"transactionIndex"`
	Value               *hexutil.Big               `json:"value"`
	Type                hexutil.Uint64             `json:"type"`
	Accesses            *types.AccessList          `json:"accessList,omitempty"`
	ChainID             *hexutil.Big               `json:"chainId,omitempty"`
	MaxFeePerBlobGas    *hexutil.Big               `json:"maxFeePerBlobGas,omitempty"`
	BlobVersionedHashes []libcommon.Hash           `json:"blobVersionedHashes,omitempty"`
	Authorizations      *[]types.JsonAuthorization `json:"authorizationList,omitempty"`
	V                   *hexutil.Big               `json:"v"`
	YParity             *hexutil.Big               `json:"yParity,omitempty"`
	R                   *hexutil.Big               `json:"r"`
	S                   *hexutil.Big               `json:"s"`
}

// NewRPCTransaction returns a transaction that will serialize to the RPC
// representation, with the given location metadata set (if available).
func NewRPCTransaction(txn types.Transaction, blockHash libcommon.Hash, blockNumber uint64, index uint64, baseFee *big.Int) *RPCTransaction {
	// Determine the signer. For replay-protected transactions, use the most permissive
	// signer, because we assume that signers are backwards-compatible with old
	// transactions. For non-protected transactions, the homestead signer is used
	// because the return value of ChainId is zero for those transactions.
	chainId := uint256.NewInt(0)
	result := &RPCTransaction{
		Type:  hexutil.Uint64(txn.Type()),
		Gas:   hexutil.Uint64(txn.GetGas()),
		Hash:  txn.Hash(),
		Input: hexutility.Bytes(txn.GetData()),
		Nonce: hexutil.Uint64(txn.GetNonce()),
		To:    txn.GetTo(),
		Value: (*hexutil.Big)(txn.GetValue().ToBig()),
	}
	if t, ok := txn.(*types.BlobTxWrapper); ok {
		txn = &t.Tx
	}

	v, r, s := txn.RawSignatureValues()
	result.V = (*hexutil.Big)(v.ToBig())
	result.R = (*hexutil.Big)(r.ToBig())
	result.S = (*hexutil.Big)(s.ToBig())

	if txn.Type() == types.LegacyTxType {
		chainId = types.DeriveChainId(v)
		// if a legacy transaction has an EIP-155 chain id, include it explicitly, otherwise chain id is not included
		if !chainId.IsZero() {
			result.ChainID = (*hexutil.Big)(chainId.ToBig())
		}
		result.GasPrice = (*hexutil.Big)(txn.GetPrice().ToBig())
	} else {
		chainId.Set(txn.GetChainID())
		result.ChainID = (*hexutil.Big)(chainId.ToBig())
		result.YParity = (*hexutil.Big)(v.ToBig())
		acl := txn.GetAccessList()
		result.Accesses = &acl

		if txn.Type() == types.AccessListTxType {
			result.GasPrice = (*hexutil.Big)(txn.GetPrice().ToBig())
		} else {
			result.GasPrice = computeGasPrice(txn, blockHash, baseFee)
			result.Tip = (*hexutil.Big)(txn.GetTip().ToBig())
			result.FeeCap = (*hexutil.Big)(txn.GetFeeCap().ToBig())
		}

		if txn.Type() == types.BlobTxType {
			txn.GetBlobGas()
			blobTx := txn.(*types.BlobTx)
			result.MaxFeePerBlobGas = (*hexutil.Big)(blobTx.MaxFeePerBlobGas.ToBig())
			result.BlobVersionedHashes = blobTx.BlobVersionedHashes
		} else if txn.Type() == types.SetCodeTxType {
			setCodeTx := txn.(*types.SetCodeTransaction)
			ats := make([]types.JsonAuthorization, len(setCodeTx.GetAuthorizations()))
			for i, a := range setCodeTx.GetAuthorizations() {
				ats[i] = types.JsonAuthorization{}.FromAuthorization(a)
			}
			result.Authorizations = &ats
		}
	}

	signer := types.LatestSignerForChainID(chainId.ToBig())
	var err error
	result.From, err = txn.Sender(*signer)
	if err != nil {
		log.Warn("sender recovery", "err", err)
	}
	if blockHash != (libcommon.Hash{}) {
		result.BlockHash = &blockHash
		result.BlockNumber = (*hexutil.Big)(new(big.Int).SetUint64(blockNumber))
		result.TransactionIndex = (*hexutil.Uint64)(&index)
	}
	return result
}

func computeGasPrice(txn types.Transaction, blockHash libcommon.Hash, baseFee *big.Int) *hexutil.Big {
	fee, overflow := uint256.FromBig(baseFee)
	if fee != nil && !overflow && blockHash != (libcommon.Hash{}) {
		// price = min(tip + baseFee, gasFeeCap)
		price := math.Min256(new(uint256.Int).Add(txn.GetTip(), fee), txn.GetFeeCap())
		return (*hexutil.Big)(price.ToBig())
	}
	return nil
}

// NewRPCBorTransaction returns a Bor transaction that will serialize to the RPC
// representation, with the given location metadata set (if available).
func NewRPCBorTransaction(opaqueTxn types.Transaction, txHash libcommon.Hash, blockHash libcommon.Hash, blockNumber uint64, index uint64, chainId *big.Int) *RPCTransaction {
	txn := opaqueTxn.(*types.LegacyTx)
	result := &RPCTransaction{
		Type:     hexutil.Uint64(txn.Type()),
		ChainID:  (*hexutil.Big)(new(big.Int)),
		GasPrice: (*hexutil.Big)(txn.GasPrice.ToBig()),
		Gas:      hexutil.Uint64(txn.GetGas()),
		Hash:     txHash,
		Input:    hexutility.Bytes(txn.GetData()),
		Nonce:    hexutil.Uint64(txn.GetNonce()),
		From:     libcommon.Address{},
		To:       txn.GetTo(),
		Value:    (*hexutil.Big)(txn.GetValue().ToBig()),
		V:        (*hexutil.Big)(big.NewInt(0)),
		R:        (*hexutil.Big)(big.NewInt(0)),
		S:        (*hexutil.Big)(big.NewInt(0)),
	}
	if blockHash != (libcommon.Hash{}) {
		result.ChainID = (*hexutil.Big)(chainId)
		result.BlockHash = &blockHash
		result.BlockNumber = (*hexutil.Big)(new(big.Int).SetUint64(blockNumber))
		result.TransactionIndex = (*hexutil.Uint64)(&index)
	}
	return result
}

// newRPCTransactionFromBlockAndTxGivenIndex returns a transaction that will serialize to the RPC representation.
func newRPCTransactionFromBlockAndTxGivenIndex(b *types.Block, txn types.Transaction, index uint64) *RPCTransaction {
	return NewRPCTransaction(txn, b.Hash(), b.NumberU64(), index, b.BaseFee())
}
