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

	"github.com/erigontech/erigon-lib/abi"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/eth/tracers/logger"
)

// CallArgs represents the arguments for a call.
type CallArgs struct {
	From                 *common.Address           `json:"from"`
	To                   *common.Address           `json:"to"`
	Gas                  *hexutil.Uint64           `json:"gas"`
	GasPrice             *hexutil.Big              `json:"gasPrice"`
	MaxPriorityFeePerGas *hexutil.Big              `json:"maxPriorityFeePerGas"`
	MaxFeePerGas         *hexutil.Big              `json:"maxFeePerGas"`
	MaxFeePerBlobGas     *hexutil.Big              `json:"maxFeePerBlobGas"`
	Value                *hexutil.Big              `json:"value"`
	Nonce                *hexutil.Uint64           `json:"nonce"`
	Data                 *hexutil.Bytes            `json:"data"`
	Input                *hexutil.Bytes            `json:"input"`
	AccessList           *types.AccessList         `json:"accessList"`
	ChainID              *hexutil.Big              `json:"chainId,omitempty"`
	BlobVersionedHashes  []common.Hash             `json:"blobVersionedHashes,omitempty"`
	AuthorizationList    []types.JsonAuthorization `json:"authorizationList"`

	SkipL1Charging *bool `json:"skipL1Charging"` // Arbitrum
}

// from retrieves the transaction sender address.
func (args *CallArgs) from() common.Address {
	if args.From == nil {
		return common.Address{}
	}
	return *args.From
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
			overflow := gasPrice.SetFromBig(args.GasPrice.ToInt())
			if overflow {
				return nil, errors.New("args.GasPrice higher than 2^256-1")
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
				return nil, errors.New("args.GasPrice higher than 2^256-1")
			}
			gasFeeCap, gasTipCap = gasPrice, gasPrice
		} else {
			// User specified 1559 gas fields (or none), use those
			gasFeeCap = new(uint256.Int)
			if args.MaxFeePerGas != nil {
				overflow := gasFeeCap.SetFromBig(args.MaxFeePerGas.ToInt())
				if overflow {
					return nil, errors.New("args.GasPrice higher than 2^256-1")
				}
			}
			gasTipCap = new(uint256.Int)
			if args.MaxPriorityFeePerGas != nil {
				overflow := gasTipCap.SetFromBig(args.MaxPriorityFeePerGas.ToInt())
				if overflow {
					return nil, errors.New("args.GasPrice higher than 2^256-1")
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
				return nil, errors.New("args.MaxFeePerBlobGas higher than 2^256-1")
			}
			maxFeePerBlobGas = blobFee
		}
	}

	value := new(uint256.Int)
	if args.Value != nil {
		overflow := value.SetFromBig(args.Value.ToInt())
		if overflow {
			return nil, errors.New("args.Value higher than 2^256-1")
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

	if args.BlobVersionedHashes != nil {
		msg.SetBlobVersionedHashes(args.BlobVersionedHashes)
	}

	if args.AuthorizationList != nil {
		authorizations := make([]types.Authorization, len(args.AuthorizationList))
		for i, auth := range args.AuthorizationList {
			var err error
			authorizations[i], err = auth.ToAuthorization()
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
	chainID, overflow := uint256.FromBig((*big.Int)(args.ChainID))
	if overflow {
		return nil, errors.New("chainId field caused an overflow (uint256)")
	}

	msg, err := args.ToMessage(globalGasCap, baseFee)
	if err != nil {
		return nil, err
	}

	var tx types.Transaction
	switch {
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
				Value:    msg.Value(),
				Data:     msg.Data(),
			},
			ChainID:    chainID,
			FeeCap:     msg.FeeCap(),
			TipCap:     msg.TipCap(),
			AccessList: al,
		}
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
					Value:    msg.Value(),
					Data:     msg.Data(),
				},
				GasPrice: msg.GasPrice(),
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
				Value:    msg.Value(),
				Data:     msg.Data(),
			},
			GasPrice: msg.GasPrice(),
		}
	}
	return tx, nil
}

// Arbiturm
// Raises the vanilla gas cap by the tx's l1 data costs in l2 terms. This creates a new gas cap that after
// data payments are made, equals the original vanilla cap for the remaining, L2-specific work the tx does.
func (args *CallArgs) L2OnlyGasCap(gasCap uint64, header *types.Header) (uint64, error) {
	msg, err := args.ToMessage(gasCap, nil)
	if err != nil {
		return 0, err
	}
	InterceptRPCGasCap(&gasCap, msg, header)
	return gasCap, nil
}

// Allows ArbOS to update the gas cap so that it ignores the message's specific L1 poster costs.
var InterceptRPCGasCap = func(gascap *uint64, msg *types.Message, header *types.Header) {}

// End arbitrum

// Account indicates the overriding fields of account during the execution of
// a message call.
// Note, state and stateDiff can't be specified at the same time. If state is
// set, message execution will only use the data in the given state. Otherwise
// if statDiff is set, all diff will be applied first and then execute the call
// message.
type Account struct {
	Nonce     *hexutil.Uint64              `json:"nonce"`
	Code      *hexutil.Bytes               `json:"code"`
	Balance   **hexutil.Big                `json:"balance"`
	State     *map[common.Hash]common.Hash `json:"state"`
	StateDiff *map[common.Hash]common.Hash `json:"stateDiff"`
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
		"extraData":        hexutil.Bytes(head.Extra),
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
func RPCMarshalBlockDeprecated(block *types.Block, inclTx bool, fullTx bool, isArbitrumNitro bool) (map[string]interface{}, error) {
	return RPCMarshalBlockExDeprecated(block, inclTx, fullTx, nil, common.Hash{}, isArbitrumNitro)
}

func RPCMarshalBlockExDeprecated(block *types.Block, inclTx bool, fullTx bool, borTx types.Transaction, borTxHash common.Hash, isArbitrumNitro bool) (map[string]interface{}, error) {
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
	uncleHashes := make([]common.Hash, len(uncles))
	for i, uncle := range uncles {
		uncleHashes[i] = uncle.Hash()
	}
	fields["uncles"] = uncleHashes

	if block.Withdrawals() != nil {
		fields["withdrawals"] = block.Withdrawals()
	}
	if isArbitrumNitro {
		fillArbitrumHeaderInfo(block.Header(), fields)
	}
	return fields, nil
}

func fillArbitrumHeaderInfo(header *types.Header, fields map[string]interface{}) {
	info := types.DeserializeHeaderExtraInformation(header)
	fields["l1BlockNumber"] = hexutil.Uint64(info.L1BlockNumber)
	fields["sendRoot"] = info.SendRoot
	fields["sendCount"] = hexutil.Uint64(info.SendCount)
}

// RPCTransaction represents a transaction that will serialize to the RPC representation of a transaction
type RPCTransaction struct {
	BlockHash            *common.Hash               `json:"blockHash"`
	BlockNumber          *hexutil.Big               `json:"blockNumber"`
	From                 common.Address             `json:"from"`
	Gas                  hexutil.Uint64             `json:"gas"`
	GasPrice             *hexutil.Big               `json:"gasPrice,omitempty"`
	MaxPriorityFeePerGas *hexutil.Big               `json:"maxPriorityFeePerGas,omitempty"`
	MaxFeePerGas         *hexutil.Big               `json:"maxFeePerGas,omitempty"`
	Hash                 common.Hash                `json:"hash"`
	Input                hexutil.Bytes              `json:"input"`
	Nonce                hexutil.Uint64             `json:"nonce"`
	To                   *common.Address            `json:"to"`
	TransactionIndex     *hexutil.Uint64            `json:"transactionIndex"`
	Value                *hexutil.Big               `json:"value"`
	Type                 hexutil.Uint64             `json:"type"`
	Accesses             *types.AccessList          `json:"accessList,omitempty"`
	ChainID              *hexutil.Big               `json:"chainId,omitempty"`
	MaxFeePerBlobGas     *hexutil.Big               `json:"maxFeePerBlobGas,omitempty"`
	BlobVersionedHashes  []common.Hash              `json:"blobVersionedHashes,omitempty"`
	Authorizations       *[]types.JsonAuthorization `json:"authorizationList,omitempty"`
	V                    *hexutil.Big               `json:"v"`
	YParity              *hexutil.Big               `json:"yParity,omitempty"`
	R                    *hexutil.Big               `json:"r"`
	S                    *hexutil.Big               `json:"s"`

	// Arbitrum fields:
	RequestId           *common.Hash    `json:"requestId,omitempty"`           // Contract SubmitRetryable Deposit
	TicketId            *common.Hash    `json:"ticketId,omitempty"`            // Retry
	MaxRefund           *hexutil.Big    `json:"maxRefund,omitempty"`           // Retry
	SubmissionFeeRefund *hexutil.Big    `json:"submissionFeeRefund,omitempty"` // Retry
	RefundTo            *common.Address `json:"refundTo,omitempty"`            // SubmitRetryable Retry
	L1BaseFee           *hexutil.Big    `json:"l1BaseFee,omitempty"`           // SubmitRetryable
	DepositValue        *hexutil.Big    `json:"depositValue,omitempty"`        // SubmitRetryable
	RetryTo             *common.Address `json:"retryTo,omitempty"`             // SubmitRetryable
	RetryValue          *hexutil.Big    `json:"retryValue,omitempty"`          // SubmitRetryable
	RetryData           *hexutil.Bytes  `json:"retryData,omitempty"`           // SubmitRetryable
	Beneficiary         *common.Address `json:"beneficiary,omitempty"`         // SubmitRetryable
	MaxSubmissionFee    *hexutil.Big    `json:"maxSubmissionFee,omitempty"`    // SubmitRetryable
}

// NewRPCTransaction returns a transaction that will serialize to the RPC
// representation, with the given location metadata set (if available).
func NewRPCTransaction(txn types.Transaction, blockHash common.Hash, blockNumber uint64, index uint64, baseFee *big.Int) *RPCTransaction {
	// Determine the signer. For replay-protected transactions, use the most permissive
	// signer, because we assume that signers are backwards-compatible with old
	// transactions. For non-protected transactions, the homestead signer is used
	// because the return value of ChainId is zero for those transactions.
	chainId := uint256.NewInt(0)
	result := &RPCTransaction{
		Type:  hexutil.Uint64(txn.Type()),
		Gas:   hexutil.Uint64(txn.GetGasLimit()),
		Hash:  txn.Hash(),
		Input: hexutil.Bytes(txn.GetData()),
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

	var signed bool
	if txn.Type() == types.LegacyTxType {
		chainId = types.DeriveChainId(v)
		// if a legacy transaction has an EIP-155 chain id, include it explicitly, otherwise chain id is not included
		if !chainId.IsZero() {
			result.ChainID = (*hexutil.Big)(chainId.ToBig())
		}
		result.GasPrice = (*hexutil.Big)(txn.GetTipCap().ToBig())
	} else {
		chainId.Set(txn.GetChainID())
		result.ChainID = (*hexutil.Big)(chainId.ToBig())
		result.YParity = (*hexutil.Big)(v.ToBig())
		acl := txn.GetAccessList()
		result.Accesses = &acl

		if txn.Type() == types.AccessListTxType {
			result.GasPrice = (*hexutil.Big)(txn.GetTipCap().ToBig())
		} else {
			result.GasPrice = computeGasPrice(txn, blockHash, baseFee)
			result.MaxPriorityFeePerGas = (*hexutil.Big)(txn.GetTipCap().ToBig())
			result.MaxFeePerGas = (*hexutil.Big)(txn.GetFeeCap().ToBig())
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

		switch tx := txn.(type) {
		case *types.ArbitrumInternalTx:
			result.GasPrice = (*hexutil.Big)(tx.GetPrice().ToBig())
		case *types.ArbitrumDepositTx:
			result.GasPrice = (*hexutil.Big)(tx.GetPrice().ToBig())
			result.RequestId = &tx.L1RequestId
		case *types.ArbitrumContractTx:
			result.GasPrice = (*hexutil.Big)(tx.GasFeeCap)
			result.RequestId = &tx.RequestId
			result.MaxFeePerGas = (*hexutil.Big)(tx.GasFeeCap)
		case *types.ArbitrumRetryTx:
			result.GasPrice = (*hexutil.Big)(tx.GasFeeCap)
			result.TicketId = &tx.TicketId
			result.RefundTo = &tx.RefundTo
			result.MaxFeePerGas = (*hexutil.Big)(tx.GasFeeCap)
			result.MaxRefund = (*hexutil.Big)(tx.MaxRefund)
			result.SubmissionFeeRefund = (*hexutil.Big)(tx.SubmissionFeeRefund)
		case *types.ArbitrumSubmitRetryableTx:
			result.GasPrice = (*hexutil.Big)(tx.GasFeeCap)
			result.RequestId = &tx.RequestId
			result.L1BaseFee = (*hexutil.Big)(tx.L1BaseFee)
			result.DepositValue = (*hexutil.Big)(tx.DepositValue)
			result.RetryTo = tx.RetryTo
			result.RetryValue = (*hexutil.Big)(tx.RetryValue)
			result.RetryData = (*hexutil.Bytes)(&tx.RetryData)
			result.Beneficiary = &tx.Beneficiary
			result.RefundTo = &tx.FeeRefundAddr
			result.MaxSubmissionFee = (*hexutil.Big)(tx.MaxSubmissionFee)
			result.MaxFeePerGas = (*hexutil.Big)(tx.GasFeeCap)
		case *types.ArbitrumUnsignedTx:
			result.GasPrice = (*hexutil.Big)(tx.GasFeeCap)
		}
		signer := types.NewArbitrumSigner(*types.LatestSignerForChainID(chainId.ToBig()))
		var err error
		result.From, err = signer.Sender(txn)
		if err != nil {
			log.Warn("sender recovery", "err", err)
		}
		signed = true
	}

	if !signed {
		signer := types.LatestSignerForChainID(chainId.ToBig())
		var err error
		result.From, err = txn.Sender(*signer)
		if err != nil {
			log.Warn("sender recovery", "err", err)
		}
	}
	if blockHash != (common.Hash{}) {
		result.BlockHash = &blockHash
		result.BlockNumber = (*hexutil.Big)(new(big.Int).SetUint64(blockNumber))
		result.TransactionIndex = (*hexutil.Uint64)(&index)
	}
	return result
}

func computeGasPrice(txn types.Transaction, blockHash common.Hash, baseFee *big.Int) *hexutil.Big {
	fee, overflow := uint256.FromBig(baseFee)
	if fee != nil && !overflow && blockHash != (common.Hash{}) {
		// price = min(tip + baseFee, gasFeeCap)
		price := math.U256Min(new(uint256.Int).Add(txn.GetTipCap(), fee), txn.GetFeeCap())
		return (*hexutil.Big)(price.ToBig())
	}
	return nil
}

// NewRPCBorTransaction returns a Bor transaction that will serialize to the RPC
// representation, with the given location metadata set (if available).
func NewRPCBorTransaction(opaqueTxn types.Transaction, txHash common.Hash, blockHash common.Hash, blockNumber uint64, index uint64, chainId *big.Int) *RPCTransaction {
	txn := opaqueTxn.(*types.LegacyTx)
	result := &RPCTransaction{
		Type:     hexutil.Uint64(txn.Type()),
		ChainID:  (*hexutil.Big)(new(big.Int)),
		GasPrice: (*hexutil.Big)(txn.GasPrice.ToBig()),
		Gas:      hexutil.Uint64(txn.GetGasLimit()),
		Hash:     txHash,
		Input:    hexutil.Bytes(txn.GetData()),
		Nonce:    hexutil.Uint64(txn.GetNonce()),
		From:     common.Address{},
		To:       txn.GetTo(),
		Value:    (*hexutil.Big)(txn.GetValue().ToBig()),
		V:        (*hexutil.Big)(big.NewInt(0)),
		R:        (*hexutil.Big)(big.NewInt(0)),
		S:        (*hexutil.Big)(big.NewInt(0)),
	}
	if blockHash != (common.Hash{}) {
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
