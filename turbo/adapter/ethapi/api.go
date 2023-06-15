// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package ethapi

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	types2 "github.com/ledgerwatch/erigon-lib/types"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/accounts/abi"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/tracers/logger"
)

// CallArgs represents the arguments for a call.
type CallArgs struct {
	From                 *libcommon.Address `json:"from"`
	To                   *libcommon.Address `json:"to"`
	Gas                  *hexutil.Uint64    `json:"gas"`
	GasPrice             *hexutil.Big       `json:"gasPrice"`
	MaxPriorityFeePerGas *hexutil.Big       `json:"maxPriorityFeePerGas"`
	MaxFeePerGas         *hexutil.Big       `json:"maxFeePerGas"`
	MaxFeePerDataGas     *hexutil.Big       `json:"maxFeePerDataGas"`
	Value                *hexutil.Big       `json:"value"`
	Nonce                *hexutil.Uint64    `json:"nonce"`
	Data                 *hexutility.Bytes  `json:"data"`
	AccessList           *types2.AccessList `json:"accessList"`
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
		maxFeePerDataGas *uint256.Int
	)
	if baseFee == nil {
		// If there's no basefee, then it must be a non-1559 execution
		gasPrice = new(uint256.Int)
		if args.GasPrice != nil {
			overflow := gasPrice.SetFromBig(args.GasPrice.ToInt())
			if overflow {
				return types.Message{}, fmt.Errorf("args.GasPrice higher than 2^256-1")
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
				return types.Message{}, fmt.Errorf("args.GasPrice higher than 2^256-1")
			}
			gasFeeCap, gasTipCap = gasPrice, gasPrice
		} else {
			// User specified 1559 gas feilds (or none), use those
			gasFeeCap = new(uint256.Int)
			if args.MaxFeePerGas != nil {
				overflow := gasFeeCap.SetFromBig(args.MaxFeePerGas.ToInt())
				if overflow {
					return types.Message{}, fmt.Errorf("args.GasPrice higher than 2^256-1")
				}
			}
			gasTipCap = new(uint256.Int)
			if args.MaxPriorityFeePerGas != nil {
				overflow := gasTipCap.SetFromBig(args.MaxPriorityFeePerGas.ToInt())
				if overflow {
					return types.Message{}, fmt.Errorf("args.GasPrice higher than 2^256-1")
				}
			}
			// Backfill the legacy gasPrice for EVM execution, unless we're all zeroes
			gasPrice = new(uint256.Int)
			if !gasFeeCap.IsZero() || !gasTipCap.IsZero() {
				gasPrice = math.U256Min(new(uint256.Int).Add(gasTipCap, baseFee), gasFeeCap)
			}
		}
		if args.MaxFeePerDataGas != nil {
			maxFeePerDataGas.SetFromBig(args.MaxFeePerDataGas.ToInt())
		}
	}

	value := new(uint256.Int)
	if args.Value != nil {
		overflow := value.SetFromBig(args.Value.ToInt())
		if overflow {
			return types.Message{}, fmt.Errorf("args.Value higher than 2^256-1")
		}
	}
	var data []byte
	if args.Data != nil {
		data = *args.Data
	}
	var accessList types2.AccessList
	if args.AccessList != nil {
		accessList = *args.AccessList
	}

	msg := types.NewMessage(addr, args.To, 0, value, gas, gasPrice, gasFeeCap, gasTipCap, data, accessList, false /* checkNonce */, false /* isFree */, maxFeePerDataGas)
	return msg, nil
}

// account indicates the overriding fields of account during the execution of
// a message call.
// Note, state and stateDiff can't be specified at the same time. If state is
// set, message execution will only use the data in the given state. Otherwise
// if statDiff is set, all diff will be applied first and then execute the call
// message.
type Account struct {
	Nonce     *hexutil.Uint64                 `json:"nonce"`
	Code      *hexutility.Bytes               `json:"code"`
	Balance   **hexutil.Big                   `json:"balance"`
	State     *map[libcommon.Hash]uint256.Int `json:"state"`
	StateDiff *map[libcommon.Hash]uint256.Int `json:"stateDiff"`
}

func NewRevertError(result *core.ExecutionResult) *RevertError {
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
				stack[i] = fmt.Sprintf("%x", math.PaddedBigBytes(stackValue, 32))
			}
			formatted[index].Stack = &stack
		}
		if trace.Memory != nil {
			memory := make([]string, 0, (len(trace.Memory)+31)/32)
			for i := 0; i+32 <= len(trace.Memory); i += 32 {
				memory = append(memory, fmt.Sprintf("%x", trace.Memory[i:i+32]))
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
	if head.DataGasUsed != nil {
		result["dataGasUsed"] = (*hexutil.Uint64)(head.DataGasUsed)
	}
	if head.ExcessDataGas != nil {
		result["excessDataGas"] = (*hexutil.Uint64)(head.ExcessDataGas)
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
		for i, tx := range txs {
			if transactions[i], err = formatTx(tx, i); err != nil {
				return nil, err
			}
		}

		if borTx != nil {
			if fullTx {
				transactions = append(transactions, newRPCBorTransaction(borTx, borTxHash, block.Hash(), block.NumberU64(), uint64(len(txs)), block.BaseFee()))
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

/*

// rpcMarshalHeader uses the generalized output filler, then adds the total difficulty field, which requires
// a `PublicBlockchainAPI`.
func (s *PublicBlockChainAPI) rpcMarshalHeader(ctx context.Context, header *types.Header) map[string]interface{} {
	fields := RPCMarshalHeader(header)
	fields["totalDifficulty"] = (*hexutil.Big)(s.b.GetTd(ctx, header.Hash()))
	return fields
}

// rpcMarshalBlock uses the generalized output filler, then adds the total difficulty field, which requires
// a `PublicBlockchainAPI`.
func (s *PublicBlockChainAPI) rpcMarshalBlock(ctx context.Context, b *types.Block, inclTx bool, fullTx bool) (map[string]interface{}, error) {
	fields, err := RPCMarshalBlock(b, inclTx, fullTx)
	if err != nil {
		return nil, err
	}
	if inclTx {
		fields["totalDifficulty"] = (*hexutil.Big)(s.b.GetTd(ctx, b.Hash()))
	}
	return fields, err
}
*/

// RPCTransaction represents a transaction that will serialize to the RPC representation of a transaction
type RPCTransaction struct {
	BlockHash        *libcommon.Hash    `json:"blockHash"`
	BlockNumber      *hexutil.Big       `json:"blockNumber"`
	From             libcommon.Address  `json:"from"`
	Gas              hexutil.Uint64     `json:"gas"`
	GasPrice         *hexutil.Big       `json:"gasPrice,omitempty"`
	Tip              *hexutil.Big       `json:"maxPriorityFeePerGas,omitempty"`
	FeeCap           *hexutil.Big       `json:"maxFeePerGas,omitempty"`
	MaxFeePerDataGas *hexutil.Big       `json:"maxFeePerDataGas,omitempty"`
	Hash             libcommon.Hash     `json:"hash"`
	Input            hexutility.Bytes   `json:"input"`
	Nonce            hexutil.Uint64     `json:"nonce"`
	To               *libcommon.Address `json:"to"`
	TransactionIndex *hexutil.Uint64    `json:"transactionIndex"`
	Value            *hexutil.Big       `json:"value"`
	Type             hexutil.Uint64     `json:"type"`
	Accesses         *types2.AccessList `json:"accessList,omitempty"`
	ChainID          *hexutil.Big       `json:"chainId,omitempty"`
	V                *hexutil.Big       `json:"v"`
	R                *hexutil.Big       `json:"r"`
	S                *hexutil.Big       `json:"s"`

	BlobVersionedHashes []libcommon.Hash `json:"blobVersionedHashes,omitempty"`
}

// newRPCTransaction returns a transaction that will serialize to the RPC
// representation, with the given location metadata set (if available).
func newRPCTransaction(tx types.Transaction, blockHash libcommon.Hash, blockNumber uint64, index uint64, baseFee *big.Int) *RPCTransaction {
	// Determine the signer. For replay-protected transactions, use the most permissive
	// signer, because we assume that signers are backwards-compatible with old
	// transactions. For non-protected transactions, the homestead signer signer is used
	// because the return value of ChainId is zero for those transactions.
	chainId := uint256.NewInt(0)
	result := &RPCTransaction{
		Type:  hexutil.Uint64(tx.Type()),
		Gas:   hexutil.Uint64(tx.GetGas()),
		Hash:  tx.Hash(),
		Input: hexutility.Bytes(tx.GetData()),
		Nonce: hexutil.Uint64(tx.GetNonce()),
		To:    tx.GetTo(),
		Value: (*hexutil.Big)(tx.GetValue().ToBig()),
	}
	switch t := tx.(type) {
	case *types.LegacyTx:
		chainId = types.DeriveChainId(&t.V)
		// if a legacy transaction has an EIP-155 chain id, include it explicitly, otherwise chain id is not included
		if !chainId.IsZero() {
			result.ChainID = (*hexutil.Big)(chainId.ToBig())
		}
		result.GasPrice = (*hexutil.Big)(t.GasPrice.ToBig())
		result.V = (*hexutil.Big)(t.V.ToBig())
		result.R = (*hexutil.Big)(t.R.ToBig())
		result.S = (*hexutil.Big)(t.S.ToBig())
	case *types.AccessListTx:
		chainId.Set(t.ChainID)
		result.ChainID = (*hexutil.Big)(chainId.ToBig())
		result.GasPrice = (*hexutil.Big)(t.GasPrice.ToBig())
		result.V = (*hexutil.Big)(t.V.ToBig())
		result.R = (*hexutil.Big)(t.R.ToBig())
		result.S = (*hexutil.Big)(t.S.ToBig())
		result.Accesses = &t.AccessList
	case *types.DynamicFeeTransaction:
		chainId.Set(t.ChainID)
		result.ChainID = (*hexutil.Big)(chainId.ToBig())
		result.Tip = (*hexutil.Big)(t.Tip.ToBig())
		result.FeeCap = (*hexutil.Big)(t.FeeCap.ToBig())
		result.V = (*hexutil.Big)(t.V.ToBig())
		result.R = (*hexutil.Big)(t.R.ToBig())
		result.S = (*hexutil.Big)(t.S.ToBig())
		result.Accesses = &t.AccessList
		// if the transaction has been mined, compute the effective gas price
		result.GasPrice = computeGasPrice(tx, blockHash, baseFee)
	case *types.SignedBlobTx:
		chainId.Set(t.GetChainID())
		result.ChainID = (*hexutil.Big)(chainId.ToBig())
		result.Tip = (*hexutil.Big)(t.GetTip().ToBig())
		result.FeeCap = (*hexutil.Big)(t.GetFeeCap().ToBig())
		v, r, s := t.RawSignatureValues()
		result.V = (*hexutil.Big)(v.ToBig())
		result.R = (*hexutil.Big)(r.ToBig())
		result.S = (*hexutil.Big)(s.ToBig())
		al := t.GetAccessList()
		result.Accesses = &al
		// if the transaction has been mined, compute the effective gas price
		result.GasPrice = computeGasPrice(tx, blockHash, baseFee)
		result.MaxFeePerDataGas = (*hexutil.Big)(t.MaxFeePerDataGas.ToBig())
		result.BlobVersionedHashes = t.GetDataHashes()
	}
	signer := types.LatestSignerForChainID(chainId.ToBig())
	var err error
	result.From, err = tx.Sender(*signer)
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

func computeGasPrice(tx types.Transaction, blockHash libcommon.Hash, baseFee *big.Int) *hexutil.Big {
	if baseFee != nil && blockHash != (libcommon.Hash{}) {
		// price = min(tip + baseFee, gasFeeCap)
		price := math.BigMin(new(big.Int).Add(tx.GetTip().ToBig(), baseFee), tx.GetFeeCap().ToBig())
		return (*hexutil.Big)(price)
	}
	return nil
}

// newRPCBorTransaction returns a Bor transaction that will serialize to the RPC
// representation, with the given location metadata set (if available).
func newRPCBorTransaction(opaqueTx types.Transaction, txHash libcommon.Hash, blockHash libcommon.Hash, blockNumber uint64, index uint64, baseFee *big.Int) *RPCTransaction {
	tx := opaqueTx.(*types.LegacyTx)
	result := &RPCTransaction{
		Type:     hexutil.Uint64(tx.Type()),
		ChainID:  (*hexutil.Big)(new(big.Int)),
		GasPrice: (*hexutil.Big)(tx.GasPrice.ToBig()),
		Gas:      hexutil.Uint64(tx.GetGas()),
		Hash:     txHash,
		Input:    hexutility.Bytes(tx.GetData()),
		Nonce:    hexutil.Uint64(tx.GetNonce()),
		From:     libcommon.Address{},
		To:       tx.GetTo(),
		Value:    (*hexutil.Big)(tx.GetValue().ToBig()),
		V:        (*hexutil.Big)(big.NewInt(0)),
		R:        (*hexutil.Big)(big.NewInt(0)),
		S:        (*hexutil.Big)(big.NewInt(0)),
	}
	if blockHash != (libcommon.Hash{}) {
		result.BlockHash = &blockHash
		result.BlockNumber = (*hexutil.Big)(new(big.Int).SetUint64(blockNumber))
		result.TransactionIndex = (*hexutil.Uint64)(&index)
	}
	return result
}

/*
// newRPCPendingTransaction returns a pending transaction that will serialize to the RPC representation
func newRPCPendingTransaction(tx types.Transaction) *RPCTransaction {
	return newRPCTransaction(tx, libcommon.Hash{}, 0, 0)
}
*/

/*
// newRPCTransactionFromBlockIndex returns a transaction that will serialize to the RPC representation.
func newRPCTransactionFromBlockIndex(b *types.Block, index uint64) *RPCTransaction {
	txs := b.Transactions()
	if index >= uint64(len(txs)) {
		return nil
	}
	return newRPCTransaction(txs[index], b.Hash(), b.NumberU64(), index, b.BaseFee())
}
*/

// newRPCTransactionFromBlockAndTxGivenIndex returns a transaction that will serialize to the RPC representation.
func newRPCTransactionFromBlockAndTxGivenIndex(b *types.Block, tx types.Transaction, index uint64) *RPCTransaction {
	return newRPCTransaction(tx, b.Hash(), b.NumberU64(), index, b.BaseFee())
}

/*
// newRPCRawTransactionFromBlockIndex returns the bytes of a transaction given a block and a transaction index.
func newRPCRawTransactionFromBlockIndex(b *types.Block, index uint64) hexutil.Bytes {
	txs := b.Transactions()
	if index >= uint64(len(txs)) {
		return nil
	}
	var blob bytes.Buffer
	if txs[index].Type() != types.LegacyTxType {
		if err := blob.WriteByte(txs[index].Type()); err != nil {
			panic(err)
		}
	}
	if err := rlp.Encode(&blob, txs[index]); err != nil {
		panic(err)
	}
	return blob.Bytes()
}
*/

/*
// newRPCTransactionFromBlockHash returns a transaction that will serialize to the RPC representation.
func newRPCTransactionFromBlockHash(b *types.Block, hash libcommon.Hash) *RPCTransaction {
	for idx, tx := range b.Transactions() {
		if tx.Hash() == hash {
			return newRPCTransactionFromBlockIndex(b, uint64(idx))
		}
	}
	return nil
}
*/

/*
// PublicTransactionPoolAPI exposes methods for the RPC interface
type PublicTransactionPoolAPI struct {
	b         Backend
	nonceLock *AddrLocker
	signer    *types.Signer
}

// NewPublicTransactionPoolAPI creates a new RPC service with methods specific for the transaction pool.
func NewPublicTransactionPoolAPI(b Backend, nonceLock *AddrLocker) *PublicTransactionPoolAPI {
	// The signer used by the API should always be the 'latest' known one because we expect
	// signers to be backwards-compatible with old transactions.
	signer := types.LatestSigner(b.ChainConfig())
	return &PublicTransactionPoolAPI{b, nonceLock, signer}
}

// GetBlockTransactionCountByNumber returns the number of transactions in the block with the given block number.
func (s *PublicTransactionPoolAPI) GetBlockTransactionCountByNumber(ctx context.Context, blockNr rpc.BlockNumber) *hexutil.Uint {
	if block, _ := s.b.BlockByNumber(ctx, blockNr); block != nil {
		n := hexutil.Uint(len(block.Transactions()))
		return &n
	}
	return nil
}

// GetBlockTransactionCountByHash returns the number of transactions in the block with the given hash.
func (s *PublicTransactionPoolAPI) GetBlockTransactionCountByHash(ctx context.Context, blockHash libcommon.Hash) *hexutil.Uint {
	if block, _ := s.b.BlockByHash(ctx, blockHash); block != nil {
		n := hexutil.Uint(len(block.Transactions()))
		return &n
	}
	return nil
}

// GetTransactionByBlockNumberAndIndex returns the transaction for the given block number and index.
func (s *PublicTransactionPoolAPI) GetTransactionByBlockNumberAndIndex(ctx context.Context, blockNr rpc.BlockNumber, index hexutil.Uint) *RPCTransaction {
	if block, _ := s.b.BlockByNumber(ctx, blockNr); block != nil {
		return newRPCTransactionFromBlockIndex(block, uint64(index))
	}
	return nil
}

// GetTransactionByBlockHashAndIndex returns the transaction for the given block hash and index.
func (s *PublicTransactionPoolAPI) GetTransactionByBlockHashAndIndex(ctx context.Context, blockHash libcommon.Hash, index hexutil.Uint) *RPCTransaction {
	if block, _ := s.b.BlockByHash(ctx, blockHash); block != nil {
		return newRPCTransactionFromBlockIndex(block, uint64(index))
	}
	return nil
}

// GetRawTransactionByBlockNumberAndIndex returns the bytes of the transaction for the given block number and index.
func (s *PublicTransactionPoolAPI) GetRawTransactionByBlockNumberAndIndex(ctx context.Context, blockNr rpc.BlockNumber, index hexutil.Uint) hexutil.Bytes {
	if block, _ := s.b.BlockByNumber(ctx, blockNr); block != nil {
		return newRPCRawTransactionFromBlockIndex(block, uint64(index))
	}
	return nil
}

// GetRawTransactionByBlockHashAndIndex returns the bytes of the transaction for the given block hash and index.
func (s *PublicTransactionPoolAPI) GetRawTransactionByBlockHashAndIndex(ctx context.Context, blockHash libcommon.Hash, index hexutil.Uint) hexutil.Bytes {
	if block, _ := s.b.BlockByHash(ctx, blockHash); block != nil {
		return newRPCRawTransactionFromBlockIndex(block, uint64(index))
	}
	return nil
}

// GetTransactionCount returns the number of transactions the given address has sent for the given block number
func (s *PublicTransactionPoolAPI) GetTransactionCount(ctx context.Context, address libcommon.Address, blockNrOrHash rpc.BlockNumberOrHash) (*hexutil.Uint64, error) {
	// Ask transaction pool for the nonce which includes pending transactions
	if blockNr, ok := blockNrOrHash.Number(); ok && blockNr == rpc.PendingBlockNumber {
		nonce, err := s.b.GetPoolNonce(ctx, address)
		if err != nil {
			return nil, err
		}
		return (*hexutil.Uint64)(&nonce), nil
	}
	// Resolve block number and use its state to ask for the nonce
	state, _, err := s.b.StateAndHeaderByNumberOrHash(ctx, blockNrOrHash)
	if state == nil || err != nil {
		return nil, err
	}
	nonce := state.GetNonce(address)
	return (*hexutil.Uint64)(&nonce), state.Error()
}

// GetTransactionByHash returns the transaction for the given hash
func (s *PublicTransactionPoolAPI) GetTransactionByHash(ctx context.Context, hash libcommon.Hash) (*RPCTransaction, error) {
	// Try to return an already finalized transaction
	tx, blockHash, blockNumber, index, err := s.b.GetTransaction(ctx, hash)
	if err != nil {
		return nil, err
	}
	if tx != nil {
		return newRPCTransaction(tx, blockHash, blockNumber, index), nil
	}
	// No finalized transaction, try to retrieve it from the pool
	if tx := s.b.GetPoolTransaction(hash); tx != nil {
		return newRPCPendingTransaction(tx), nil
	}

	// Transaction unknown, return as such
	return nil, nil
}

// GetRawTransactionByHash returns the bytes of the transaction for the given hash.
func (s *PublicTransactionPoolAPI) GetRawTransactionByHash(ctx context.Context, hash libcommon.Hash) (hexutil.Bytes, error) {
	// Retrieve a finalized transaction, or a pooled otherwise
	tx, _, _, _, err := s.b.GetTransaction(ctx, hash)
	if err != nil {
		return nil, err
	}
	if tx == nil {
		if tx = s.b.GetPoolTransaction(hash); tx == nil {
			// Transaction not found anywhere, abort
			return nil, nil
		}
	}
	// Serialize to RLP and return
	var blob bytes.Buffer
	if tx.Type() != types.LegacyTxType {
		if err := blob.WriteByte(tx.Type()); err != nil {
			return nil, err
		}
	}
	if err := rlp.Encode(&blob, tx); err != nil {
		return nil, err
	}
	return blob.Bytes(), nil
}

// GetTransactionReceipt returns the transaction receipt for the given transaction hash.
func (s *PublicTransactionPoolAPI) GetTransactionReceipt(ctx context.Context, hash libcommon.Hash) (map[string]interface{}, error) {
	tx, blockHash, blockNumber, index, err := s.b.GetTransaction(ctx, hash)
	if err != nil {
		return nil, nil
	}
	receipts, err := s.b.GetReceipts(ctx, blockHash)
	if err != nil {
		return nil, err
	}
	if len(receipts) <= int(index) {
		return nil, nil
	}
	receipt := receipts[index]

	// Derive the sender.
	signer := types.MakeSigner(s.b.ChainConfig(), blockNumber)
	from, _ := tx.Sender(*signer)

	fields := map[string]interface{}{
		"blockHash":         blockHash,
		"blockNumber":       hexutil.Uint64(blockNumber),
		"transactionHash":   hash,
		"transactionIndex":  hexutil.Uint64(index),
		"from":              from,
		"to":                tx.GetTo(),
		"gasUsed":           hexutil.Uint64(receipt.GasUsed),
		"cumulativeGasUsed": hexutil.Uint64(receipt.CumulativeGasUsed),
		"contractAddress":   nil,
		"logs":              receipt.Logs,
		"logsBloom":         receipt.Bloom,
		"type":              hexutil.Uint(tx.Type()),
	}

	// Assign receipt status or post state.
	if len(receipt.PostState) > 0 {
		fields["root"] = hexutil.Bytes(receipt.PostState)
	} else {
		fields["status"] = hexutil.Uint(receipt.Status)
	}
	if receipt.Logs == nil {
		fields["logs"] = [][]*types.Log{}
	}
	// If the ContractAddress is 20 0x0 bytes, assume it is not a contract creation
	if receipt.ContractAddress != (libcommon.Address{}) {
		fields["contractAddress"] = receipt.ContractAddress
	}
	return fields, nil
}

// SendTxArgs represents the arguments to sumbit a new transaction into the transaction pool.
type SendTxArgs struct {
	From     libcommon.Address  `json:"from"`
	To       *libcommon.Address `json:"to"`
	Gas      *hexutil.Uint64 `json:"gas"`
	GasPrice *hexutil.Big    `json:"gasPrice"`
	MaxPriorityFeePerGas      *hexutil.Big    `json:"tip"`
	MaxFeePerGas   *hexutil.Big    `json:"feeCap"`
	Value    *hexutil.Big    `json:"value"`
	Nonce    *hexutil.Uint64 `json:"nonce"`
	// We accept "data" and "input" for backwards-compatibility reasons. "input" is the
	// newer name and should be preferred by clients.
	Data  *hexutil.Bytes `json:"data"`
	Input *hexutil.Bytes `json:"input"`

	// For non-legacy transactions
	AccessList *types.AccessList `json:"accessList,omitempty"`
	ChainID    *hexutil.Big      `json:"chainId,omitempty"`
}

// setDefaults fills in default values for unspecified tx fields.
func (args *SendTxArgs) setDefaults(ctx context.Context, b Backend) error {
	if args.GasPrice == nil {
		price, err := b.SuggestPrice(ctx)
		if err != nil {
			return err
		}
		args.GasPrice = (*hexutil.Big)(price)
	}
	if args.Value == nil {
		args.Value = new(hexutil.Big)
	}
	if args.Nonce == nil {
		nonce, err := b.GetPoolNonce(ctx, args.From)
		if err != nil {
			return err
		}
		args.Nonce = (*hexutil.Uint64)(&nonce)
	}
	if args.Data != nil && args.Input != nil && !bytes.Equal(*args.Data, *args.Input) {
		return errors.New(`both "data" and "input" are set and not equal. Please use "input" to pass transaction call data`)
	}
	if args.To == nil {
		// Contract creation
		var input []byte
		if args.Data != nil {
			input = *args.Data
		} else if args.Input != nil {
			input = *args.Input
		}
		if len(input) == 0 {
			return errors.New(`contract creation without any data provided`)
		}
	}

	// Estimate the gas usage if necessary.
	if args.Gas == nil {
		// For backwards-compatibility reason, we try both input and data
		// but input is preferred.
		input := args.Input
		if input == nil {
			input = args.Data
		}
		callArgs := CallArgs{
			From:       &args.From, // From shouldn't be nil
			To:         args.To,
			GasPrice:   args.GasPrice,
			Value:      args.Value,
			Data:       input,
			AccessList: args.AccessList,
		}
		pendingBlockNr := rpc.BlockNumberOrHashWithNumber(rpc.PendingBlockNumber)
		estimated, err := DoEstimateGas(ctx, b, callArgs, pendingBlockNr, b.RPCGasCap())
		if err != nil {
			return err
		}
		args.Gas = &estimated
		log.Trace("Estimate gas usage automatically", "gas", args.Gas)
	}
	if args.ChainID == nil {
		id := (*hexutil.Big)(b.ChainConfig().ChainID)
		args.ChainID = id
	}
	return nil
}

// toTransaction converts the arguments to a transaction.
// This assumes that setDefaults has been called.
func (args *SendTxArgs) toTransaction() types.Transaction {
	var input []byte
	if args.Input != nil {
		input = *args.Input
	} else if args.Data != nil {
		input = *args.Data
	}

	var tx types.Transaction
	gasPrice, _ := uint256.FromBig((*big.Int)(args.GasPrice))
	value, _ := uint256.FromBig((*big.Int)(args.Value))
	if args.AccessList == nil {
		tx = &types.LegacyTx{
			CommonTx: types.CommonTx{
				To:    args.To,
				Nonce: uint64(*args.Nonce),
				Gas:   uint64(*args.Gas),
				Value: value,
				Data:  input,
			},
			GasPrice: gasPrice,
		}
	} else {
		chainId, _ := uint256.FromBig((*big.Int)(args.ChainID))
		if args.MaxFeePerGas == nil {
			tx = &types.AccessListTx{
				LegacyTx: types.LegacyTx{
					CommonTx: types.CommonTx{
						To:    args.To,
						Nonce: uint64(*args.Nonce),
						Gas:   uint64(*args.Gas),
						Value: value,
						Data:  input,
					},
					GasPrice: gasPrice,
				},
				ChainID:    chainId,
				AccessList: *args.AccessList,
			}
		} else {
			tip, _ := uint256.FromBig((*big.Int)(args.MaxPriorityFeePerGas))
			feeCap, _ := uint256.FromBig((*big.Int)(args.MaxFeePerGas))
			tx = &types.DynamicFeeTransaction{
				CommonTx: types.CommonTx{
					To:    args.To,
					Nonce: uint64(*args.Nonce),
					Gas:   uint64(*args.Gas),
					Value: value,
					Data:  input,
				},
				MaxPriorityFeePerGas:        tip,
				MaxFeePerGas:     feeCap,
				ChainID:    chainId,
				AccessList: *args.AccessList,
			}
		}
	}
	return tx
}

// SubmitTransaction is a helper function that submits tx to txPool and logs a message.
func SubmitTransaction(ctx context.Context, b Backend, tx types.Transaction) (libcommon.Hash, error) {
	// If the transaction fee cap is already specified, ensure the
	// fee of the given transaction is _reasonable_.
	if err := checkTxFee(tx.GetPrice().ToBig(), tx.GetGas(), b.RPCTxFeeCap()); err != nil {
		return libcommon.Hash{}, err
	}
	if !b.UnprotectedAllowed() && !tx.Protected() {
		// Ensure only eip155 signed transactions are submitted if EIP155Required is set.
		return libcommon.Hash{}, errors.New("only replay-protected (EIP-155) transactions allowed over RPC")
	}
	if err := b.SendTx(ctx, tx); err != nil {
		return libcommon.Hash{}, err
	}
	// Print a log with full tx details for manual investigations and interventions
	signer := types.MakeSigner(b.ChainConfig(), b.CurrentBlock().Number().Uint64())
	from, err := tx.Sender(*signer)
	if err != nil {
		return libcommon.Hash{}, err
	}

	if tx.GetTo() == nil {
		addr := crypto.CreateAddress(from, tx.GetNonce())
		log.Info("Submitted contract creation", "hash", tx.Hash().Hex(), "from", from, "nonce", tx.GetNonce(), "contract", addr.Hex(), "value", tx.GetValue())
	} else {
		log.Info("Submitted transaction", "hash", tx.Hash().Hex(), "from", from, "nonce", tx.GetNonce(), "recipient", tx.GetTo(), "value", tx.GetValue())
	}
	return tx.Hash(), nil
}

// FillTransaction fills the defaults (nonce, gas, gasPrice) on a given unsigned transaction,
// and returns it to the caller for further processing (signing + broadcast)
func (s *PublicTransactionPoolAPI) FillTransaction(ctx context.Context, args SendTxArgs) (*SignTransactionResult, error) {
	// Set some sanity defaults and terminate on failure
	if err := args.setDefaults(ctx, s.b); err != nil {
		return nil, err
	}
	// Assemble the transaction and obtain rlp
	tx := args.toTransaction()
	var blob bytes.Buffer
	if tx.Type() != types.LegacyTxType {
		if err := blob.WriteByte(tx.Type()); err != nil {
			return nil, err
		}
	}
	if err := rlp.Encode(&blob, tx); err != nil {
		return nil, err
	}
	return &SignTransactionResult{blob.Bytes(), tx}, nil
}

// SendRawTransaction will add the signed transaction to the transaction pool.
// The sender is responsible for signing the transaction and using the correct nonce.
func (s *PublicTransactionPoolAPI) SendRawTransaction(ctx context.Context, input hexutil.Bytes) (libcommon.Hash, error) {
	tx, err := types.DecodeTransaction(rlp.NewStream(bytes.NewReader(input), 0))
	if err != nil {
		return libcommon.Hash{}, err
	}
	return SubmitTransaction(ctx, s.b, tx)
}

// SignTransactionResult represents a RLP encoded signed transaction.
type SignTransactionResult struct {
	Raw hexutil.Bytes     `json:"raw"`
	Tx  types.Transaction `json:"tx"`
}

// PublicDebugAPI is the collection of Ethereum APIs exposed over the public
// debugging endpoint.
type PublicDebugAPI struct {
	b Backend
}

// NewPublicDebugAPI creates a new API definition for the public debug methods
// of the Ethereum service.
func NewPublicDebugAPI(b Backend) *PublicDebugAPI {
	return &PublicDebugAPI{b: b}
}

// GetBlockRlp retrieves the RLP encoded for of a single block.
func (api *PublicDebugAPI) GetBlockRlp(ctx context.Context, number uint64) (string, error) {
	block, _ := api.b.BlockByNumber(ctx, rpc.BlockNumber(number))
	if block == nil {
		return "", fmt.Errorf("block #%d not found", number)
	}
	encoded, err := rlp.EncodeToBytes(block)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", encoded), nil
}

// PrintBlock retrieves a block and returns its pretty printed form.
func (api *PublicDebugAPI) PrintBlock(ctx context.Context, number uint64) (string, error) {
	block, _ := api.b.BlockByNumber(ctx, rpc.BlockNumber(number))
	if block == nil {
		return "", fmt.Errorf("block #%d not found", number)
	}
	return spew.Sdump(block), nil
}

// SeedHash retrieves the seed hash of a block.
func (api *PublicDebugAPI) SeedHash(ctx context.Context, number uint64) (string, error) {
	block, _ := api.b.BlockByNumber(ctx, rpc.BlockNumber(number))
	if block == nil {
		return "", fmt.Errorf("block #%d not found", number)
	}
	return fmt.Sprintf("0x%x", ethash.SeedHash(number)), nil
}

// PrivateDebugAPI is the collection of Ethereum APIs exposed over the private
// debugging endpoint.
type PrivateDebugAPI struct {
	b Backend
}

// NewPrivateDebugAPI creates a new API definition for the private debug methods
// of the Ethereum service.
func NewPrivateDebugAPI(b Backend) *PrivateDebugAPI {
	return &PrivateDebugAPI{b: b}
}

// ChaindbProperty returns properties of the chain database.
func (api *PrivateDebugAPI) ChaindbProperty(property string) (string, error) {
	return "N/A", nil
}

// ChaindbCompact flattens the entire key-value database into a single level,
// removing all unused slots and merging all keys.
func (api *PrivateDebugAPI) ChaindbCompact() error {
	// Intentionally disabled in Erigon
	return nil
}

// SetHead rewinds the head of the blockchain to a previous block.
func (api *PrivateDebugAPI) SetHead(number hexutil.Uint64) {
	api.b.SetHead(uint64(number))
}

// PublicNetAPI offers network related RPC methods
type PublicNetAPI struct {
	net            *p2p.Server
	networkVersion uint64
}

// NewPublicNetAPI creates a new net API instance.
func NewPublicNetAPI(net *p2p.Server, networkVersion uint64) *PublicNetAPI {
	return &PublicNetAPI{net, networkVersion}
}

// Listening returns an indication if the node is listening for network connections.
func (s *PublicNetAPI) Listening() bool {
	return true // always listening
}

// PeerCount returns the number of connected peers
func (s *PublicNetAPI) PeerCount() hexutil.Uint {
	return hexutil.Uint(s.net.PeerCount())
}

// Version returns the current ethereum protocol version.
func (s *PublicNetAPI) Version() string {
	return fmt.Sprintf("%d", s.networkVersion)
}

// checkTxFee is an internal function used to check whether the fee of
// the given transaction is _reasonable_(under the cap).
func checkTxFee(gasPrice *big.Int, gas uint64, cap float64) error {
	// Short circuit if there is no cap for transaction fee at all.
	if cap == 0 {
		return nil
	}
	feeEth := new(big.Float).Quo(new(big.Float).SetInt(new(big.Int).Mul(gasPrice, new(big.Int).SetUint64(gas))), new(big.Float).SetInt(big.NewInt(params.Ether)))
	feeFloat, _ := feeEth.Float64()
	if feeFloat > cap {
		return fmt.Errorf("tx fee (%.2f ether) exceeds the configured cap (%.2f ether)", feeFloat, cap)
	}
	return nil
}
*/
