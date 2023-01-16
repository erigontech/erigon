package rpctest

import (
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/state"
)

const Geth = "geth"
const Erigon = "erigon"

type EthError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type CommonResponse struct {
	Version   string    `json:"jsonrpc"`
	RequestId int       `json:"id"`
	Error     *EthError `json:"error"`
}

type EthBlockNumber struct {
	CommonResponse
	Number hexutil.Uint64 `json:"result"`
}

type EthBalance struct {
	CommonResponse
	Balance hexutil.Big `json:"result"`
}

type EthTransaction struct {
	From     libcommon.Address  `json:"from"`
	To       *libcommon.Address `json:"to"` // Pointer because it might be missing
	Hash     string             `json:"hash"`
	Gas      hexutil.Big        `json:"gas"`
	GasPrice hexutil.Big        `json:"gasPrice"`
	Input    hexutil.Bytes      `json:"input"`
	Value    hexutil.Big        `json:"value"`
}

type EthSendRawTransaction struct {
	CommonResponse
	TxnHash libcommon.Hash `json:"result"`
}

type EthTxPool struct {
	CommonResponse
	Result interface{} `json:"result"`
}

type EthBlockByNumberResult struct {
	Difficulty   hexutil.Big       `json:"difficulty"`
	Miner        libcommon.Address `json:"miner"`
	Transactions []EthTransaction  `json:"transactions"`
	TxRoot       libcommon.Hash    `json:"transactionsRoot"`
	Hash         libcommon.Hash    `json:"hash"`
}

type EthBlockByNumber struct {
	CommonResponse
	Result *EthBlockByNumberResult `json:"result"`
}

type StructLog struct {
	Op      string            `json:"op"`
	Pc      uint64            `json:"pc"`
	Depth   uint64            `json:"depth"`
	Error   *EthError         `json:"error"`
	Gas     uint64            `json:"gas"`
	GasCost uint64            `json:"gasCost"`
	Memory  []string          `json:"memory"`
	Stack   []string          `json:"stack"`
	Storage map[string]string `json:"storage"`
}

type EthTxTraceResult struct {
	Gas         uint64      `json:"gas"`
	Failed      bool        `json:"failed"`
	ReturnValue string      `json:"returnValue"`
	StructLogs  []StructLog `json:"structLogs"`
}

type EthTxTrace struct {
	CommonResponse
	Result EthTxTraceResult `json:"result"`
}

type TraceCall struct {
	CommonResponse
	Result TraceCallResult `json:"result"`
}

type TraceCallResult struct {
	Output    hexutil.Bytes                            `json:"output"`
	Trace     []TraceCallTrace                         `json:"trace"`
	StateDiff map[libcommon.Address]TraceCallStateDiff `json:"stateDiff"`
}

type TraceCallTrace struct {
	Type         string                `json:"type"`
	Action       TraceCallAction       `json:"action"`
	Result       *TraceCallTraceResult `json:"result"`
	Subtraces    int                   `json:"subtraces"`
	TraceAddress []int                 `json:"traceAddress"`
	Error        string                `json:"error"`
}

// TraceCallAction is superset of all possible action types
type TraceCallAction struct {
	From          libcommon.Address `json:"from"`
	To            libcommon.Address `json:"to"`
	Address       libcommon.Address `json:"address"`
	RefundAddress libcommon.Address `json:"refundAddress"`
	Gas           hexutil.Big       `json:"gas"`
	Value         hexutil.Big       `json:"value"`
	Balance       hexutil.Big       `json:"balance"`
	Init          hexutil.Bytes     `json:"init"`
	Input         hexutil.Bytes     `json:"input"`
	CallType      string            `json:"callType"`
}

type TraceCallTraceResult struct {
	GasUsed hexutil.Big       `json:"gasUsed"`
	Output  hexutil.Bytes     `json:"output"`
	Address libcommon.Address `json:"address"`
	Code    hexutil.Bytes     `json:"code"`
}

type TraceCallStateDiff struct {
	Balance interface{}                                             `json:"balance"`
	Nonce   interface{}                                             `json:"nonce"`
	Code    interface{}                                             `json:"code"`
	Storage map[libcommon.Hash]map[string]TraceCallStateDiffStorage `json:"storage"`
}

type TraceCallStateDiffStorage struct {
	From libcommon.Hash `json:"from"`
	To   libcommon.Hash `json:"to"`
}

type DebugModifiedAccounts struct {
	CommonResponse
	Result []libcommon.Address `json:"result"`
}

func (ma *DebugModifiedAccounts) Print() {
	r := ma.Result
	rset := make(map[libcommon.Address]struct{})
	for _, a := range r {
		rset[a] = struct{}{}
	}
	for a := range rset {
		fmt.Printf("%x\n", a)
	}
}

// StorageRangeResult is the result of a debug_storageRangeAt API call.
type StorageRangeResult struct {
	Storage storageMap      `json:"storage"`
	NextKey *libcommon.Hash `json:"nextKey"` // nil if Storage includes the last key in the trie.
}

type storageMap map[libcommon.Hash]storageEntry

type storageEntry struct {
	Key   *libcommon.Hash `json:"key"`
	Value libcommon.Hash  `json:"value"`
}

type DebugStorageRange struct {
	CommonResponse
	Result StorageRangeResult `json:"result"`
}

type DebugAccountRange struct {
	CommonResponse
	Result state.IteratorDump `json:"result"`
}

// Log represents a contract log event. These events are generated by the LOG opcode and
// stored/indexed by the node.
type Log struct { //nolint
	// Consensus fields:
	// address of the contract that generated the event
	Address libcommon.Address `json:"address" gencodec:"required"`
	// list of topics provided by the contract.
	Topics []libcommon.Hash `json:"topics" gencodec:"required"`
	// supplied by the contract, usually ABI-encoded
	Data hexutil.Bytes `json:"data" gencodec:"required"`

	// Derived fields. These fields are filled in by the node
	// but not secured by consensus.
	// block in which the transaction was included
	BlockNumber hexutil.Uint64 `json:"blockNumber"`
	// hash of the transaction
	TxHash libcommon.Hash `json:"transactionHash" gencodec:"required"`
	// index of the transaction in the block
	TxIndex hexutil.Uint `json:"transactionIndex" gencodec:"required"`
	// hash of the block in which the transaction was included
	BlockHash libcommon.Hash `json:"blockHash"`
	// index of the log in the receipt
	Index hexutil.Uint `json:"logIndex" gencodec:"required"`

	// The Removed field is true if this log was reverted due to a chain reorganisation.
	// You must pay attention to this field if you receive logs through a filter query.
	Removed bool `json:"removed"`
}

type Receipt struct {
	// Consensus fields
	PostState         libcommon.Hash `json:"root"`
	Status            hexutil.Uint64 `json:"status"`
	CumulativeGasUsed hexutil.Uint64 `json:"cumulativeGasUsed" gencodec:"required"`
	Bloom             hexutil.Bytes  `json:"logsBloom"         gencodec:"required"`
	Logs              []*Log         `json:"logs"              gencodec:"required"`

	// Implementation fields (don't reorder!)
	TxHash          libcommon.Hash     `json:"transactionHash" gencodec:"required"`
	ContractAddress *libcommon.Address `json:"contractAddress"`
	GasUsed         hexutil.Uint64     `json:"gasUsed" gencodec:"required"`
}

type EthReceipt struct {
	CommonResponse
	Result Receipt `json:"result"`
}

type EthGetProof struct {
	CommonResponse
	Result AccountResult `json:"result"`
}

type EthGetLogs struct {
	CommonResponse
	Result []Log `json:"result"`
}

type EthGetTransactionCount struct {
	CommonResponse
	Result hexutil.Uint64 `json:"result"`
}

// AccountResult is the result struct for GetProof
type AccountResult struct {
	Address      libcommon.Address `json:"address"`
	AccountProof []string          `json:"accountProof"`
	Balance      *hexutil.Big      `json:"balance"`
	CodeHash     libcommon.Hash    `json:"codeHash"`
	Nonce        hexutil.Uint64    `json:"nonce"`
	StorageHash  libcommon.Hash    `json:"storageHash"`
	StorageProof []StorageResult   `json:"storageProof"`
}
type StorageResult struct {
	Key   string       `json:"key"`
	Value *hexutil.Big `json:"value"`
	Proof []string     `json:"proof"`
}

type ParityListStorageKeysResult struct {
	CommonResponse
	Result []hexutil.Bytes `json:"result"`
}
