package commands

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"strings"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/rpc"
	"github.com/ledgerwatch/turbo-geth/turbo/adapter"
	"github.com/ledgerwatch/turbo-geth/turbo/rpchelper"
	"github.com/ledgerwatch/turbo-geth/turbo/transactions"
)

// TraceFilterRequest represents the arguments for trace_filter.
type TraceFilterRequest struct {
	FromBlock   *hexutil.Uint64   `json:"fromBlock"`
	ToBlock     *hexutil.Uint64   `json:"toBlock"`
	FromAddress []*common.Address `json:"fromAddress"`
	ToAddress   []*common.Address `json:"toAddress"`
	After       *uint64           `json:"after"`
	Count       *uint64           `json:"count"`
}

// TraceAPI RPC commands for traces
type TraceAPI interface {
	Call(ctx context.Context, req TraceFilterRequest) ([]interface{}, error)
	CallMany(ctx context.Context, req TraceFilterRequest) ([]interface{}, error)
	RawTransaction(ctx context.Context, req TraceFilterRequest) ([]interface{}, error)
	ReplayBlockTransactions(ctx context.Context, req TraceFilterRequest) ([]interface{}, error)
	ReplayTransaction(ctx context.Context, req TraceFilterRequest) ([]interface{}, error)
	Block(ctx context.Context, blockNr rpc.BlockNumber) ([]interface{}, error)
	Filter(ctx context.Context, req TraceFilterRequest) ([]interface{}, error)
	Get(ctx context.Context, txHash common.Hash, txIndicies []hexutil.Uint64) (interface{}, error)
	Transaction(ctx context.Context, txHash common.Hash) ([]interface{}, error)
	BlockReward(ctx context.Context, blockNr rpc.BlockNumber) (big.Int, error)
	UncleReward(ctx context.Context, blockNr rpc.BlockNumber) (big.Int, error)
	Issuance(ctx context.Context, blockNr rpc.BlockNumber) (big.Int, error)
}

// TraceAPIImpl is implementation of the TraceAPI interface based on remote Db access
type TraceAPIImpl struct {
	db        ethdb.KV
	dbReader  ethdb.Getter
	maxTraces uint64
}

// NewTraceAPI returns TraceAPIImpl instance
func NewTraceAPI(db ethdb.KV, dbReader ethdb.Getter, maxTraces uint64) *TraceAPIImpl {
	return &TraceAPIImpl{
		db:        db,
		dbReader:  dbReader,
		maxTraces: maxTraces,
	}
}

func retrieveHistory(tx ethdb.Tx, addr *common.Address, fromBlock uint64, toBlock uint64) ([]uint64, error) {
	addrBytes := addr.Bytes()
	ca := tx.Cursor(dbutils.AccountsHistoryBucket).Prefix(addrBytes)
	var blockNumbers []uint64

	for k, v, err := ca.First(); k != nil; k, v, err = ca.Next() {
		if err != nil {
			return nil, err
		}

		numbers, _, err := dbutils.WrapHistoryIndex(v).Decode()
		if err != nil {
			return nil, err
		}

		blockNumbers = append(blockNumbers, numbers...)
	}

	// cleanup for invalid blocks
	start := -1
	end := -1
	for i, b := range blockNumbers {
		if b >= fromBlock && b <= toBlock && start == -1 {
			start = i
			continue
		}

		if b > toBlock {
			end = i
			break
		}
	}

	if start == -1 {
		return []uint64{}, nil
	}

	if end == -1 {
		return blockNumbers[start:], nil
	}
	// Remove dublicates
	return blockNumbers[start:end], nil
}

func isAddressInFilter(addr *common.Address, filter []*common.Address) bool {
	if filter == nil {
		return true
	}
	i := sort.Search(len(filter), func(i int) bool {
		return bytes.Equal(filter[i].Bytes(), addr.Bytes())
	})

	return i != len(filter)
}

// Call Implements trace_call
func (api *TraceAPIImpl) Call(ctx context.Context, req TraceFilterRequest) ([]interface{}, error) {
	return api.Junk(ctx)
}

// CallMany Implements trace_call
func (api *TraceAPIImpl) CallMany(ctx context.Context, req TraceFilterRequest) ([]interface{}, error) {
	return api.Junk(ctx)
}

// RawTransaction Implements trace_rawtransaction
func (api *TraceAPIImpl) RawTransaction(ctx context.Context, req TraceFilterRequest) ([]interface{}, error) {
	return api.Junk(ctx)
}

// ReplayBlockTransactions Implements trace_replayBlockTransactions
func (api *TraceAPIImpl) ReplayBlockTransactions(ctx context.Context, req TraceFilterRequest) ([]interface{}, error) {
	return api.Junk(ctx)
}

// ReplayTransaction Implements trace_replaytransactions
func (api *TraceAPIImpl) ReplayTransaction(ctx context.Context, req TraceFilterRequest) ([]interface{}, error) {
	return api.Junk(ctx)
}

// BlockReward returns the block reward for this block
func (api *TraceAPIImpl) BlockReward(ctx context.Context, blockNr rpc.BlockNumber) (big.Int, error) {
	//genesisHash := rawdb.ReadBlockByNumber(api.dbReader, 0).Hash()
	//chainConfig := rawdb.ReadChainConfig(api.dbReader, genesisHash)
	//
	//block, err := api.getBlockByRPCNumber(blockNr)
	//if err != nil {
	//	return big.Int{}, err
	//}
	//minerReward, _ := ethash.AccumulateRewards(chainConfig, block.Header(), block.Uncles())
	//return minerReward, nil
	return big.Int{}, nil
}

// UncleReward returns the uncle reward for this block
func (api *TraceAPIImpl) UncleReward(ctx context.Context, blockNr rpc.BlockNumber) (big.Int, error) {
	//block, err := api.getBlockByRPCNumber(blockNr)
	//if err != nil {
	//	return big.Int{}, err
	//}
	//return uncleReward(block), nil
	return big.Int{}, nil
}

// Issuance returns the issuance for this block
func (api *TraceAPIImpl) Issuance(ctx context.Context, blockNr rpc.BlockNumber) (big.Int, error) {
	return big.Int{}, nil
}

// Block Implements trace_block
func (api *TraceAPIImpl) Block(ctx context.Context, blockNr rpc.BlockNumber) ([]interface{}, error) {
	blockNum, err := getBlockNumber(blockNr, api.dbReader)
	if err != nil {
		return nil, err
	}
	bn := hexutil.Uint64(blockNum)
	var req TraceFilterRequest
	req.FromBlock = &bn
	req.ToBlock = &bn
	req.FromAddress = nil
	req.ToAddress = nil
	req.After = nil
	req.Count = nil

	traces, err := api.Filter(ctx, req)
	if err != nil {
		return nil, err
	}
	return traces, err
}

//Junk shting
func (api *TraceAPIImpl) Junk(ctx context.Context) ([]interface{}, error) {
	txHash := common.HexToHash("0x5c504ed432cb51138bcf09aa5e8a410dd4a1e204ef84bfed1be16dfba1b22060")
	return api.Junk2(ctx, txHash)
}

// func (api *TraceAPIImpl) getBlockByRPCNumber(blockNr rpc.BlockNumber) (*types.Block, error) {
// 	blockNum, err := getBlockNumber(blockNr, api.dbReader)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return rawdb.ReadBlockByNumber(api.dbReader, blockNum), nil
// }

// Get Implements trace_get
func (api *TraceAPIImpl) Get(ctx context.Context, txHash common.Hash, indicies []hexutil.Uint64) (interface{}, error) {
	// oddly, Parity accepts an array of indicies, but only delivers on the first one
	if len(indicies) > 1 {
		return nil, nil
	}

	firstIndex := uint64(indicies[0]) + 1
	traces, err := api.Junk1(ctx, txHash)
	if err != nil {
		return nil, err
	}
	for i, trace := range traces {
		if uint64(i) == firstIndex {
			return trace, nil
		}
	}
	return nil, err
}

// Transaction Implements trace_transaction
func (api *TraceAPIImpl) Transaction(ctx context.Context, txHash common.Hash) ([]interface{}, error) {
	return api.Junk2(ctx, txHash)
}

// Junk1 - testing use only
func (api *TraceAPIImpl) Junk1(ctx context.Context, txHash common.Hash) (ParityTraces, error) {
	getter := adapter.NewBlockGetter(api.dbReader)
	chainContext := adapter.NewChainContext(api.dbReader)
	genesisHash := rawdb.ReadBlockByNumber(api.dbReader, 0).Hash()
	chainConfig := rawdb.ReadChainConfig(api.dbReader, genesisHash)
	traceType := "callTracer" // nolint: goconst

	tx, blockHash, blockNumber, txIndex := rawdb.ReadTransaction(api.dbReader, txHash)
	// _, blockHash, _, txIndex := rawdb.ReadTransaction(api.dbReader, txHash)
	msg, vmctx, ibs, _, err := transactions.ComputeTxEnv(ctx, getter, chainConfig, chainContext, api.db, blockHash, txIndex)
	if err != nil {
		return nil, err
	}

	trace, err := transactions.TraceTx(ctx, msg, vmctx, ibs, &eth.TraceConfig{Tracer: &traceType})
	if err != nil {
		return nil, err
	}
	traceJSON, ok := trace.(json.RawMessage)
	if !ok {
		return nil, fmt.Errorf("unknown type in trace_filter")
	}
	var gethTrace GethTrace
	jsonStr, _ := traceJSON.MarshalJSON()
	json.Unmarshal(jsonStr, &gethTrace) // nolint errcheck

	resp := ParityTraces{}
	converted := api.convertToParityTrace(gethTrace, blockHash, blockNumber, tx, txIndex, []int{})
	resp = append(resp, converted...)
	return resp, nil
}

// Junk2 - testing use only
func (api *TraceAPIImpl) Junk2(ctx context.Context, txHash common.Hash) ([]interface{}, error) {
	getter := adapter.NewBlockGetter(api.dbReader)
	chainContext := adapter.NewChainContext(api.dbReader)
	genesisHash := rawdb.ReadBlockByNumber(api.dbReader, 0).Hash()
	chainConfig := rawdb.ReadChainConfig(api.dbReader, genesisHash)
	traceType := "callTracer" // nolint: goconst

	tx, blockHash, blockNumber, txIndex := rawdb.ReadTransaction(api.dbReader, txHash)
	msg, vmctx, ibs, _, err := transactions.ComputeTxEnv(ctx, getter, chainConfig, chainContext, api.db, blockHash, txIndex)
	if err != nil {
		return nil, err
	}

	trace, err := transactions.TraceTx(ctx, msg, vmctx, ibs, &eth.TraceConfig{Tracer: &traceType})
	if err != nil {
		return nil, err
	}
	traceJSON, ok := trace.(json.RawMessage)
	if !ok {
		return nil, fmt.Errorf("unknown type in trace_filter")
	}
	var gethTrace GethTrace
	jsonStr, _ := traceJSON.MarshalJSON()
	json.Unmarshal(jsonStr, &gethTrace) // nolint errcheck

	var resp []interface{} // nolint prealloc
	converted := api.convertToParityTrace(gethTrace, blockHash, blockNumber, tx, txIndex, []int{})
	for _, convert := range converted {
		resp = append(resp, convert)
	}
	return resp, nil
}

// Filter Implements trace_filter
func (api *TraceAPIImpl) Filter(ctx context.Context, req TraceFilterRequest) ([]interface{}, error) {
	var filteredTransactionsHash []common.Hash
	var txTypes []bool
	var maxTracesCount uint64
	var offset uint64
	var skipped uint64

	sort.Slice(req.FromAddress, func(i int, j int) bool {
		return bytes.Compare(req.FromAddress[i].Bytes(), req.FromAddress[j].Bytes()) == -1
	})

	sort.Slice(req.ToAddress, func(i int, j int) bool {
		return bytes.Compare(req.ToAddress[i].Bytes(), req.ToAddress[j].Bytes()) == -1
	})

	var fromBlock uint64
	var toBlock uint64
	if req.FromBlock == nil {
		fromBlock = 0
	} else {
		fromBlock = uint64(*req.FromBlock)
	}

	if req.ToBlock == nil {
		headNumber := rawdb.ReadHeaderNumber(api.dbReader, rawdb.ReadHeadHeaderHash(api.dbReader))
		toBlock = *headNumber
	} else {
		toBlock = uint64(*req.ToBlock)
	}

	if fromBlock > toBlock {
		// TODO(tjayrush): Parity reports no error in this case
		return nil, nil //fmt.Errorf("invalid parameters: toBlock must be greater than fromBlock")
	}

	if req.Count == nil {
		maxTracesCount = api.maxTraces
	} else {
		maxTracesCount = *req.Count
	}

	if req.After == nil {
		offset = 0
	} else {
		offset = *req.After
	}

	if err := api.db.View(ctx, func(tx ethdb.Tx) error {
		if req.FromAddress != nil || req.ToAddress != nil { // use address history index to retrieve matching transactions
			var historyFilter []*common.Address
			isFromAddress := req.FromAddress != nil
			if isFromAddress {
				historyFilter = req.FromAddress
			} else {
				historyFilter = req.ToAddress
			}

			for _, addr := range historyFilter {

				addrBytes := addr.Bytes()
				blockNumbers, err := retrieveHistory(tx, addr, fromBlock, toBlock)
				if err != nil {
					return err
				}

				for _, num := range blockNumbers {

					block := rawdb.ReadBlockByNumber(api.dbReader, num)
					senders := rawdb.ReadSenders(api.dbReader, block.Hash(), num)
					txs := block.Transactions()
					for i, tx := range txs {
						if uint64(len(filteredTransactionsHash)) == maxTracesCount {
							if uint64(len(filteredTransactionsHash)) == api.maxTraces {
								return fmt.Errorf("too many traces found")
							}
							return nil
						}

						var to *common.Address
						if tx.To() == nil {
							to = &common.Address{}
						} else {
							to = tx.To()
						}

						if isFromAddress {
							if !isAddressInFilter(to, req.ToAddress) {
								continue
							}
							if bytes.Equal(senders[i].Bytes(), addrBytes) {
								filteredTransactionsHash = append(filteredTransactionsHash, tx.Hash())
								txTypes = append(txTypes, false)
							}
						} else if bytes.Equal(to.Bytes(), addrBytes) {
							if skipped < offset {
								skipped++
								continue
							}
							filteredTransactionsHash = append(filteredTransactionsHash, tx.Hash())
							txTypes = append(txTypes, false)
						}
					}
					//if skipped < offset {
					//	skipped++
					//	continue
					//}
					// filteredTransactionsHash = append(filteredTransactionsHash, block.Hash())
					// txTypes = append(txTypes, true)
				}
			}
		} else if req.FromBlock != nil || req.ToBlock != nil { // iterate over blocks

			for blockNum := fromBlock; blockNum < toBlock+1; blockNum++ {
				block := rawdb.ReadBlockByNumber(api.dbReader, blockNum)
				blockTransactions := block.Transactions()
				for _, tx := range blockTransactions {
					if uint64(len(filteredTransactionsHash)) == maxTracesCount {
						if uint64(len(filteredTransactionsHash)) == api.maxTraces {
							return fmt.Errorf("too many traces found")
						}
						return nil
					}
					if skipped < offset {
						skipped++
						continue
					}
					filteredTransactionsHash = append(filteredTransactionsHash, tx.Hash())
					txTypes = append(txTypes, false)
				}
				if skipped < offset {
					skipped++
					continue
				}
				filteredTransactionsHash = append(filteredTransactionsHash, block.Hash())
				txTypes = append(txTypes, true)
			}
		} else {
			return fmt.Errorf("invalid parameters")
		}
		return nil
	}); err != nil {
		return nil, err
	}
	getter := adapter.NewBlockGetter(api.dbReader)
	chainContext := adapter.NewChainContext(api.dbReader)
	genesisHash := rawdb.ReadBlockByNumber(api.dbReader, 0).Hash()
	chainConfig := rawdb.ReadChainConfig(api.dbReader, genesisHash)
	traceType := "callTracer" // nolint: goconst
	traces := []interface{}{}
	for i, theHash := range filteredTransactionsHash {
		if txTypes[i] {
			block := rawdb.ReadBlockByHash(api.dbReader, theHash)
			minerReward, uncleRewards := ethash.AccumulateRewards(chainConfig, block.Header(), block.Uncles())
			var tr ParityTrace
			tr.Action.Author = strings.ToLower(block.Coinbase().String())
			tr.Action.RewardType = "block"
			tr.Action.Value = minerReward.String()
			tr.BlockHash = block.Hash()
			tr.BlockNumber = block.NumberU64()
			tr.Type = "reward" // nolint: goconst
			traces = append(traces, tr)
			for i, uncle := range block.Uncles() {
				if i < len(uncleRewards) {
					var tr ParityTrace
					tr.Action.Author = strings.ToLower(uncle.Coinbase.String())
					tr.Action.RewardType = "uncle"
					tr.Action.Value = uncleRewards[i].String()
					tr.BlockHash = block.Hash()
					tr.BlockNumber = block.NumberU64()
					tr.Type = "reward" // nolint: goconst
					traces = append(traces, tr)
				}
			}
		} else {
			tx, blockHash, blockNumber, txIndex := rawdb.ReadTransaction(api.dbReader, theHash)
			msg, vmctx, ibs, _, err := transactions.ComputeTxEnv(ctx, getter, chainConfig, chainContext, api.db, blockHash, txIndex)
			if err != nil {
				return nil, err
			}
			trace, err := transactions.TraceTx(ctx, msg, vmctx, ibs, &eth.TraceConfig{Tracer: &traceType})
			if err != nil {
				return nil, err
			}
			traceJSON, ok := trace.(json.RawMessage)
			if !ok {
				return nil, fmt.Errorf("unknown type in trace_filter")
			}
			var gethTrace GethTrace
			jsonStr, _ := traceJSON.MarshalJSON()
			json.Unmarshal(jsonStr, &gethTrace) // nolint errcheck
			converted := api.convertToParityTrace(gethTrace, blockHash, blockNumber, tx, txIndex, []int{})
			for _, convert := range converted {
				traces = append(traces, convert)
			}
		}
	}
	return traces, nil
}

func (api *TraceAPIImpl) convertToParityTrace(gethTrace GethTrace, blockHash common.Hash, blockNumber uint64, tx *types.Transaction, txIndex uint64, depth []int) ParityTraces {
	var traces ParityTraces
	var pt ParityTrace

	callType := strings.ToLower(gethTrace.Type)
	if callType == "create" {
		pt.Action.CallType = ""
		pt.Action.From = gethTrace.From
		pt.Action.Init = gethTrace.Input
		pt.Result.Address = gethTrace.To
		pt.Result.Output = ""
		pt.Action.Value = gethTrace.Value
		pt.Result.Code = gethTrace.Output
		pt.Action.Gas = gethTrace.Gas
		pt.Result.GasUsed = gethTrace.GasUsed

	} else if callType == "selfdestruct" {
		pt.Action.CallType = ""
		pt.Action.Input = gethTrace.Input
		pt.Result.Output = gethTrace.Output
		pt.Action.Value = gethTrace.Value
		pt.Action.Gas = gethTrace.Gas
		pt.Result.GasUsed = gethTrace.GasUsed
		pt.Action.SelfDestructed = gethTrace.From
		pt.Action.RefundAddress = gethTrace.To
		acc, err := rpchelper.GetAccount(api.db, blockNumber, common.HexToAddress(gethTrace.From))
		if err != nil {
			return traces
		}
		if acc != nil {
			val := acc.Balance.ToBig()
			pt.Action.Balance = (*val).String()
		}
	} else {
		pt.Action.CallType = callType
		pt.Action.Input = gethTrace.Input
		pt.Action.From = gethTrace.From
		pt.Action.To = gethTrace.To
		pt.Result.Output = gethTrace.Output
		pt.Action.Value = gethTrace.Value
		pt.Result.Code = "" // gethTrace.XXX
		pt.Action.Gas = gethTrace.Gas
		pt.Result.GasUsed = gethTrace.GasUsed

		if pt.Action.Gas == "0xdeadbeef" {
			a, _ := hexutil.DecodeUint64(pt.Action.Gas)
			b, _ := hexutil.DecodeUint64(pt.Result.GasUsed)
			c := a - b
			pt.Action.Gas = hexutil.EncodeUint64(c)
			pt.Result.GasUsed = hexutil.EncodeUint64(0)
		}
	}

	pt.BlockHash = blockHash
	pt.BlockNumber = blockNumber
	pt.Subtraces = len(gethTrace.Calls)
	pt.TraceAddress = depth
	pt.TransactionHash = tx.Hash()
	pt.TransactionPosition = txIndex
	pt.Type = callType
	if pt.Type == "delegatecall" || pt.Type == "staticcall" {
		pt.Type = "call"
	}

	traces = append(traces, pt)

	for i, item := range gethTrace.Calls {
		newDepth := append(depth, i)
		subTraces := api.convertToParityTrace(*item, blockHash, blockNumber, tx, txIndex, newDepth)
		traces = append(traces, subTraces...)
	}

	return traces
}

// GethTrace A trace from Geth
type GethTrace struct {
	Type    string     `json:"type"`
	From    string     `json:"from"`
	To      string     `json:"to"`
	Value   string     `json:"value"`
	Gas     string     `json:"gas"`
	GasUsed string     `json:"gasUsed"`
	Input   string     `json:"input"`
	Output  string     `json:"output"`
	Time    string     `json:"time"`
	Calls   GethTraces `json:"calls"`
}

// GethTraces array of GetTraces
type GethTraces []*GethTrace

func (p GethTrace) String() string {
	var ret string
	ret += fmt.Sprintf("Type: %s\n", p.Type)
	ret += fmt.Sprintf("From: %s\n", p.From)
	ret += fmt.Sprintf("To: %s\n", p.To)
	ret += fmt.Sprintf("Value: %s\n", p.Value)
	ret += fmt.Sprintf("Gas: %s\n", p.Gas)
	ret += fmt.Sprintf("GasUsed: %s\n", p.GasUsed)
	ret += fmt.Sprintf("Input: %s\n", p.Input)
	ret += fmt.Sprintf("Output: %s\n", p.Output)
	return ret
}

// TraceAction A parity formatted trace action
type TraceAction struct {
	Author         string `json:"author,omitempty"`
	RewardType     string `json:"rewardType,omitempty"`
	SelfDestructed string `json:"address,omitempty"`
	Balance        string `json:"balance,omitempty"`
	CallType       string `json:"callType,omitempty"`
	From           string `json:"from,omitempty"`
	Gas            string `json:"gas,omitempty"`
	Init           string `json:"init,omitempty"`
	Input          string `json:"input,omitempty"`
	RefundAddress  string `json:"refundAddress,omitempty"`
	To             string `json:"to,omitempty"`
	Value          string `json:"value,omitempty"`
}

// TraceResult A parity formatted trace result
type TraceResult struct {
	Address string `json:"address,omitempty"`
	Code    string `json:"code,omitempty"`
	GasUsed string `json:"gasUsed,omitempty"`
	Output  string `json:"output,omitempty"`
}

// ParityTrace a Parity formatted trace
type ParityTrace struct {
	Action              TraceAction `json:"action"`
	BlockHash           common.Hash `json:"blockHash"`
	BlockNumber         uint64      `json:"blockNumber"`
	Result              TraceResult `json:"result"`
	Subtraces           int         `json:"subtraces"`
	TraceAddress        []int       `json:"traceAddress"`
	TransactionHash     common.Hash `json:"transactionHash"`
	TransactionPosition uint64      `json:"transactionPosition"`
	Type                string      `json:"type"`
}

func (t ParityTrace) String() string {
	var ret string
	ret += fmt.Sprintf("Action.SelfDestructed: %s\n", t.Action.SelfDestructed)
	ret += fmt.Sprintf("Action.Balance: %s\n", t.Action.Balance)
	ret += fmt.Sprintf("Action.CallType: %s\n", t.Action.CallType)
	ret += fmt.Sprintf("Action.From: %s\n", t.Action.From)
	ret += fmt.Sprintf("Action.Gas: %s\n", t.Action.Gas)
	ret += fmt.Sprintf("Action.Init: %s\n", t.Action.Init)
	ret += fmt.Sprintf("Action.Input: %s\n", t.Action.Input)
	ret += fmt.Sprintf("Action.RefundAddress: %s\n", t.Action.RefundAddress)
	ret += fmt.Sprintf("Action.To: %s\n", t.Action.To)
	ret += fmt.Sprintf("Action.Value: %s\n", t.Action.Value)
	ret += fmt.Sprintf("BlockHash: %v\n", t.BlockHash)
	ret += fmt.Sprintf("BlockNumber: %d\n", t.BlockNumber)
	ret += fmt.Sprintf("Result.Address: %s\n", t.Result.Address)
	ret += fmt.Sprintf("Result.Code: %s\n", t.Result.Code)
	ret += fmt.Sprintf("Result.GasUsed: %s\n", t.Result.GasUsed)
	ret += fmt.Sprintf("Result.Output: %s\n", t.Result.Output)
	ret += fmt.Sprintf("Subtraces: %d\n", t.Subtraces)
	//ret += fmt.Sprintf("TraceAddress: %s\n", t.TraceAddress)
	ret += fmt.Sprintf("TransactionHash: %v\n", t.TransactionHash)
	ret += fmt.Sprintf("TransactionPosition: %d\n", t.TransactionPosition)
	ret += fmt.Sprintf("Type: %s\n", t.Type)
	return ret
}

// ParityTraces what
type ParityTraces []ParityTrace
