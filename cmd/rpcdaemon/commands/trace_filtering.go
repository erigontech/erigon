package commands

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/eth/tracers"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/bitmapdb"
	"github.com/ledgerwatch/turbo-geth/rpc"
	"github.com/ledgerwatch/turbo-geth/turbo/adapter"
	"github.com/ledgerwatch/turbo-geth/turbo/transactions"
)

// Transaction implements trace_transaction
// TODO(tjayrush): I think this should return an []interface{}, so we can return both Parity and Geth traces
func (api *TraceAPIImpl) Transaction(ctx context.Context, txHash common.Hash) (ParityTraces, error) {
	tx, err := api.kv.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	traces, err := api.getTransactionTraces(tx, ctx, txHash)
	if err != nil {
		return nil, err
	}
	return traces, err
}

// Get implements trace_get
// TODO(tjayrush): This command should take an rpc.BlockNumber .This would allow blockNumbers and 'latest',
// TODO(tjayrush): 'pending', etc. Parity only accepts block hash.
// TODO(tjayrush): Also, for some reason, Parity definesthe second parameter as an array of indexes, but
// TODO(tjayrush): only accepts a single one
// TODO(tjayrush): I think this should return an interface{}, so we can return both Parity and Geth traces
func (api *TraceAPIImpl) Get(ctx context.Context, txHash common.Hash, indicies []hexutil.Uint64) (*ParityTrace, error) {
	tx, err := api.kv.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// TODO(tjayrush): Parity fails if it gets more than a single index. Returns nothing in this case.
	if len(indicies) > 1 {
		return nil, nil
	}

	traces, err := api.getTransactionTraces(tx, ctx, txHash)
	if err != nil {
		return nil, err
	}

	// TODO(tjayrush): For some reason, the 'get' index is one-based
	firstIndex := int(indicies[0]) + 1
	for i, trace := range traces {
		if i == firstIndex {
			return &trace, nil
		}
	}
	return nil, err
}

// Block implements trace_block
func (api *TraceAPIImpl) Block(ctx context.Context, blockNr rpc.BlockNumber) (ParityTraces, error) {
	tx, err := api.kv.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	blockNum, err := getBlockNumber(blockNr, tx)
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

// Filter implements trace_filter
// TODO(tjayrush): Eventually, we will need to protect ourselves from 'large' queries. Parity crashes when a range query of a very large size
// is sent. We need to protect ourselves with maxTraces. It may already be done
func (api *TraceAPIImpl) Filter(ctx context.Context, req TraceFilterRequest) (ParityTraces, error) {
	tx, err1 := api.kv.Begin(ctx)
	if err1 != nil {
		return nil, fmt.Errorf("traceFilter cannot open tx: %v", err1)
	}
	defer tx.Rollback()

	var filteredHashes []common.Hash
	// TODO(tjayrush): Parity intersperses block/uncle reward traces with transaction call traces. We need to be able to tell the
	// difference. I do that with this boolean array. This will be re-written shortly. For now, we use this simple boolean flag.
	// Eventually, we will fill the ParityTrace array directly as we go (and we can probably even do better than that)
	var traceTypes []bool
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
		headNumber := rawdb.ReadHeaderNumber(ethdb.NewRoTxDb(tx), rawdb.ReadHeadHeaderHash(ethdb.NewRoTxDb(tx)))
		toBlock = *headNumber
	} else {
		toBlock = uint64(*req.ToBlock)
	}

	if fromBlock > toBlock {
		// TODO(tjayrush): Parity reports no error in this case it simply returns an empty response
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
			blockNumbers, errHistory := retrieveHistory(ethdb.NewRoTxDb(tx), addr, fromBlock, toBlock)
			if errHistory != nil {
				return nil, errHistory
			}

			for _, num := range blockNumbers {
				block, err := rawdb.ReadBlockByNumber(ethdb.NewRoTxDb(tx), num)
				if err != nil {
					return nil, err
				}
				senders, errSenders := rawdb.ReadSenders(ethdb.NewRoTxDb(tx), block.Hash(), num)
				if errSenders != nil {
					return nil, errSenders
				}
				for i, txn := range block.Transactions() {
					if uint64(len(filteredHashes)) == maxTracesCount {
						if uint64(len(filteredHashes)) == api.maxTraces {
							return nil, fmt.Errorf("too many traces found")
						}
						return nil, nil
					}

					var to *common.Address
					if txn.To() == nil {
						to = &common.Address{}
					} else {
						to = txn.To()
					}

					if isFromAddress {
						if !isAddressInFilter(to, req.ToAddress) {
							continue
						}
						if bytes.Equal(senders[i].Bytes(), addrBytes) {
							filteredHashes = append(filteredHashes, txn.Hash())
							traceTypes = append(traceTypes, false)
						}
					} else if bytes.Equal(to.Bytes(), addrBytes) {
						if skipped < offset {
							skipped++
							continue
						}
						filteredHashes = append(filteredHashes, txn.Hash())
						traceTypes = append(traceTypes, false)
					}
				}
				// TODO(tjayrush): Parity does not (for some unknown reason) include blockReward traces here
			}
		}
	} else if req.FromBlock != nil || req.ToBlock != nil { // iterate over blocks

		for blockNum := fromBlock; blockNum < toBlock+1; blockNum++ {
			block, err := rawdb.ReadBlockByNumber(ethdb.NewRoTxDb(tx), blockNum)
			if err != nil {
				return nil, err
			}
			for _, txn := range block.Transactions() {
				if uint64(len(filteredHashes)) == maxTracesCount {
					if uint64(len(filteredHashes)) == api.maxTraces {
						return nil, fmt.Errorf("too many traces found")
					}
					return nil, nil
				}
				if skipped < offset {
					skipped++
					continue
				}
				filteredHashes = append(filteredHashes, txn.Hash())
				traceTypes = append(traceTypes, false)
			}
			// TODO(tjayrush): Need much, much better testing surrounding offset and count especially when including block rewards
			if skipped < offset {
				skipped++
				continue
			}
			// We need to intersperse block reward traces to match Parity
			filteredHashes = append(filteredHashes, block.Hash())
			traceTypes = append(traceTypes, true)
		}
	} else {
		return nil, fmt.Errorf("invalid parameters")
	}

	getter := adapter.NewBlockGetter(tx)
	chainContext := adapter.NewChainContext(tx)
	genesis, err := rawdb.ReadBlockByNumber(ethdb.NewRoTxDb(tx), 0)
	if err != nil {
		return nil, err
	}
	genesisHash := genesis.Hash()
	chainConfig, err := rawdb.ReadChainConfig(ethdb.NewRoTxDb(tx), genesisHash)
	if err != nil {
		return nil, err
	}
	traceType := "callTracer" // nolint: goconst
	traces := ParityTraces{}

	for i, txOrBlockHash := range filteredHashes {
		if traceTypes[i] {
			// In this case, we're processing a block (or uncle) reward trace. The hash is a block hash
			// Because Geth does not return blockReward or uncleReward traces, we must create them here
			block, err := rawdb.ReadBlockByHash(ethdb.NewRoTxDb(tx), txOrBlockHash)
			if err != nil {
				return nil, err
			}
			minerReward, uncleRewards := ethash.AccumulateRewards(chainConfig, block.Header(), block.Uncles())
			fmt.Printf("%v\n", minerReward)
			var tr ParityTrace
			//tr.Action.Author = strings.ToLower(block.Coinbase().String())
			//tr.Action.RewardType = "block" // goconst
			//tr.Action.Value = minerReward.String()
			tr.BlockHash = &common.Hash{}
			copy(tr.BlockHash[:], block.Hash().Bytes())
			tr.BlockNumber = new(uint64)
			*tr.BlockNumber = block.NumberU64()
			tr.Type = "reward" // nolint: goconst
			traces = append(traces, tr)
			for i, uncle := range block.Uncles() {
				fmt.Printf("%v\n", uncle)
				if i < len(uncleRewards) {
					var tr ParityTrace
					//tr.Action.Author = strings.ToLower(uncle.Coinbase.String())
					//tr.Action.RewardType = "uncle" // goconst
					//tr.Action.Value = uncleRewards[i].String()
					tr.BlockHash = &common.Hash{}
					copy(tr.BlockHash[:], block.Hash().Bytes())
					tr.BlockNumber = new(uint64)
					*tr.BlockNumber = block.NumberU64()
					tr.Type = "reward" // nolint: goconst
					traces = append(traces, tr)
				}
			}
		} else {
			// In this case, we're processing a transaction hash
			txn, blockHash, blockNumber, txIndex := rawdb.ReadTransaction(ethdb.NewRoTxDb(tx), txOrBlockHash)
			msg, blockCtx, txCtx, ibs, _, err := transactions.ComputeTxEnv(ctx, getter, chainConfig, chainContext, tx, blockHash, txIndex)
			if err != nil {
				return nil, err
			}
			trace, err := transactions.TraceTx(ctx, msg, blockCtx, txCtx, ibs, &tracers.TraceConfig{Tracer: &traceType}, chainConfig)
			if err != nil {
				return nil, err
			}
			traceJSON, ok := trace.(json.RawMessage)
			if !ok {
				return nil, fmt.Errorf("unknown type in trace_filter")
			}
			var gethTrace GethTrace
			jsonStr, _ := traceJSON.MarshalJSON()
			json.Unmarshal(jsonStr, &gethTrace) // nolint:errcheck
			converted := api.convertToParityTrace(gethTrace, blockHash, blockNumber, txn, txIndex, []int{})
			traces = append(traces, converted...)
		}
	}
	return traces, nil
}

func retrieveHistory(tx ethdb.Getter, addr *common.Address, fromBlock uint64, toBlock uint64) ([]uint64, error) {
	blocks, err := bitmapdb.Get(tx, dbutils.AccountsHistoryBucket, addr.Bytes(), uint32(fromBlock), uint32(toBlock+1))
	if err != nil {
		return nil, err
	}
	blocks.RemoveRange(fromBlock, toBlock+1)
	return toU64(blocks.ToArray()), nil
}

func toU64(in []uint32) []uint64 {
	out := make([]uint64, len(in))
	for i := range in {
		out[i] = uint64(in[i])
	}
	return out
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

// getTransactionTraces - returns the traces for a single transaction. Used by trace_get and trace_transaction.
// TODO(tjayrush):
// Implementation Notes:
// -- For convienience, we return both Parity and Geth traces for now. In the future we will either separate
//    these functions or eliminate Geth traces
// -- The function convertToParityTraces takes a hierarchical Geth trace and returns a flattened Parity trace
func (api *TraceAPIImpl) getTransactionTraces(tx ethdb.Tx, ctx context.Context, txHash common.Hash) (ParityTraces, error) {
	getter := adapter.NewBlockGetter(tx)
	chainContext := adapter.NewChainContext(tx)
	genesis, err := rawdb.ReadBlockByNumber(ethdb.NewRoTxDb(tx), 0)
	if err != nil {
		return nil, err
	}
	genesisHash := genesis.Hash()
	chainConfig, err := rawdb.ReadChainConfig(ethdb.NewRoTxDb(tx), genesisHash)
	if err != nil {
		return nil, err
	}
	traceType := "callTracer" // nolint: goconst

	txn, blockHash, blockNumber, txIndex := rawdb.ReadTransaction(ethdb.NewRoTxDb(tx), txHash)
	msg, blockCtx, txCtx, ibs, _, err := transactions.ComputeTxEnv(ctx, getter, chainConfig, chainContext, tx, blockHash, txIndex)
	if err != nil {
		return nil, err
	}

	// Time spent 176 out of 205
	trace, err := transactions.TraceTx(ctx, msg, blockCtx, txCtx, ibs, &tracers.TraceConfig{Tracer: &traceType}, chainConfig)
	if err != nil {
		return nil, err
	}

	traceJSON, ok := trace.(json.RawMessage)
	if !ok {
		return nil, fmt.Errorf("unknown type in trace_filter")
	}

	var gethTrace GethTrace
	jsonStr, _ := traceJSON.MarshalJSON()
	// Time spent 26 out of 205
	json.Unmarshal(jsonStr, &gethTrace) // nolint:errcheck

	traces := ParityTraces{}
	// Time spent 3 out of 205
	converted := api.convertToParityTrace(gethTrace, blockHash, blockNumber, txn, txIndex, []int{})
	traces = append(traces, converted...)

	return traces, nil
}

// TraceFilterRequest represents the arguments for trace_filter
type TraceFilterRequest struct {
	FromBlock   *hexutil.Uint64   `json:"fromBlock"`
	ToBlock     *hexutil.Uint64   `json:"toBlock"`
	FromAddress []*common.Address `json:"fromAddress"`
	ToAddress   []*common.Address `json:"toAddress"`
	After       *uint64           `json:"after"`
	Count       *uint64           `json:"count"`
}
