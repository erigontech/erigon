package commands

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/eth"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/rpc"
	"github.com/ledgerwatch/turbo-geth/turbo/adapter"
	"github.com/ledgerwatch/turbo-geth/turbo/transactions"
)

// Transaction Implements trace_transaction
// TODO(tjayrush): I think this should return an []interface{}, so we can return both Parity and Geth traces
func (api *TraceAPIImpl) Transaction(ctx context.Context, txHash common.Hash) (ParityTraces, error) {
	traces, err := api.getTransactionTraces(ctx, txHash)
	if err != nil {
		return nil, err
	}
	return traces, err
}

// Get Implements trace_get
// TODO(tjayrush): This command should take an rpc.BlockNumber .This would allow blockNumbers and 'latest',
// TODO(tjayrush): 'pending', etc. Parity only accepts block hash.
// TODO(tjayrush): Also, for some reason, Parity definesthe second parameter as an array of indexes, but
// TODO(tjayrush): only accepts a single one
// TODO(tjayrush): I think this should return an interface{}, so we can return both Parity and Geth traces
func (api *TraceAPIImpl) Get(ctx context.Context, txHash common.Hash, indicies []hexutil.Uint64) (*ParityTrace, error) {
	// TODO(tjayrush): Parity fails if it gets more than a single index. Returns nothing in this case.
	if len(indicies) > 1 {
		return nil, nil
	}

	traces, err := api.getTransactionTraces(ctx, txHash)
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

// Block Implements trace_block
func (api *TraceAPIImpl) Block(ctx context.Context, blockNr rpc.BlockNumber) (ParityTraces, error) {
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

// Filter Implements trace_filter
// TODO(tjayrush): Eventually, we will need to protect ourselves from 'large' queries. Parity crashes when a range query of a very large size
// is sent. We need to protect ourselves with maxTraces. It may already be done
func (api *TraceAPIImpl) Filter(ctx context.Context, req TraceFilterRequest) (ParityTraces, error) {
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
		headNumber := rawdb.ReadHeaderNumber(api.dbReader, rawdb.ReadHeadHeaderHash(api.dbReader))
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
						if uint64(len(filteredHashes)) == maxTracesCount {
							if uint64(len(filteredHashes)) == api.maxTraces {
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
								filteredHashes = append(filteredHashes, tx.Hash())
								traceTypes = append(traceTypes, false)
							}
						} else if bytes.Equal(to.Bytes(), addrBytes) {
							if skipped < offset {
								skipped++
								continue
							}
							filteredHashes = append(filteredHashes, tx.Hash())
							traceTypes = append(traceTypes, false)
						}
					}
					// TODO(tjayrush): Parity does not (for some unknown reason) include blockReward traces here
				}
			}
		} else if req.FromBlock != nil || req.ToBlock != nil { // iterate over blocks

			for blockNum := fromBlock; blockNum < toBlock+1; blockNum++ {
				block := rawdb.ReadBlockByNumber(api.dbReader, blockNum)
				blockTransactions := block.Transactions()
				for _, tx := range blockTransactions {
					if uint64(len(filteredHashes)) == maxTracesCount {
						if uint64(len(filteredHashes)) == api.maxTraces {
							return fmt.Errorf("too many traces found")
						}
						return nil
					}
					if skipped < offset {
						skipped++
						continue
					}
					filteredHashes = append(filteredHashes, tx.Hash())
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
	traces := ParityTraces{}
	for i, txOrBlockHash := range filteredHashes {
		if traceTypes[i] {
			// In this case, we're processing a block (or uncle) reward trace. The hash is a block hash
			// Because Geth does not return blockReward or uncleReward traces, we must create them here
			block := rawdb.ReadBlockByHash(api.dbReader, txOrBlockHash)
			minerReward, uncleRewards := ethash.AccumulateRewards(chainConfig, block.Header(), block.Uncles())
			var tr ParityTrace
			tr.Action.Author = strings.ToLower(block.Coinbase().String())
			tr.Action.RewardType = "block" // goconst
			tr.Action.Value = minerReward.String()
			tr.BlockHash = block.Hash()
			tr.BlockNumber = block.NumberU64()
			tr.Type = "reward" // nolint: goconst
			traces = append(traces, tr)
			for i, uncle := range block.Uncles() {
				if i < len(uncleRewards) {
					var tr ParityTrace
					tr.Action.Author = strings.ToLower(uncle.Coinbase.String())
					tr.Action.RewardType = "uncle" // goconst
					tr.Action.Value = uncleRewards[i].String()
					tr.BlockHash = block.Hash()
					tr.BlockNumber = block.NumberU64()
					tr.Type = "reward" // nolint: goconst
					traces = append(traces, tr)
				}
			}
		} else {
			// In this case, we're processing a transaction hash
			tx, blockHash, blockNumber, txIndex := rawdb.ReadTransaction(api.dbReader, txOrBlockHash)
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
			traces = append(traces, converted...)
		}
	}
	return traces, nil
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

// getTransactionTraces - returns the traces for a single transaction. Used by trace_get and trace_transaction.
// TODO(tjayrush):
// Implementation Notes:
// -- For convienience, we return both Parity and Geth traces for now. In the future we will either separate
//    these functions or eliminate Geth traces
// -- The function convertToParityTraces takes a hierarchical Geth trace and returns a flattened Parity trace
func (api *TraceAPIImpl) getTransactionTraces(ctx context.Context, txHash common.Hash) (ParityTraces, error) {
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

	traces := ParityTraces{}
	converted := api.convertToParityTrace(gethTrace, blockHash, blockNumber, tx, txIndex, []int{})
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
