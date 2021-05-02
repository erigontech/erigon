package commands

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth/tracers"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/bitmapdb"
	"github.com/ledgerwatch/turbo-geth/rpc"
	"github.com/ledgerwatch/turbo-geth/turbo/adapter"
	"github.com/ledgerwatch/turbo-geth/turbo/transactions"
)

// Transaction implements trace_transaction
func (api *TraceAPIImpl) Transaction(ctx context.Context, txHash common.Hash) (ParityTraces, error) {
	tx, err := api.kv.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	txn, _, blockNumber, txIndex := rawdb.ReadTransaction(tx, txHash)
	if txn == nil {
		return nil, nil // not error, see https://github.com/ledgerwatch/turbo-geth/issues/1645
	}

	bn := hexutil.Uint64(blockNumber)

	// Extract transactions from block
	hash, hashErr := rawdb.ReadCanonicalHash(tx, blockNumber)
	if hashErr != nil {
		return nil, hashErr
	}
	block, senders, sendersErr := rawdb.ReadBlockWithSenders(tx, hash, uint64(bn))
	if sendersErr != nil {
		return nil, sendersErr
	}
	if block == nil {
		return nil, nil
	}

	blockTxs := block.Transactions()
	if len(blockTxs) != len(senders) {
		return nil, errors.New("block txs len != senders len")
	}

	txs := make([]TransactionWithSender, 0, len(senders))
	for n, tx := range blockTxs {
		if uint64(n) <= txIndex {
			txs = append(txs, TransactionWithSender{
				tx:     tx,
				sender: senders[n],
			})
		}
	}

	baseBn := bn
	if baseBn > 0 {
		baseBn -= 1
	}

	// Returns an array of trace arrays, one trace array for each transaction
	traces, err := api.callManyTransactions(ctx, tx, txs, hash, rpc.BlockNumber(baseBn))
	if err != nil {
		return nil, err
	}

	out := make([]ParityTrace, 0, len(traces))
	blockno := uint64(bn)
	for txno, trace := range traces {
		txhash := txs[txno].tx.Hash()
		txpos := uint64(txno)
		// We're only looking for a specific transaction
		if txpos == txIndex {
			for _, pt := range trace.Trace {
				pt.BlockHash = &hash
				pt.BlockNumber = &blockno
				pt.TransactionHash = &txhash
				pt.TransactionPosition = &txpos
				out = append(out, *pt)
			}
		}
	}

	return out, err
}

// Get implements trace_get
func (api *TraceAPIImpl) Get(ctx context.Context, txHash common.Hash, indicies []hexutil.Uint64) (*ParityTrace, error) {
	// Parity fails if it gets more than a single index. It returns nothing in this case. Must we?
	if len(indicies) > 1 {
		return nil, nil
	}

	traces, err := api.Transaction(ctx, txHash)
	if err != nil {
		return nil, err
	}

	// 'trace_get' index starts at one (oddly)
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
	tx, err := api.kv.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	blockNum, err := getBlockNumber(blockNr, tx)
	if err != nil {
		return nil, err
	}
	bn := hexutil.Uint64(blockNum)

	// Extract transactions from block
	hash, hashErr := rawdb.ReadCanonicalHash(tx, blockNum)
	if hashErr != nil {
		return nil, hashErr
	}
	block, senders, sendersErr := rawdb.ReadBlockWithSenders(tx, hash, uint64(bn))
	if sendersErr != nil {
		return nil, sendersErr
	}
	if block == nil {
		return nil, nil
	}

	blockTxs := block.Transactions()

	if len(blockTxs) != len(senders) {
		return nil, errors.New("block txs len != senders len")
	}

	txs := make([]TransactionWithSender, 0, len(senders))
	for n, tx := range blockTxs {
		txs = append(txs, TransactionWithSender{
			tx:     tx,
			sender: senders[n],
		})
	}

	baseBn := bn
	if baseBn > 0 {
		baseBn -= 1
	}

	traces, err := api.callManyTransactions(ctx, tx, txs, hash, rpc.BlockNumber(baseBn))
	if err != nil {
		return nil, err
	}

	out := make([]ParityTrace, 0, len(traces))
	blockno := uint64(bn)
	for txno, trace := range traces {
		txhash := txs[txno].tx.Hash()
		txpos := uint64(txno)
		for _, pt := range trace.Trace {
			pt.BlockHash = &hash
			pt.BlockNumber = &blockno
			pt.TransactionHash = &txhash
			pt.TransactionPosition = &txpos
			out = append(out, *pt)
		}
	}
	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}
	minerReward, uncleRewards := ethash.AccumulateRewards(chainConfig, block.Header(), block.Uncles())
	var tr ParityTrace
	var rewardAction = &RewardTraceAction{}
	rewardAction.Author = block.Coinbase()
	rewardAction.RewardType = "block" // nolint: goconst
	rewardAction.Value.ToInt().Set(minerReward.ToBig())
	tr.Action = rewardAction
	tr.BlockHash = &common.Hash{}
	copy(tr.BlockHash[:], block.Hash().Bytes())
	tr.BlockNumber = new(uint64)
	*tr.BlockNumber = block.NumberU64()
	tr.Type = "reward" // nolint: goconst
	tr.TraceAddress = []int{}
	out = append(out, tr)
	for i, uncle := range block.Uncles() {
		if i < len(uncleRewards) {
			var tr ParityTrace
			rewardAction = &RewardTraceAction{}
			rewardAction.Author = uncle.Coinbase
			rewardAction.RewardType = "uncle" // nolint: goconst
			rewardAction.Value.ToInt().Set(uncleRewards[i].ToBig())
			tr.Action = rewardAction
			tr.BlockHash = &common.Hash{}
			copy(tr.BlockHash[:], block.Hash().Bytes())
			tr.BlockNumber = new(uint64)
			*tr.BlockNumber = block.NumberU64()
			tr.Type = "reward" // nolint: goconst
			tr.TraceAddress = []int{}
			out = append(out, tr)
		}
	}

	return out, err
}

// Filter implements trace_filter
// TODO(tjayrush): Eventually, we will need to protect ourselves from 'large' queries. Parity crashes when a range query of a very large size
// is sent. We need to protect ourselves with maxTraces. It may already be done
func (api *TraceAPIImpl) Filter(ctx context.Context, req TraceFilterRequest) (ParityTraces, error) {
	tx, err1 := api.kv.BeginRo(ctx)
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
		headNumber := rawdb.ReadHeaderNumber(tx, rawdb.ReadHeadHeaderHash(tx))
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
			blockNumbers, errHistory := retrieveHistory(tx, addr, fromBlock, toBlock)
			if errHistory != nil {
				return nil, errHistory
			}

			for _, num := range blockNumbers {
				block, senders, err := rawdb.ReadBlockByNumberWithSenders(tx, num)
				if err != nil {
					return nil, err
				}
				for i, txn := range block.Transactions() {
					if uint64(len(filteredHashes)) == maxTracesCount {
						if uint64(len(filteredHashes)) == api.maxTraces {
							return nil, fmt.Errorf("too many traces found")
						}
						return nil, nil
					}

					var to *common.Address
					if txn.GetTo() == nil {
						to = &common.Address{}
					} else {
						to = txn.GetTo()
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
			block, err := rawdb.ReadBlockByNumber(tx, blockNum)
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
	engine := ethash.NewFaker()
	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}
	traceType := "callTracer" // nolint: goconst
	traces := ParityTraces{}
	getHeader := func(hash common.Hash, number uint64) *types.Header {
		return rawdb.ReadHeader(tx, hash, number)
	}
	for i, txOrBlockHash := range filteredHashes {
		if traceTypes[i] {
			// In this case, we're processing a block (or uncle) reward trace. The hash is a block hash
			// Because Geth does not return blockReward or uncleReward traces, we must create them here
			block, err := rawdb.ReadBlockByHash(tx, txOrBlockHash)
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
			txn, blockHash, blockNumber, txIndex := rawdb.ReadTransaction(tx, txOrBlockHash)
			msg, blockCtx, txCtx, ibs, _, err := transactions.ComputeTxEnv(ctx, getter, chainConfig, getHeader, engine, tx, blockHash, txIndex)
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

type TransactionWithSender struct {
	tx     types.Transaction
	sender common.Address
}

func (api *TraceAPIImpl) callManyTransactions(ctx context.Context, dbtx ethdb.Tx, txs []TransactionWithSender, blockHash common.Hash, blockNo rpc.BlockNumber) ([]*TraceCallResult, error) {
	toExecute := []interface{}{}

	for _, txWithSender := range txs {
		tx := txWithSender.tx
		sender := txWithSender.sender
		gas := hexutil.Uint64(tx.GetGas())
		gasPrice := hexutil.Big(*tx.GetPrice().ToBig())
		value := hexutil.Big(*tx.GetValue().ToBig())
		toExecute = append(toExecute, []interface{}{TraceCallParam{
			From:     &sender,
			To:       tx.GetTo(),
			Gas:      &gas,
			GasPrice: &gasPrice,
			Value:    &value,
			Data:     tx.GetData(),
		}, []string{TraceTypeTrace, TraceTypeStateDiff}})
	}

	calls, callsErr := json.Marshal(toExecute)
	if callsErr != nil {
		return nil, callsErr
	}
	traces, cmErr := api.doCallMany(ctx, dbtx, calls, &rpc.BlockNumberOrHash{
		BlockNumber:      &blockNo,
		BlockHash:        &blockHash,
		RequireCanonical: true,
	})

	if cmErr != nil {
		return nil, cmErr
	}

	return traces, nil
}

func retrieveHistory(tx ethdb.Tx, addr *common.Address, fromBlock uint64, toBlock uint64) ([]uint64, error) {
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

// TraceFilterRequest represents the arguments for trace_filter
type TraceFilterRequest struct {
	FromBlock   *hexutil.Uint64   `json:"fromBlock"`
	ToBlock     *hexutil.Uint64   `json:"toBlock"`
	FromAddress []*common.Address `json:"fromAddress"`
	ToAddress   []*common.Address `json:"toAddress"`
	After       *uint64           `json:"after"`
	Count       *uint64           `json:"count"`
}
