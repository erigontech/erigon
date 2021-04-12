package commands

import (
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
// TODO(tjayrush): I think this should return an []interface{}, so we can return both Parity and Geth traces
func (api *TraceAPIImpl) Transaction(ctx context.Context, txHash common.Hash) (ParityTraces, error) {
	tx, err := api.kv.BeginRo(ctx)
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
	tx, err := api.kv.BeginRo(ctx)
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
	dbtx, err := api.kv.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer dbtx.Rollback()
	blockNum, err := getBlockNumber(blockNr, dbtx)
	if err != nil {
		return nil, err
	}
	bn := hexutil.Uint64(blockNum)

	// Extract transactions from block
	hash, hashErr := rawdb.ReadCanonicalHash(dbtx, blockNum)
	if hashErr != nil {
		return nil, hashErr
	}

	txs, txsErr := getBlockTransactions(dbtx, hash, blockNum)
	if txsErr != nil {
		return nil, txsErr
	}

	baseBn := bn
	if baseBn > 0 {
		baseBn -= 1
	}

	traces, err := api.callManyTransactions(ctx, dbtx, txs, hash, rpc.BlockNumber(baseBn))
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

	return out, err
}

// Filter implements trace_filter
// NOTE: We do not store full traces - we just store index for each address
// Pull blocks which have txs with matching address
func (api *TraceAPIImpl) Filter(ctx context.Context, req TraceFilterRequest) (ParityTraces, error) {
	dbtx, err1 := api.kv.BeginRo(ctx)
	if err1 != nil {
		return nil, fmt.Errorf("traceFilter cannot open tx: %v", err1)
	}
	defer dbtx.Rollback()

	var fromBlock uint64
	var toBlock uint64
	if req.FromBlock == nil {
		fromBlock = 0
	} else {
		fromBlock = uint64(*req.FromBlock)
	}

	if req.ToBlock == nil {
		headNumber := rawdb.ReadHeaderNumber(dbtx, rawdb.ReadHeadHeaderHash(dbtx))
		toBlock = *headNumber
	} else {
		toBlock = uint64(*req.ToBlock)
	}

	if fromBlock > toBlock {
		return nil, fmt.Errorf("invalid parameters: fromBlock cannot be greater than toBlock")
	}

	fromAddresses := make(map[common.Address]struct{}, len(req.FromAddress))
	toAddresses := make(map[common.Address]struct{}, len(req.ToAddress))

	blocksMap := map[uint32]struct{}{}
	var loadAddresses = func(addr common.Address) error {
		// Load bitmap for address from trace index
		b, err := bitmapdb.Get(dbtx, dbutils.CallFromIndex, addr.Bytes(), uint32(fromBlock), uint32(toBlock))
		if err != nil {
			return err
		}

		// Extract block numbers from bitmap
		for _, block := range b.ToArray() {
			// Observe the limits
			if uint64(block) >= fromBlock && uint64(block) <= toBlock {
				blocksMap[block] = struct{}{}
			}
		}

		return nil
	}

	for _, addr := range req.FromAddress {
		if addr != nil {
			if err := loadAddresses(*addr); err != nil {
				return nil, err
			}

			fromAddresses[*addr] = struct{}{}
		}
	}

	for _, addr := range req.ToAddress {
		if addr != nil {
			if err := loadAddresses(*addr); err != nil {
				return nil, err
			}

			toAddresses[*addr] = struct{}{}
		}
	}

	// Sort blocks
	blockSet := make([]int, 0, len(blocksMap))
	for blk := range blocksMap {
		blockSet = append(blockSet, int(blk))
	}
	sort.Ints(blockSet)

	type blk struct {
		blockNr   int
		blockHash common.Hash
		txs       []TransactionWithSender
	}

	blocks := make([]blk, 0, len(blocksMap))
	for _, b := range blockSet {
		// Extract transactions from block
		hash, hashErr := rawdb.ReadCanonicalHash(dbtx, uint64(b))
		if hashErr != nil {
			return nil, hashErr
		}

		t, tErr := getBlockTransactions(dbtx, hash, uint64(b))
		if tErr != nil {
			return nil, tErr
		}

		blocks = append(blocks, blk{
			blockNr:   b,
			blockHash: hash,
			txs:       t,
		})
	}

	traces := []ParityTrace{}

	// Execute all transactions in picked blocks
	for _, s := range blocks {
		t, tErr := api.callManyTransactions(ctx, dbtx, s.txs, s.blockHash, rpc.BlockNumber(s.blockNr))
		if tErr != nil {
			return nil, tErr
		}

		for _, trace := range t {
			// Check if transaction concerns any of the addresses we wanted
			if filter_trace(trace, fromAddresses, toAddresses) {
				for _, pt := range trace.Trace {
					traces = append(traces, *pt)
				}
			}
		}
	}

	return traces, nil
}

func filter_trace(trace *TraceCallResult, fromAddresses map[common.Address]struct{}, toAddresses map[common.Address]struct{}) bool {
	for _, pt := range trace.Trace {
		switch action := pt.Action.(type) {
		case CallTraceAction:
			_, f := fromAddresses[action.From]
			_, t := toAddresses[action.To]
			if f || t {
				return true
			}
		case CreateTraceAction:
			_, f := fromAddresses[action.From]
			if f {
				return true
			}

			if res, ok := pt.Result.(CreateTraceResult); ok {
				if res.Address != nil {
					if _, t := fromAddresses[*res.Address]; t {
						return true
					}
				}
			}
		case SuicideTraceAction:
			_, f := fromAddresses[action.RefundAddress]
			_, t := toAddresses[action.Address]
			if f || t {
				return true
			}
		}

	}

	return false
}

type TransactionWithSender struct {
	tx     types.Transaction
	sender common.Address
}

func getBlockTransactions(dbtx ethdb.Tx, blockHash common.Hash, blockNo uint64) ([]TransactionWithSender, error) {
	block, senders, sendersErr := rawdb.ReadBlockWithSenders(ethdb.NewRoTxDb(dbtx), blockHash, blockNo)
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

	return txs, nil
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

// getTransactionTraces - returns the traces for a single transaction. Used by trace_get and trace_transaction.
// TODO(tjayrush):
// Implementation Notes:
// -- For convienience, we return both Parity and Geth traces for now. In the future we will either separate
//    these functions or eliminate Geth traces
// -- The function convertToParityTraces takes a hierarchical Geth trace and returns a flattened Parity trace
func (api *TraceAPIImpl) getTransactionTraces(tx ethdb.Tx, ctx context.Context, txHash common.Hash) (ParityTraces, error) {
	getter := adapter.NewBlockGetter(tx)
	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}
	traceType := "callTracer" // nolint: goconst

	txn, blockHash, blockNumber, txIndex := rawdb.ReadTransaction(ethdb.NewRoTxDb(tx), txHash)

	getHeader := func(hash common.Hash, number uint64) *types.Header {
		return rawdb.ReadHeader(tx, hash, number)
	}
	msg, blockCtx, txCtx, ibs, _, err := transactions.ComputeTxEnv(ctx, getter, chainConfig, getHeader, ethash.NewFaker(), tx, blockHash, txIndex)
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
