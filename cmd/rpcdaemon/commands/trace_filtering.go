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
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/bitmapdb"
	"github.com/ledgerwatch/turbo-geth/rpc"
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
	block, senders, sendersErr := rawdb.ReadBlockWithSenders(dbtx, blockHash, blockNo)
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

// TraceFilterRequest represents the arguments for trace_filter
type TraceFilterRequest struct {
	FromBlock   *hexutil.Uint64   `json:"fromBlock"`
	ToBlock     *hexutil.Uint64   `json:"toBlock"`
	FromAddress []*common.Address `json:"fromAddress"`
	ToAddress   []*common.Address `json:"toAddress"`
	After       *uint64           `json:"after"`
	Count       *uint64           `json:"count"`
}
