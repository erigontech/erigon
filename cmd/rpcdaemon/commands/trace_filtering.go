package commands

import (
	"context"
	"fmt"

	"github.com/RoaringBitmap/roaring/roaring64"
	jsoniter "github.com/json-iterator/go"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/ethdb/bitmapdb"
	"github.com/ledgerwatch/erigon/rpc"
)

// Transaction implements trace_transaction
func (api *TraceAPIImpl) Transaction(ctx context.Context, txHash common.Hash) (ParityTraces, error) {
	tx, err := api.kv.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}

	blockNumber, err := rawdb.ReadTxLookupEntry(tx, txHash)
	if err != nil {
		return nil, err
	}
	if blockNumber == nil {
		return nil, nil // not error, see https://github.com/ledgerwatch/erigon/issues/1645
	}

	// Extract transactions from block
	block, _, bErr := rawdb.ReadBlockByNumberWithSenders(tx, *blockNumber)
	if bErr != nil {
		return nil, bErr
	}
	if block == nil {
		return nil, fmt.Errorf("could not find block  %d", *blockNumber)
	}
	var txIndex int
	for idx, txn := range block.Transactions() {
		if txn.Hash() == txHash {
			txIndex = idx
			break
		}
	}
	bn := hexutil.Uint64(*blockNumber)

	parentNr := bn
	if parentNr > 0 {
		parentNr -= 1
	}
	hash := block.Hash()

	// Returns an array of trace arrays, one trace array for each transaction
	traces, err := api.callManyTransactions(ctx, tx, block.Transactions(), []string{TraceTypeTrace}, block.ParentHash(), rpc.BlockNumber(parentNr), block.Header(), txIndex, types.MakeSigner(chainConfig, *blockNumber))
	if err != nil {
		return nil, err
	}

	out := make([]ParityTrace, 0, len(traces))
	blockno := uint64(bn)
	for txno, trace := range traces {
		txhash := block.Transactions()[txno].Hash()
		// We're only looking for a specific transaction
		if txno == txIndex {
			for _, pt := range trace.Trace {
				pt.BlockHash = &hash
				pt.BlockNumber = &blockno
				pt.TransactionHash = &txhash
				txpos := uint64(txno)
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
	block, _, bErr := rawdb.ReadBlockWithSenders(tx, hash, uint64(bn))
	if bErr != nil {
		return nil, bErr
	}
	if block == nil {
		return nil, fmt.Errorf("could not find block %x %d", hash, uint64(bn))
	}

	parentNr := bn
	if parentNr > 0 {
		parentNr -= 1
	}

	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}
	traces, err := api.callManyTransactions(ctx, tx, block.Transactions(), []string{TraceTypeTrace}, block.ParentHash(), rpc.BlockNumber(parentNr), block.Header(), -1 /* all tx indices */, types.MakeSigner(chainConfig, blockNum))
	if err != nil {
		return nil, err
	}

	out := make([]ParityTrace, 0, len(traces))
	blockno := uint64(bn)
	for txno, trace := range traces {
		txhash := block.Transactions()[txno].Hash()
		txpos := uint64(txno)
		for _, pt := range trace.Trace {
			pt.BlockHash = &hash
			pt.BlockNumber = &blockno
			pt.TransactionHash = &txhash
			pt.TransactionPosition = &txpos
			out = append(out, *pt)
		}
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
// NOTE: We do not store full traces - we just store index for each address
// Pull blocks which have txs with matching address
func (api *TraceAPIImpl) Filter(ctx context.Context, req TraceFilterRequest, stream *jsoniter.Stream) error {
	dbtx, err1 := api.kv.BeginRo(ctx)
	if err1 != nil {
		stream.WriteNil()
		return fmt.Errorf("traceFilter cannot open tx: %v", err1)
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
		stream.WriteNil()
		return fmt.Errorf("invalid parameters: fromBlock cannot be greater than toBlock")
	}

	fromAddresses := make(map[common.Address]struct{}, len(req.FromAddress))
	toAddresses := make(map[common.Address]struct{}, len(req.ToAddress))

	var allBlocks roaring64.Bitmap
	for _, addr := range req.FromAddress {
		if addr != nil {
			b, err := bitmapdb.Get64(dbtx, kv.CallFromIndex, addr.Bytes(), fromBlock, toBlock)
			if err != nil {
				stream.WriteNil()
				return err
			}
			allBlocks.Or(b)
			fromAddresses[*addr] = struct{}{}
		}
	}
	for _, addr := range req.ToAddress {
		if addr != nil {
			b, err := bitmapdb.Get64(dbtx, kv.CallToIndex, addr.Bytes(), fromBlock, toBlock)
			if err != nil {
				stream.WriteNil()
				return err
			}
			allBlocks.Or(b)
			toAddresses[*addr] = struct{}{}
		}
	}
	// Special case - if no addresses specified, take all traces
	if len(req.FromAddress) == 0 && len(req.ToAddress) == 0 {
		allBlocks.AddRange(fromBlock, toBlock+1)
	} else {
		allBlocks.RemoveRange(0, fromBlock)
		allBlocks.RemoveRange(toBlock+1, uint64(0x100000000))
	}

	chainConfig, err := api.chainConfig(dbtx)
	if err != nil {
		stream.WriteNil()
		return err
	}
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	stream.WriteArrayStart()
	first := true
	// Execute all transactions in picked blocks

	count := uint64(^uint(0)) // this just makes it easier to use below
	if req.Count != nil {
		count = *req.Count
	}
	after := uint64(0) // this just makes it easier to use below
	if req.After != nil {
		after = *req.After
	}
	nSeen := uint64(0)
	nExported := uint64(0)

	it := allBlocks.Iterator()
	for it.HasNext() {
		b := uint64(it.Next())
		// Extract transactions from block
		hash, hashErr := rawdb.ReadCanonicalHash(dbtx, b)
		if hashErr != nil {
			stream.WriteNil()
			return hashErr
		}

		block, _, bErr := rawdb.ReadBlockWithSenders(dbtx, hash, b)
		if bErr != nil {
			stream.WriteNil()
			return bErr
		}
		if block == nil {
			stream.WriteNil()
			return fmt.Errorf("could not find block %x %d", hash, b)
		}

		blockHash := block.Hash()
		blockNumber := block.NumberU64()
		txs := block.Transactions()
		t, tErr := api.callManyTransactions(ctx, dbtx, txs, []string{TraceTypeTrace}, block.ParentHash(), rpc.BlockNumber(block.NumberU64()-1), block.Header(), -1 /* all tx indices */, types.MakeSigner(chainConfig, b))
		if tErr != nil {
			stream.WriteNil()
			return tErr
		}
		includeAll := len(fromAddresses) == 0 && len(toAddresses) == 0
		for i, trace := range t {
			txPosition := uint64(i)
			txHash := txs[i].Hash()
			// Check if transaction concerns any of the addresses we wanted
			for _, pt := range trace.Trace {
				if includeAll || filter_trace(pt, fromAddresses, toAddresses) {
					nSeen++
					pt.BlockHash = &blockHash
					pt.BlockNumber = &blockNumber
					pt.TransactionHash = &txHash
					pt.TransactionPosition = &txPosition
					b, err := json.Marshal(pt)
					if err != nil {
						stream.WriteNil()
						return err
					}
					if nSeen > after && nExported < count {
						if first {
							first = false
						} else {
							stream.WriteMore()
						}
						stream.Write(b)
						nExported++
					}
				}
			}
		}
		minerReward, uncleRewards := ethash.AccumulateRewards(chainConfig, block.Header(), block.Uncles())
		if _, ok := toAddresses[block.Coinbase()]; ok || includeAll {
			nSeen++
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
			b, err := json.Marshal(tr)
			if err != nil {
				stream.WriteNil()
				return err
			}
			if nSeen > after && nExported < count {
				if first {
					first = false
				} else {
					stream.WriteMore()
				}
				stream.Write(b)
				nExported++
			}
		}
		for i, uncle := range block.Uncles() {
			if _, ok := toAddresses[uncle.Coinbase]; ok || includeAll {
				if i < len(uncleRewards) {
					nSeen++
					var tr ParityTrace
					rewardAction := &RewardTraceAction{}
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
					b, err := json.Marshal(tr)
					if err != nil {
						stream.WriteNil()
						return err
					}
					if nSeen > after && nExported < count {
						if first {
							first = false
						} else {
							stream.WriteMore()
						}
						stream.Write(b)
						nExported++
					}
				}
			}
		}
	}
	stream.WriteArrayEnd()
	return stream.Flush()
}

func filter_trace(pt *ParityTrace, fromAddresses map[common.Address]struct{}, toAddresses map[common.Address]struct{}) bool {
	switch action := pt.Action.(type) {
	case *CallTraceAction:
		_, f := fromAddresses[action.From]
		_, t := toAddresses[action.To]
		if f || t {
			return true
		}
	case *CreateTraceAction:
		_, f := fromAddresses[action.From]
		if f {
			return true
		}

		if res, ok := pt.Result.(*CreateTraceResult); ok {
			if res.Address != nil {
				if _, t := toAddresses[*res.Address]; t {
					return true
				}
			}
		}
	case *SuicideTraceAction:
		_, f := fromAddresses[action.Address]
		_, t := toAddresses[action.RefundAddress]
		if f || t {
			return true
		}
	}

	return false
}

func (api *TraceAPIImpl) callManyTransactions(ctx context.Context, dbtx kv.Tx, txs []types.Transaction, traceTypes []string, parentHash common.Hash, parentNo rpc.BlockNumber, header *types.Header, txIndex int, signer *types.Signer) ([]*TraceCallResult, error) {
	callParams := make([]TraceCallParam, 0, len(txs))
	msgs := make([]types.Message, len(txs))
	for i, tx := range txs {
		hash := tx.Hash()
		callParams = append(callParams, TraceCallParam{
			txHash:     &hash,
			traceTypes: traceTypes,
		})
		var err error
		if msgs[i], err = tx.AsMessage(*signer, header.BaseFee); err != nil {
			return nil, fmt.Errorf("convert tx into msg: %w", err)
		}
	}

	traces, cmErr := api.doCallMany(ctx, dbtx, msgs, callParams, &rpc.BlockNumberOrHash{
		BlockNumber:      &parentNo,
		BlockHash:        &parentHash,
		RequireCanonical: true,
	}, header, false /* gasBailout */, txIndex)

	if cmErr != nil {
		return nil, cmErr
	}

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
