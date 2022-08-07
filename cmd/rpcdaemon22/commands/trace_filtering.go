package commands

import (
	"context"
	"fmt"
	"sort"

	"github.com/RoaringBitmap/roaring/roaring64"
	jsoniter "github.com/json-iterator/go"
	"github.com/ledgerwatch/erigon-lib/kv"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/turbo/transactions"
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

	blockNumber, ok, err := api.txnLookup(ctx, tx, txHash)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	block, err := api.blockByNumberWithSenders(tx, blockNumber)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}

	// Extract transactions from block
	block, bErr := api.blockByNumberWithSenders(tx, blockNumber)
	if bErr != nil {
		return nil, bErr
	}
	if block == nil {
		return nil, fmt.Errorf("could not find block  %d", blockNumber)
	}
	var txIndex int
	for idx, txn := range block.Transactions() {
		if txn.Hash() == txHash {
			txIndex = idx
			break
		}
	}
	bn := hexutil.Uint64(blockNumber)

	parentNr := bn
	if parentNr > 0 {
		parentNr -= 1
	}
	hash := block.Hash()

	// Returns an array of trace arrays, one trace array for each transaction
	traces, err := api.callManyTransactions(ctx, tx, block.Transactions(), []string{TraceTypeTrace}, block.ParentHash(), rpc.BlockNumber(parentNr), block.Header(), txIndex, types.MakeSigner(chainConfig, blockNumber), chainConfig.Rules(blockNumber))
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
	if blockNum == 0 {
		return []ParityTrace{}, nil
	}
	bn := hexutil.Uint64(blockNum)

	// Extract transactions from block
	block, bErr := api.blockByNumberWithSenders(tx, blockNum)
	if bErr != nil {
		return nil, bErr
	}
	if block == nil {
		return nil, fmt.Errorf("could not find block %d", uint64(bn))
	}
	hash := block.Hash()

	parentNr := bn
	if parentNr > 0 {
		parentNr -= 1
	}

	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}
	traces, err := api.callManyTransactions(ctx, tx, block.Transactions(), []string{TraceTypeTrace}, block.ParentHash(), rpc.BlockNumber(parentNr), block.Header(), -1 /* all tx indices */, types.MakeSigner(chainConfig, blockNum), chainConfig.Rules(blockNum))
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
		return fmt.Errorf("traceFilter cannot open tx: %w", err1)
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

	var fromTxNum, toTxNum uint64
	if fromBlock > 0 {
		fromTxNum = api._txNums[fromBlock-1]
	}
	toTxNum = api._txNums[toBlock] // toBlock is an inclusive bound

	if fromBlock > toBlock {
		stream.WriteNil()
		return fmt.Errorf("invalid parameters: fromBlock cannot be greater than toBlock")
	}

	fromAddresses := make(map[common.Address]struct{}, len(req.FromAddress))
	toAddresses := make(map[common.Address]struct{}, len(req.ToAddress))

	var (
		allTxs roaring64.Bitmap
		txsTo  roaring64.Bitmap
	)
	ac := api._agg.MakeContext()

	for _, addr := range req.FromAddress {
		if addr != nil {
			it := ac.TraceFromIterator(addr.Bytes(), fromTxNum, toTxNum, nil)
			for it.HasNext() {
				allTxs.Add(it.Next())
			}
			fromAddresses[*addr] = struct{}{}
		}
	}

	for _, addr := range req.ToAddress {
		if addr != nil {
			it := ac.TraceToIterator(addr.Bytes(), fromTxNum, toTxNum, nil)
			for it.HasNext() {
				txsTo.Add(it.Next())
			}
			toAddresses[*addr] = struct{}{}
		}
	}

	switch req.Mode {
	case TraceFilterModeIntersection:
		allTxs.And(&txsTo)
	case TraceFilterModeUnion:
		fallthrough
	default:
		allTxs.Or(&txsTo)
	}

	// Special case - if no addresses specified, take all traces
	if len(req.FromAddress) == 0 && len(req.ToAddress) == 0 {
		allTxs.AddRange(fromTxNum, toTxNum+1)
	} else {
		allTxs.RemoveRange(0, fromTxNum)
		allTxs.RemoveRange(toTxNum, uint64(0x1000000000000))
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
	includeAll := len(fromAddresses) == 0 && len(toAddresses) == 0
	it := allTxs.Iterator()
	var lastBlockNum uint64
	var lastBlockHash common.Hash
	var lastHeader *types.Header
	var lastSigner *types.Signer
	var lastRules *params.Rules
	stateReader := state.NewHistoryReader22(ac, nil /* ReadIndices */)
	noop := state.NewNoopWriter()
	for it.HasNext() {
		txNum := uint64(it.Next())
		// Find block number
		blockNum := uint64(sort.Search(len(api._txNums), func(i int) bool {
			return api._txNums[i] > txNum
		}))
		if blockNum > lastBlockNum {
			if lastHeader, err = api._blockReader.HeaderByNumber(ctx, nil, blockNum); err != nil {
				stream.WriteNil()
				return err
			}
			lastBlockNum = blockNum
			lastBlockHash = lastHeader.Hash()
			lastSigner = types.MakeSigner(chainConfig, blockNum)
			lastRules = chainConfig.Rules(blockNum)
		}
		if txNum+1 == api._txNums[blockNum] {
			body, _, err := api._blockReader.Body(ctx, nil, lastBlockHash, blockNum)
			if err != nil {
				stream.WriteNil()
				return err
			}
			// Block reward section, handle specially
			minerReward, uncleRewards := ethash.AccumulateRewards(chainConfig, lastHeader, body.Uncles)
			if _, ok := toAddresses[lastHeader.Coinbase]; ok || includeAll {
				nSeen++
				var tr ParityTrace
				var rewardAction = &RewardTraceAction{}
				rewardAction.Author = lastHeader.Coinbase
				rewardAction.RewardType = "block" // nolint: goconst
				rewardAction.Value.ToInt().Set(minerReward.ToBig())
				tr.Action = rewardAction
				tr.BlockHash = &common.Hash{}
				copy(tr.BlockHash[:], lastBlockHash.Bytes())
				tr.BlockNumber = new(uint64)
				*tr.BlockNumber = blockNum
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
			for i, uncle := range body.Uncles {
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
						copy(tr.BlockHash[:], lastBlockHash[:])
						tr.BlockNumber = new(uint64)
						*tr.BlockNumber = blockNum
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
			continue
		}
		var startTxNum uint64
		if blockNum > 0 {
			startTxNum = api._txNums[blockNum-1]
		}
		txIndex := txNum - startTxNum - 1
		//fmt.Printf("txNum=%d, blockNum=%d, txIndex=%d\n", txNum, blockNum, txIndex)
		txn, err := api._txnReader.TxnByIdxInBlock(ctx, nil, blockNum, int(txIndex))
		if err != nil {
			stream.WriteNil()
			return err
		}
		txHash := txn.Hash()
		msg, err := txn.AsMessage(*lastSigner, lastHeader.BaseFee, lastRules)
		if err != nil {
			stream.WriteNil()
			return err
		}
		contractHasTEVM := func(contractHash common.Hash) (bool, error) { return false, nil }
		blockCtx, txCtx := transactions.GetEvmContext(msg, lastHeader, true /* requireCanonical */, dbtx, contractHasTEVM, api._blockReader)
		stateReader.SetTxNum(txNum)
		stateCache := shards.NewStateCache(32, 0 /* no limit */) // this cache living only during current RPC call, but required to store state writes
		cachedReader := state.NewCachedReader(stateReader, stateCache)
		cachedWriter := state.NewCachedWriter(noop, stateCache)
		vmConfig := vm.Config{}
		vmConfig.SkipAnalysis = core.SkipAnalysis(chainConfig, blockNum)
		traceResult := &TraceCallResult{Trace: []*ParityTrace{}}
		var ot OeTracer
		ot.compat = api.compatibility
		ot.r = traceResult
		ot.idx = []string{fmt.Sprintf("%d-", txIndex)}
		ot.traceAddr = []int{}
		vmConfig.Debug = true
		vmConfig.Tracer = &ot
		ibs := state.New(cachedReader)
		evm := vm.NewEVM(blockCtx, txCtx, ibs, chainConfig, vmConfig)

		gp := new(core.GasPool).AddGas(msg.Gas())
		ibs.Prepare(txHash, lastBlockHash, int(txIndex))
		var execResult *core.ExecutionResult
		execResult, err = core.ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */)
		if err != nil {
			stream.WriteNil()
			return err
		}
		traceResult.Output = common.CopyBytes(execResult.ReturnData)
		if err = ibs.FinalizeTx(evm.ChainRules(), noop); err != nil {
			stream.WriteNil()
			return err
		}
		if err = ibs.CommitBlock(evm.ChainRules(), cachedWriter); err != nil {
			stream.WriteNil()
			return err
		}
		for _, pt := range traceResult.Trace {
			if includeAll || filter_trace(pt, fromAddresses, toAddresses) {
				nSeen++
				pt.BlockHash = &lastBlockHash
				pt.BlockNumber = &blockNum
				pt.TransactionHash = &txHash
				pt.TransactionPosition = &txIndex
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

func (api *TraceAPIImpl) callManyTransactions(ctx context.Context, dbtx kv.Tx, txs []types.Transaction, traceTypes []string, parentHash common.Hash, parentNo rpc.BlockNumber, header *types.Header, txIndex int, signer *types.Signer, rules *params.Rules) ([]*TraceCallResult, error) {
	callParams := make([]TraceCallParam, 0, len(txs))
	msgs := make([]types.Message, len(txs))
	for i, tx := range txs {
		hash := tx.Hash()
		callParams = append(callParams, TraceCallParam{
			txHash:     &hash,
			traceTypes: traceTypes,
		})
		var err error
		if msgs[i], err = tx.AsMessage(*signer, header.BaseFee, rules); err != nil {
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
	Mode        TraceFilterMode   `json:"mode"`
	After       *uint64           `json:"after"`
	Count       *uint64           `json:"count"`
}

type TraceFilterMode string

const (
	// Default mode for TraceFilter. Unions results referred to addresses from FromAddress or ToAddress
	TraceFilterModeUnion = "union"
	// IntersectionMode retrives results referred to addresses provided both in FromAddress and ToAddress
	TraceFilterModeIntersection = "intersection"
)
