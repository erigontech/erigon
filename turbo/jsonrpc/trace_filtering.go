// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package jsonrpc

import (
	"context"
	"errors"
	"fmt"

	"github.com/erigontech/erigon/turbo/shards"
	jsoniter "github.com/json-iterator/go"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/order"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/kv/stream"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/consensus/ethash"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/eth/consensuschain"
	"github.com/erigontech/erigon/eth/tracers/config"
	"github.com/erigontech/erigon/ethdb"
	bortypes "github.com/erigontech/erigon/polygon/bor/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/turbo/rpchelper"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/turbo/transactions"
)

// Transaction implements trace_transaction
func (api *TraceAPIImpl) Transaction(ctx context.Context, txHash common.Hash, gasBailOut *bool, traceConfig *config.TraceConfig) (ParityTraces, error) {
	if gasBailOut == nil {
		gasBailOut = new(bool) // false by default
	}
	tx, err := api.kv.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}

	var isBorStateSyncTxn bool
	blockNumber, ok, err := api.txnLookup(ctx, tx, txHash)
	if err != nil {
		return nil, err
	}
	if !ok {
		if chainConfig.Bor == nil {
			return nil, nil
		}

		// otherwise this may be a bor state sync transaction - check
		if api.bridgeReader != nil {
			blockNumber, ok, err = api.bridgeReader.EventTxnLookup(ctx, txHash)
		} else {
			blockNumber, ok, err = api._blockReader.EventLookup(ctx, tx, txHash)
		}
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nil
		}

		isBorStateSyncTxn = true
	}

	block, err := api.blockByNumberWithSenders(ctx, tx, blockNumber)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}

	var txIndex int
	if isBorStateSyncTxn {
		txIndex = block.Transactions().Len()
	} else {
		var found bool
		for idx := 0; idx < block.Transactions().Len(); idx++ {
			txn := block.Transactions()[idx]
			if txn.Hash() == txHash {
				txIndex = idx
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("txn with hash %x belongs to currentely non-canonical block %d. only canonical blocks can be traced", txHash, block.NumberU64())
		}
	}

	bn := hexutil.Uint64(blockNumber)
	hash := block.Hash()
	signer := types.MakeSigner(chainConfig, blockNumber, block.Time())
	// Returns an array of trace arrays, one trace array for each transaction
	traces, _, err := api.callManyTransactions(ctx, tx, block, []string{TraceTypeTrace}, txIndex, *gasBailOut, signer, chainConfig, traceConfig)
	if err != nil {
		return nil, err
	}

	out := make([]ParityTrace, 0, len(traces))
	blockno := uint64(bn)
	for txno, trace := range traces {
		// We're only looking for a specific transaction
		if txno == txIndex {
			for _, pt := range trace.Trace {
				pt.BlockHash = &hash
				pt.BlockNumber = &blockno
				pt.TransactionHash = trace.TransactionHash
				txpos := uint64(txno)
				pt.TransactionPosition = &txpos
				out = append(out, *pt)
			}
		}
	}

	return out, err
}

// Get implements trace_get
func (api *TraceAPIImpl) Get(ctx context.Context, txHash common.Hash, indicies []hexutil.Uint64, gasBailOut *bool, traceConfig *config.TraceConfig) (*ParityTrace, error) {
	// Parity fails if it gets more than a single index. It returns nothing in this case. Must we?
	if len(indicies) > 1 {
		return nil, nil
	}
	traces, err := api.Transaction(ctx, txHash, gasBailOut, traceConfig)
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

func rewardKindToString(kind consensus.RewardKind) string {
	switch kind {
	case consensus.RewardAuthor:
		return "block"
	case consensus.RewardEmptyStep:
		return "emptyStep"
	case consensus.RewardExternal:
		return "external"
	case consensus.RewardUncle:
		return "uncle"
	default:
		return "unknown"
	}
}

// Block implements trace_block
func (api *TraceAPIImpl) Block(ctx context.Context, blockNr rpc.BlockNumber, gasBailOut *bool, traceConfig *config.TraceConfig) (ParityTraces, error) {
	if gasBailOut == nil {
		gasBailOut = new(bool) // false by default
	}
	tx, err := api.kv.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	blockNum, hash, _, err := rpchelper.GetBlockNumber(ctx, rpc.BlockNumberOrHashWithNumber(blockNr), tx, api._blockReader, api.filters)
	if err != nil {
		return nil, err
	}
	if blockNum == 0 {
		return []ParityTrace{}, nil
	}
	bn := hexutil.Uint64(blockNum)

	// Extract transactions from block
	block, bErr := api.blockWithSenders(ctx, tx, hash, blockNum)
	if bErr != nil {
		return nil, bErr
	}
	if block == nil {
		return nil, fmt.Errorf("could not find block %d", uint64(bn))
	}

	cfg, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}
	signer := types.MakeSigner(cfg, blockNum, block.Time())
	traces, syscall, err := api.callManyTransactions(ctx, tx, block, []string{TraceTypeTrace}, -1 /* all txn indices */, *gasBailOut /* gasBailOut */, signer, cfg, traceConfig)
	if err != nil {
		return nil, err
	}

	out := make([]ParityTrace, 0, len(traces))
	for txno, trace := range traces {
		txpos := uint64(txno)
		for _, pt := range trace.Trace {
			pt.BlockHash = &hash
			pt.BlockNumber = &blockNum
			pt.TransactionHash = trace.TransactionHash
			pt.TransactionPosition = &txpos
			out = append(out, *pt)
		}
	}

	rewards, err := api.engine().CalculateRewards(cfg, block.Header(), block.Uncles(), syscall)
	if err != nil {
		return nil, err
	}

	for _, r := range rewards {
		var tr ParityTrace
		rewardAction := &RewardTraceAction{}
		rewardAction.Author = r.Beneficiary
		rewardAction.RewardType = rewardKindToString(r.Kind)
		rewardAction.Value.ToInt().Set(r.Amount.ToBig())
		tr.Action = rewardAction
		tr.BlockHash = &common.Hash{}
		copy(tr.BlockHash[:], block.Hash().Bytes())
		tr.BlockNumber = new(uint64)
		*tr.BlockNumber = block.NumberU64()
		tr.Type = "reward" // nolint: goconst
		tr.TraceAddress = []int{}
		out = append(out, tr)
	}

	return out, err
}

func traceFilterBitmapsV3(tx kv.TemporalTx, req TraceFilterRequest, from, to uint64) (fromAddresses, toAddresses map[common.Address]struct{}, allBlocks stream.U64, err error) {
	fromAddresses = make(map[common.Address]struct{}, len(req.FromAddress))
	toAddresses = make(map[common.Address]struct{}, len(req.ToAddress))
	var blocksTo stream.U64

	for _, addr := range req.FromAddress {
		if addr != nil {
			it, err := tx.IndexRange(kv.TracesFromIdx, addr.Bytes(), int(from), int(to), order.Asc, kv.Unlim)
			if errors.Is(err, ethdb.ErrKeyNotFound) {
				continue
			}
			allBlocks = stream.Union[uint64](allBlocks, it, order.Asc, -1)
			fromAddresses[*addr] = struct{}{}
		}
	}

	for _, addr := range req.ToAddress {
		if addr != nil {
			it, err := tx.IndexRange(kv.TracesToIdx, addr.Bytes(), int(from), int(to), order.Asc, kv.Unlim)
			if errors.Is(err, ethdb.ErrKeyNotFound) {
				continue
			}
			blocksTo = stream.Union[uint64](blocksTo, it, order.Asc, -1)
			toAddresses[*addr] = struct{}{}
		}
	}

	switch req.Mode {
	case TraceFilterModeIntersection:
		allBlocks = stream.Intersect[uint64](allBlocks, blocksTo, -1)
	case TraceFilterModeUnion:
		fallthrough
	default:
		allBlocks = stream.Union[uint64](allBlocks, blocksTo, order.Asc, -1)
	}

	// Special case - if no addresses specified, take all traces
	if len(req.FromAddress) == 0 && len(req.ToAddress) == 0 {
		allBlocks = stream.Range[uint64](from, to)
		//} else {
		//allBlocks.RemoveRange(0, from)
		//allBlocks.RemoveRange(to, uint64(0x100000000))
	}

	return fromAddresses, toAddresses, allBlocks, nil
}

// Filter implements trace_filter
// NOTE: We do not store full traces - we just store index for each address
// Pull blocks which have txs with matching address
func (api *TraceAPIImpl) Filter(ctx context.Context, req TraceFilterRequest, gasBailOut *bool, traceConfig *config.TraceConfig, stream *jsoniter.Stream) error {
	if gasBailOut == nil {
		//nolint
		gasBailOut = new(bool) // false by default
	}
	dbtx, err1 := api.kv.BeginRo(ctx)
	if err1 != nil {
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
		headNumber, err := api._blockReader.HeaderNumber(ctx, dbtx, rawdb.ReadHeadHeaderHash(dbtx))
		if err != nil {
			return err
		}
		toBlock = *headNumber
	} else {
		toBlock = uint64(*req.ToBlock)
	}
	if fromBlock > toBlock {
		return errors.New("invalid parameters: fromBlock cannot be greater than toBlock")
	}

	return api.filterV3(ctx, dbtx.(kv.TemporalTx), fromBlock, toBlock, req, stream, *gasBailOut, traceConfig)
}

func (api *TraceAPIImpl) filterV3(ctx context.Context, dbtx kv.TemporalTx, fromBlock, toBlock uint64, req TraceFilterRequest, stream *jsoniter.Stream, gasBailOut bool, traceConfig *config.TraceConfig) error {
	var fromTxNum, toTxNum uint64
	var err error
	txNumsReader := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, api._blockReader))

	if fromBlock > 0 {
		fromTxNum, err = txNumsReader.Min(dbtx, fromBlock)
		if err != nil {
			return err
		}
	}
	toTxNum, err = txNumsReader.Max(dbtx, toBlock) // toBlock is an inclusive bound
	if err != nil {
		return err
	}
	toTxNum++ //+1 because internally Erigon using semantic [from, to), but some RPC have different semantic
	fromAddresses, toAddresses, allTxs, err := traceFilterBitmapsV3(dbtx, req, fromTxNum, toTxNum)
	if err != nil {
		return err
	}
	it := rawdbv3.TxNums2BlockNums(dbtx, txNumsReader, allTxs, order.Asc)
	defer it.Close()

	chainConfig, err := api.chainConfig(ctx, dbtx)
	if err != nil {
		return err
	}
	engine := api.engine()

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
	vmConfig := vm.Config{}
	nSeen := uint64(0)
	nExported := uint64(0)
	includeAll := len(fromAddresses) == 0 && len(toAddresses) == 0

	var lastBlockHash common.Hash
	var lastHeader *types.Header
	var lastSigner *types.Signer
	var lastRules *chain.Rules

	stateReader := state.NewHistoryReaderV3()
	stateReader.SetTx(dbtx)
	noop := state.NewNoopWriter()
	isPos := false
	for it.HasNext() {
		txNum, blockNum, txIndex, isFnalTxn, blockNumChanged, err := it.Next()
		if err != nil {
			if first {
				first = false
			} else {
				stream.WriteMore()
			}
			stream.WriteObjectStart()
			rpc.HandleError(err, stream)
			stream.WriteObjectEnd()
			continue
		}

		if blockNumChanged {
			if lastHeader, err = api._blockReader.HeaderByNumber(ctx, dbtx, blockNum); err != nil {
				if first {
					first = false
				} else {
					stream.WriteMore()
				}
				stream.WriteObjectStart()
				rpc.HandleError(err, stream)
				stream.WriteObjectEnd()
				continue
			}
			if lastHeader == nil {
				if first {
					first = false
				} else {
					stream.WriteMore()
				}
				stream.WriteObjectStart()
				rpc.HandleError(fmt.Errorf("header not found: %d", blockNum), stream)
				stream.WriteObjectEnd()
				continue
			}

			if !isPos && chainConfig.TerminalTotalDifficulty != nil {
				header := lastHeader
				isPos = header.Difficulty.Sign() == 0 || header.Difficulty.Cmp(chainConfig.TerminalTotalDifficulty) >= 0
			}

			lastBlockHash = lastHeader.Hash()
			lastSigner = types.MakeSigner(chainConfig, blockNum, lastHeader.Time)
			lastRules = chainConfig.Rules(blockNum, lastHeader.Time)
		}
		if isFnalTxn {
			// TODO(yperbasis) proper rewards for Gnosis

			// if we are in POS
			// we don't check for uncles or block rewards
			if isPos {
				continue
			}

			body, _, err := api._blockReader.Body(ctx, dbtx, lastBlockHash, blockNum)
			if err != nil {
				if first {
					first = false
				} else {
					stream.WriteMore()
				}
				stream.WriteObjectStart()
				rpc.HandleError(err, stream)
				stream.WriteObjectEnd()
				continue
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
					if first {
						first = false
					} else {
						stream.WriteMore()
					}
					stream.WriteObjectStart()
					rpc.HandleError(err, stream)
					stream.WriteObjectEnd()
					continue
				}
				if nSeen > after && nExported < count {
					if first {
						first = false
					} else {
						stream.WriteMore()
					}
					if _, err := stream.Write(b); err != nil {
						return err
					}
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
							if first {
								first = false
							} else {
								stream.WriteMore()
							}
							stream.WriteObjectStart()
							rpc.HandleError(err, stream)
							stream.WriteObjectEnd()
							continue
						}
						if nSeen > after && nExported < count {
							if first {
								first = false
							} else {
								stream.WriteMore()
							}
							if _, err := stream.Write(b); err != nil {
								return err
							}
							nExported++
						}
					}
				}
			}
			continue
		}
		if txIndex == -1 { //is system tx
			continue
		}
		txIndexU64 := uint64(txIndex)
		//fmt.Printf("txNum=%d, blockNum=%d, txIndex=%d\n", txNum, blockNum, txIndex)
		txn, err := api._txnReader.TxnByIdxInBlock(ctx, dbtx, blockNum, txIndex)
		if err != nil {
			if first {
				first = false
			} else {
				stream.WriteMore()
			}
			stream.WriteObjectStart()
			rpc.HandleError(err, stream)
			stream.WriteObjectEnd()
			continue
		}
		if txn == nil {
			continue //guess block doesn't have transactions
		}
		txHash := txn.Hash()
		msg, err := txn.AsMessage(*lastSigner, lastHeader.BaseFee, lastRules)
		if err != nil {
			if first {
				first = false
			} else {
				stream.WriteMore()
			}
			stream.WriteObjectStart()
			rpc.HandleError(err, stream)
			stream.WriteObjectEnd()
			continue
		}

		stateReader.SetTxNum(txNum)
		stateCache := shards.NewStateCache(32, 0 /* no limit */) // this cache living only during current RPC call, but required to store state writes
		cachedReader := state.NewCachedReader(stateReader, stateCache)
		//cachedReader := stateReader
		cachedWriter := state.NewCachedWriter(noop, stateCache)
		//cachedWriter := noop

		vmConfig.SkipAnalysis = core.SkipAnalysis(chainConfig, blockNum)
		traceResult := &TraceCallResult{Trace: []*ParityTrace{}}
		var ot OeTracer
		ot.config, err = parseOeTracerConfig(traceConfig)
		if err != nil {
			return err
		}
		ot.compat = api.compatibility
		ot.r = traceResult
		ot.idx = []string{fmt.Sprintf("%d-", txIndex)}
		ot.traceAddr = []int{}
		vmConfig.Debug = true
		vmConfig.Tracer = &ot
		ibs := state.New(cachedReader)

		blockCtx := transactions.NewEVMBlockContext(engine, lastHeader, true /* requireCanonical */, dbtx, api._blockReader, chainConfig)
		txCtx := core.NewEVMTxContext(msg)
		evm := vm.NewEVM(blockCtx, txCtx, ibs, chainConfig, vmConfig)

		gp := new(core.GasPool).AddGas(msg.Gas()).AddBlobGas(msg.BlobGas())
		ibs.SetTxContext(txIndex)
		var execResult *evmtypes.ExecutionResult
		execResult, err = core.ApplyMessage(evm, msg, gp, true /* refunds */, gasBailOut)
		if err != nil {
			if first {
				first = false
			} else {
				stream.WriteMore()
			}
			stream.WriteObjectStart()
			rpc.HandleError(err, stream)
			stream.WriteObjectEnd()
			continue
		}
		traceResult.Output = common.Copy(execResult.ReturnData)
		if err = ibs.FinalizeTx(evm.ChainRules(), noop); err != nil {
			if first {
				first = false
			} else {
				stream.WriteMore()
			}
			stream.WriteObjectStart()
			rpc.HandleError(err, stream)
			stream.WriteObjectEnd()
			continue
		}
		if err = ibs.CommitBlock(evm.ChainRules(), cachedWriter); err != nil {
			if first {
				first = false
			} else {
				stream.WriteMore()
			}
			stream.WriteObjectStart()
			rpc.HandleError(err, stream)
			stream.WriteObjectEnd()
			continue
		}
		isIntersectionMode := req.Mode == TraceFilterModeIntersection
		for _, pt := range traceResult.Trace {
			if includeAll || filterTrace(pt, fromAddresses, toAddresses, isIntersectionMode) {
				nSeen++
				pt.BlockHash = &lastBlockHash
				pt.BlockNumber = &blockNum
				pt.TransactionHash = &txHash
				pt.TransactionPosition = &txIndexU64
				b, err := json.Marshal(pt)
				if err != nil {
					if first {
						first = false
					} else {
						stream.WriteMore()
					}
					stream.WriteObjectStart()
					rpc.HandleError(err, stream)
					stream.WriteObjectEnd()
					continue
				}
				if nSeen > after && nExported < count {
					if first {
						first = false
					} else {
						stream.WriteMore()
					}
					if _, err := stream.Write(b); err != nil {
						return err
					}
					nExported++
				}
			}
		}
	}
	stream.WriteArrayEnd()
	return stream.Flush()
}

func filterTrace(pt *ParityTrace, fromAddresses map[common.Address]struct{}, toAddresses map[common.Address]struct{}, isIntersectionMode bool) bool {
	f, t := false, false
	switch action := pt.Action.(type) {
	case *CallTraceAction:
		_, f = fromAddresses[action.From]
		_, t = toAddresses[action.To]
	case *CreateTraceAction:
		_, f = fromAddresses[action.From]

		if res, ok := pt.Result.(*CreateTraceResult); ok {
			if res.Address != nil {
				_, t = toAddresses[*res.Address]
			}
		}
	case *SuicideTraceAction:
		_, f = fromAddresses[action.Address]
		_, t = toAddresses[action.RefundAddress]
	}

	if isIntersectionMode {
		return f && t
	} else {
		return f || t
	}
}

func (api *TraceAPIImpl) callManyTransactions(
	ctx context.Context,
	dbtx kv.Tx,
	block *types.Block,
	traceTypes []string,
	txIndex int,
	gasBailOut bool,
	signer *types.Signer,
	cfg *chain.Config,
	traceConfig *config.TraceConfig,
) ([]*TraceCallResult, consensus.SystemCall, error) {
	blockNumber := block.NumberU64()
	pNo := blockNumber
	if pNo > 0 {
		pNo -= 1
	}

	parentNo := rpc.BlockNumber(pNo)
	rules := cfg.Rules(blockNumber, block.Time())
	header := block.Header()
	txs := block.Transactions()
	var borStateSyncTxn types.Transaction
	var borStateSyncTxnHash common.Hash
	if cfg.Bor != nil {
		// check if this block has state sync txn
		blockHash := block.Hash()
		borStateSyncTxnHash = bortypes.ComputeBorTxHash(blockNumber, blockHash)

		var ok bool
		var err error

		if api.bridgeReader != nil {
			_, ok, err = api.bridgeReader.EventTxnLookup(ctx, borStateSyncTxnHash)

		} else {
			_, ok, err = api._blockReader.EventLookup(ctx, dbtx, borStateSyncTxnHash)
		}
		if err != nil {
			return nil, nil, err
		}
		if ok {
			borStateSyncTxn = bortypes.NewBorTransaction()
			txs = append(txs, borStateSyncTxn)
		}
	}

	callParams := make([]TraceCallParam, 0, len(txs))

	parentHash := block.ParentHash()
	parentNrOrHash := rpc.BlockNumberOrHash{
		BlockNumber:      &parentNo,
		BlockHash:        &parentHash,
		RequireCanonical: true,
	}

	stateReader, err := rpchelper.CreateStateReader(ctx, dbtx, api._blockReader, parentNrOrHash, 0, api.filters, api.stateCache, cfg.ChainName)
	if err != nil {
		return nil, nil, err
	}
	stateCache := shards.NewStateCache(
		32, 0 /* no limit */) // this cache living only during current RPC call, but required to store state writes
	cachedReader := state.NewCachedReader(stateReader, stateCache)
	noop := state.NewNoopWriter()
	cachedWriter := state.NewCachedWriter(noop, stateCache)
	ibs := state.New(cachedReader)

	engine := api.engine()
	consensusHeaderReader := consensuschain.NewReader(cfg, dbtx, nil, nil)
	logger := log.New("trace_filtering")
	err = core.InitializeBlockExecution(engine.(consensus.Engine), consensusHeaderReader, block.HeaderNoCopy(), cfg, ibs, logger, nil)
	if err != nil {
		return nil, nil, err
	}
	if err = ibs.CommitBlock(rules, cachedWriter); err != nil {
		return nil, nil, err
	}

	msgs := make([]types.Message, len(txs))
	for i, txn := range txs {
		isBorStateSyncTxn := txn == borStateSyncTxn
		var txnHash common.Hash
		var msg types.Message
		var err error
		if isBorStateSyncTxn {
			txnHash = borStateSyncTxnHash
			// we use an empty message for bor state sync txn since it gets handled differently
		} else {
			txnHash = txn.Hash()
			msg, err = txn.AsMessage(*signer, header.BaseFee, rules)
			if err != nil {
				return nil, nil, fmt.Errorf("convert txn into msg: %w", err)
			}

			// gnosis might have a fee free account here
			if msg.FeeCap().IsZero() && engine != nil {
				syscall := func(contract common.Address, data []byte) ([]byte, error) {
					return core.SysCallContract(contract, data, cfg, ibs, header, engine, true /* constCall */)
				}
				msg.SetIsFree(engine.IsServiceTransaction(msg.From(), syscall))
			}
		}

		callParams = append(callParams, TraceCallParam{
			txHash:            &txnHash,
			traceTypes:        traceTypes,
			isBorStateSyncTxn: isBorStateSyncTxn,
		})

		msgs[i] = msg
	}

	traces, cmErr := api.doCallMany(ctx, dbtx, stateReader, stateCache, cachedWriter, ibs, msgs, callParams,
		&parentNrOrHash, header, gasBailOut /* gasBailout */, txIndex, traceConfig)

	if cmErr != nil {
		return nil, nil, cmErr
	}

	syscall := func(contract common.Address, data []byte) ([]byte, error) {
		return core.SysCallContract(contract, data, cfg, ibs, header, engine, false /* constCall */)
	}

	return traces, syscall, nil
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
	// TraceFilterModeUnion is default mode for TraceFilter.
	// Unions results referred to addresses from FromAddress or ToAddress
	TraceFilterModeUnion = "union"
	// TraceFilterModeIntersection retrieves results referred to addresses provided both in FromAddress and ToAddress
	TraceFilterModeIntersection = "intersection"
)
