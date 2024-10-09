package commands

import (
	"context"
	"fmt"
	"math/big"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/gateway-fm/cdk-erigon-lib/kv/rawdbv3"
	jsoniter "github.com/json-iterator/go"

	"github.com/ledgerwatch/erigon/chain"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/turbo/transactions"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
)

func (api *TraceAPIImpl) filterV3(ctx context.Context, dbtx kv.TemporalTx, fromBlock, toBlock uint64, req TraceFilterRequest, stream *jsoniter.Stream) error {
	var fromTxNum, toTxNum uint64
	var err error
	if fromBlock > 0 {
		fromTxNum, err = rawdbv3.TxNums.Min(dbtx, fromBlock)
		if err != nil {
			return err
		}
	}
	toTxNum, err = rawdbv3.TxNums.Max(dbtx, toBlock) // toBlock is an inclusive bound
	if err != nil {
		return err
	}
	toTxNum++ //+1 because internally Erigon using semantic [from, to), but some RPC have different semantic
	fromAddresses, toAddresses, allTxs, err := traceFilterBitmapsV3(dbtx, req, fromTxNum, toTxNum)
	if err != nil {
		return err
	}

	chainConfig, err := api.chainConfig(dbtx)
	if err != nil {
		return err
	}
	engine := api.engine()

	json := jsoniter.ConfigCompatibleWithStandardLibrary
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
	it := MapTxNum2BlockNum(dbtx, allTxs)

	var lastBlockHash common.Hash
	var lastHeader *types.Header
	var lastSigner *types.Signer
	var lastRules *chain.Rules

	stateReader := state.NewHistoryReaderV3()
	stateReader.SetTx(dbtx)
	noop := state.NewNoopWriter()
	isPos := false
	hermezReader := hermez_db.NewHermezDbReader(dbtx)

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
				isPos = header.Difficulty.Cmp(common.Big0) == 0 || header.Difficulty.Cmp(chainConfig.TerminalTotalDifficulty) >= 0
			}

			lastBlockHash = lastHeader.Hash()
			lastSigner = types.MakeSigner(chainConfig, blockNum)
			lastRules = chainConfig.Rules(blockNum, lastHeader.Time)
		}
		if isFnalTxn {
			// if we are in POS
			// we dont check for uncles or block rewards
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
				rewardAction := &RewardTraceAction{}
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
							stream.Write(b)
							nExported++
						}
					}
				}
			}
			continue
		}
		if txIndex == -1 { // is system tx
			continue
		}
		txIndexU64 := uint64(txIndex)
		// fmt.Printf("txNum=%d, blockNum=%d, txIndex=%d\n", txNum, blockNum, txIndex)
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
			continue // guess block doesn't have transactions
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

		effectiveGasPricePercentage, err := hermezReader.GetEffectiveGasPricePercentage(txn.Hash())
		if err != nil {
			return err
		}
		msg.SetEffectiveGasPricePercentage(effectiveGasPricePercentage)

		stateReader.SetTxNum(txNum)
		stateCache := shards.NewStateCache(32, 0 /* no limit */) // this cache living only during current RPC call, but required to store state writes
		cachedReader := state.NewCachedReader(stateReader, stateCache)
		cachedWriter := state.NewCachedWriter(noop, stateCache)
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

		blockCtx := transactions.NewEVMBlockContext(engine, lastHeader, true /* requireCanonical */, dbtx, api._blockReader)
		txCtx := core.NewEVMTxContext(msg)
		evm := vm.NewEVM(blockCtx, txCtx, ibs, chainConfig, vmConfig)

		gp := new(core.GasPool).AddGas(msg.Gas())
		ibs.Prepare(txHash, lastBlockHash, txIndex)
		var execResult *core.ExecutionResult
		execResult, err = core.ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */)
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
		for _, pt := range traceResult.Trace {
			if includeAll || filter_trace(pt, fromAddresses, toAddresses) {
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
					stream.Write(b)
					nExported++
				}
			}
		}
	}
	stream.WriteArrayEnd()
	return stream.Flush()
}

func (api *TraceAPIImpl) callManyTransactions(
	ctx context.Context,
	dbtx kv.Tx,
	block *types.Block,
	traceTypes []string,
	txIndex int,
	signer *types.Signer,
	cfg *chain.Config,
) ([]*TraceCallResult, error) {
	blockNumber := block.NumberU64()
	pNo := blockNumber
	if pNo > 0 {
		pNo -= 1
	}
	parentNo := rpc.BlockNumber(pNo)
	rules := cfg.Rules(blockNumber, block.Time())
	header := block.Header()
	var excessDataGas *big.Int
	parentBlock, err := api.blockByRPCNumber(parentNo, dbtx)
	if err != nil {
		return nil, err
	} else if parentBlock != nil {
		excessDataGas = parentBlock.ExcessDataGas()
	}
	txs := block.Transactions()
	callParams := make([]TraceCallParam, 0, len(txs))
	reader, err := rpchelper.CreateHistoryStateReader(dbtx, blockNumber, txIndex, api.historyV3(dbtx), cfg.ChainName)
	if err != nil {
		return nil, err
	}
	stateDb := state.New(reader)
	if err != nil {
		return nil, err
	}
	engine := api.engine()
	consensusHeaderReader := stagedsync.NewChainReaderImpl(cfg, dbtx, nil)
	err = core.InitializeBlockExecution(engine.(consensus.Engine), consensusHeaderReader, block.HeaderNoCopy(), block.Transactions(), block.Uncles(), cfg, stateDb, excessDataGas)
	if err != nil {
		return nil, err
	}
	hermezReader := hermez_db.NewHermezDbReader(dbtx)

	msgs := make([]types.Message, len(txs))
	for i, tx := range txs {
		hash := tx.Hash()
		callParams = append(callParams, TraceCallParam{
			txHash:     &hash,
			traceTypes: traceTypes,
		})
		var err error

		msg, err := tx.AsMessage(*signer, header.BaseFee, rules)
		if err != nil {
			return nil, fmt.Errorf("convert tx into msg: %w", err)
		}

		// now read back the effective gas price and set it for execution
		effectiveGasPricePercentage, err := hermezReader.GetEffectiveGasPricePercentage(tx.Hash())
		if err != nil {
			return nil, err
		}
		msg.SetEffectiveGasPricePercentage(effectiveGasPricePercentage)

		// gnosis might have a fee free account here
		if msg.FeeCap().IsZero() && engine != nil {
			syscall := func(contract common.Address, data []byte) ([]byte, error) {
				return core.SysCallContract(contract, data, *cfg, stateDb, header, engine, true /* constCall */, excessDataGas)
			}
			msg.SetIsFree(engine.IsServiceTransaction(msg.From(), syscall))
		}

		msgs[i] = msg
	}

	parentHash := block.ParentHash()

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
