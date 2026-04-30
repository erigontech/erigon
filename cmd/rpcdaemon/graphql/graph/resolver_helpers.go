package graph

import (
	"context"
	"strings"

	"github.com/erigontech/erigon/cmd/rpcdaemon/graphql/graph/model"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
)

func (r *Resolver) resolveAccountAtBlock(ctx context.Context, address string, defaultBlock uint64, override *uint64) (*model.Account, error) {
	blockNum := rpc.BlockNumber(defaultBlock)
	if override != nil {
		blockNum = rpc.BlockNumber(*override)
	}
	addr := common.HexToAddress(address)
	balance, nonce, code, err := r.GraphQLAPI.GetAccountInfo(ctx, addr, blockNum)
	if err != nil {
		return nil, err
	}
	return &model.Account{
		Address:          strings.ToLower(address),
		Balance:          balance,
		TransactionCount: nonce,
		Code:             code,
		BlockNum:         uint64(blockNum),
	}, nil
}

func (r *queryResolver) buildBlock(res map[string]any) (*model.Block, error) {
	block := &model.Block{}
	absBlk := res["block"]
	if absBlk == nil {
		return block, nil
	}

	blk := absBlk.(map[string]any)

	block.Difficulty = *convertDataToStringP(blk, "difficulty")
	block.TotalDifficulty = *convertDataToStringP(blk, "totalDifficulty")
	block.ExtraData = *convertDataToStringP(blk, "extraData")
	block.GasLimit = uint64(*convertDataToUint64P(blk, "gasLimit"))
	block.GasUsed = *convertDataToUint64P(blk, "gasUsed")
	block.Hash = *convertDataToStringP(blk, "hash")
	block.Miner = &model.Account{}
	if address := convertDataToStringP(blk, "miner"); address != nil {
		block.Miner.Address = strings.ToLower(*address)
	}
	if mixHash := convertDataToStringP(blk, "mixHash"); mixHash != nil {
		block.MixHash = *mixHash
	}
	if blockNonce := convertDataToStringP(blk, "nonce"); blockNonce != nil {
		block.Nonce = *blockNonce
	}
	block.Number = *convertDataToUint64P(blk, "number")
	block.Miner.BlockNum = block.Number
	block.Parent = &model.Block{}
	block.Parent.Hash = *convertDataToStringP(blk, "parentHash")
	block.ReceiptsRoot = *convertDataToStringP(blk, "receiptsRoot")
	block.StateRoot = *convertDataToStringP(blk, "stateRoot")
	block.Timestamp = *convertDataToStringP(blk, "timestamp")
	block.TransactionCount = convertDataToIntP(blk, "transactionCount")
	block.TransactionsRoot = *convertDataToStringP(blk, "transactionsRoot")
	block.BaseFeePerGas = convertDataToStringP(blk, "baseFeePerGas")
	block.LogsBloom = "0x" + *convertDataToStringP(blk, "logsBloom")
	block.OmmerHash = *convertDataToStringP(blk, "sha3Uncles")

	uncles := blk["uncles"].([]common.Hash)
	block.Ommers = make([]*model.Block, 0, len(uncles))
	for _, ommerHash := range uncles {
		block.Ommers = append(block.Ommers, &model.Block{Hash: ommerHash.String()})
	}
	ommerCount := len(block.Ommers)
	block.OmmerCount = &ommerCount

	rcp := res["receipts"].([]map[string]any)
	block.Transactions = make([]*model.Transaction, 0, len(rcp))
	for _, transReceipt := range rcp {
		trans := r.buildTransaction(block, transReceipt)
		block.Transactions = append(block.Transactions, trans)
	}

	withdrawals := res["withdrawals"].([]map[string]any)
	block.Withdrawals = make([]*model.Withdrawal, 0, len(withdrawals))
	for _, withdrawal := range withdrawals {
		w := &model.Withdrawal{}
		w.Index = *convertDataToIntP(withdrawal, "index")
		w.Validator = *convertDataToIntP(withdrawal, "validator")
		w.Address = *convertDataToStringP(withdrawal, "address")
		w.Amount = *convertDataToStringP(withdrawal, "amount")
		block.Withdrawals = append(block.Withdrawals, w)
	}

	return block, nil
}

func (r *queryResolver) buildTransaction(block *model.Block, transReceipt map[string]any) *model.Transaction {
	trans := &model.Transaction{}
	trans.Block = block
	trans.CumulativeGasUsed = convertDataToUint64P(transReceipt, "cumulativeGasUsed")
	trans.Gas = *convertDataToUint64P(transReceipt, "gas")
	trans.InputData = *convertDataToStringP(transReceipt, "data")
	trans.EffectiveGasPrice = convertDataToStringP(transReceipt, "effectiveGasPrice")
	if trans.EffectiveGasPrice != nil {
		trans.GasPrice = *trans.EffectiveGasPrice
	}
	trans.GasUsed = convertDataToUint64P(transReceipt, "gasUsed")
	trans.Hash = *convertDataToStringP(transReceipt, "transactionHash")
	trans.Index = convertDataToIntP(transReceipt, "transactionIndex")
	trans.MaxFeePerGas = convertDataToStringP(transReceipt, "maxFeePerGas")
	trans.MaxPriorityFeePerGas = convertDataToStringP(transReceipt, "maxPriorityFeePerGas")
	if transNonce := convertDataToStringP(transReceipt, "nonce"); transNonce != nil {
		trans.Nonce = *transNonce
	}
	trans.Status = convertDataToUint64P(transReceipt, "status")
	trans.Type = convertDataToUint64P(transReceipt, "type")
	trans.Value = *convertDataToStringP(transReceipt, "value")

	logs := transReceipt["logs"].(types.Logs)
	trans.Logs = make([]*model.Log, 0, len(logs))
	for _, rlog := range logs {
		tlog := model.Log{
			Index: int(rlog.Index),
			Data:  hexutil.Encode(rlog.Data),
		}
		tlog.Account = model.NewAccountAtBlock(block.Number)
		tlog.Account.Address = strings.ToLower(rlog.Address.String())
		tlog.Topics = make([]string, 0, len(rlog.Topics))
		for _, rtopic := range rlog.Topics {
			tlog.Topics = append(tlog.Topics, rtopic.String())
		}
		trans.Logs = append(trans.Logs, &tlog)
	}

	trans.From = model.NewAccountAtBlock(block.Number)
	trans.From.Address = strings.ToLower(*convertDataToStringP(transReceipt, "from"))

	if toAddress := convertDataToStringP(transReceipt, "to"); toAddress != nil {
		trans.To = model.NewAccountAtBlock(block.Number)
		trans.To.Address = strings.ToLower(*toAddress)
	}

	if contractAddr := convertDataToStringP(transReceipt, "contractAddress"); contractAddr != nil {
		trans.CreatedContract = model.NewAccountAtBlock(block.Number)
		trans.CreatedContract.Address = strings.ToLower(*contractAddr)
	}

	if al, ok := transReceipt["accessList"].(types.AccessList); ok {
		trans.AccessList = make([]*model.AccessTuple, len(al))
		for i, entry := range al {
			keys := make([]string, len(entry.StorageKeys))
			for j, k := range entry.StorageKeys {
				keys[j] = k.Hex()
			}
			trans.AccessList[i] = &model.AccessTuple{
				Address:     strings.ToLower(entry.Address.String()),
				StorageKeys: keys,
			}
		}
	}

	return trans
}
