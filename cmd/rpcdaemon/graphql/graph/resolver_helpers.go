package graph

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/erigontech/erigon/cmd/rpcdaemon/graphql/graph/model"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
)

func (r *Resolver) resolveAccountAtBlock(ctx context.Context, address string, defaultBlock uint64, override *uint64) (*model.Account, error) {
	blockNum := defaultBlock
	if override != nil {
		blockNum = *override
	}
	if !common.IsHexAddress(address) {
		return nil, fmt.Errorf("invalid address %q", address)
	}
	if blockNum > uint64(math.MaxInt64) {
		return nil, fmt.Errorf("block number %d exceeds max supported value", blockNum)
	}
	addr := common.HexToAddress(address)
	balance, nonce, code, err := r.GraphQLAPI.GetAccountInfo(ctx, addr, rpc.BlockNumber(blockNum))
	if err != nil {
		return nil, err
	}
	return &model.Account{
		Address:          strings.ToLower(address),
		Balance:          balance,
		TransactionCount: nonce,
		Code:             code,
		BlockNum:         blockNum,
	}, nil
}

func (r *Resolver) resolveAccountAtRequestedBlock(ctx context.Context, account *model.Account, block *uint64) (*model.Account, error) {
	if account == nil || account.Address == "" {
		return nil, nil
	}
	return r.resolveAccountAtBlock(ctx, account.Address, account.BlockNum, block)
}

func normalizeHex(value string) string {
	return strings.ToLower(value)
}

func flattenBlockLogs(transactions []*model.Transaction) []*model.Log {
	var logs []*model.Log
	for _, tx := range transactions {
		if tx == nil {
			continue
		}
		logs = append(logs, tx.Logs...)
	}
	return logs
}

func blockLogMatchesFilter(log *model.Log, filter model.BlockFilterCriteria) bool {
	if log == nil {
		return false
	}

	if len(filter.Addresses) > 0 {
		if log.Account == nil || log.Account.Address == "" {
			return false
		}
		match := false
		for _, address := range filter.Addresses {
			if normalizeHex(address) == normalizeHex(log.Account.Address) {
				match = true
				break
			}
		}
		if !match {
			return false
		}
	}

	for i, alternatives := range filter.Topics {
		if len(alternatives) == 0 {
			continue
		}
		if i >= len(log.Topics) {
			return false
		}
		match := false
		for _, topic := range alternatives {
			if normalizeHex(topic) == normalizeHex(log.Topics[i]) {
				match = true
				break
			}
		}
		if !match {
			return false
		}
	}
	return true
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
	block.TransactionCount = convertDataToUint64P(blk, "transactionCount")
	block.TransactionsRoot = *convertDataToStringP(blk, "transactionsRoot")
	block.BaseFeePerGas = convertDataToStringP(blk, "baseFeePerGas")
	block.LogsBloom = "0x" + *convertDataToStringP(blk, "logsBloom")
	block.OmmerHash = *convertDataToStringP(blk, "sha3Uncles")
	block.WithdrawalsRoot = convertDataToStringP(blk, "withdrawalsRoot")
	block.BlobGasUsed = convertDataToUint64P(blk, "blobGasUsed")
	block.ExcessBlobGas = convertDataToUint64P(blk, "excessBlobGas")

	uncles := blk["uncles"].([]common.Hash)
	block.Ommers = make([]*model.Block, 0, len(uncles))
	for _, ommerHash := range uncles {
		block.Ommers = append(block.Ommers, &model.Block{Hash: ommerHash.String()})
	}
	ommerCount := uint64(len(block.Ommers))
	block.OmmerCount = &ommerCount

	rcp := res["receipts"].([]map[string]any)
	block.Transactions = make([]*model.Transaction, 0, len(rcp))
	for _, transReceipt := range rcp {
		trans := r.buildTransaction(block, transReceipt)
		block.Transactions = append(block.Transactions, trans)
	}

	if block.WithdrawalsRoot != nil {
		withdrawals, _ := res["withdrawals"].([]map[string]any)
		block.Withdrawals = make([]*model.Withdrawal, 0, len(withdrawals))
		for _, withdrawal := range withdrawals {
			w := &model.Withdrawal{}
			w.Index = *convertDataToUint64P(withdrawal, "index")
			w.Validator = *convertDataToUint64P(withdrawal, "validator")
			w.Address = strings.ToLower(*convertDataToStringP(withdrawal, "address"))
			w.Amount = *convertDataToStringP(withdrawal, "amount")
			block.Withdrawals = append(block.Withdrawals, w)
		}
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
	trans.Index = convertDataToUint64P(transReceipt, "transactionIndex")
	trans.MaxFeePerGas = convertDataToStringP(transReceipt, "maxFeePerGas")
	trans.MaxPriorityFeePerGas = convertDataToStringP(transReceipt, "maxPriorityFeePerGas")
	trans.MaxFeePerBlobGas = convertDataToStringP(transReceipt, "maxFeePerBlobGas")
	trans.BlobGasUsed = convertDataToUint64P(transReceipt, "blobGasUsed")
	trans.BlobGasPrice = convertDataToStringP(transReceipt, "blobGasPrice")
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
			Index: uint64(rlog.Index),
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
