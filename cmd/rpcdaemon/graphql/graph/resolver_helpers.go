package graph

import (
	"context"
	"fmt"
	"strings"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/cmd/rpcdaemon/graphql/graph/model"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	ethapi "github.com/erigontech/erigon/rpc/ethapi"
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

func ptr[T any](v T) *T { return &v }

func (r *queryResolver) buildBlock(res map[string]any) (*model.Block, error) {
	block := &model.Block{}
	absBlk := res["block"]
	if absBlk == nil {
		return block, nil
	}

	blk, ok := absBlk.(*ethapi.RPCBlock)
	if !ok {
		return nil, fmt.Errorf("unexpected block type %T", absBlk)
	}

	hexU256 := func(v *hexutil.U256) string {
		if v == nil {
			return ""
		}
		return v.String()
	}

	block.Difficulty = hexU256(blk.Difficulty)
	block.TotalDifficulty = hexU256(blk.TotalDifficulty)
	block.ExtraData = blk.ExtraData.String()
	block.GasLimit = uint64(blk.GasLimit)
	block.GasUsed = uint64(blk.GasUsed)
	block.Miner = &model.Account{}
	// A pending block has no hash, miner or nonce yet; MarkPending nils them.
	if blk.Hash != nil {
		block.Hash = blk.Hash.Hex()
	}
	if blk.Miner != nil {
		block.Miner.Address = strings.ToLower(blk.Miner.Hex())
	}
	if blk.Nonce != nil {
		block.Nonce = hexutil.Encode(blk.Nonce[:])
	}
	block.MixHash = blk.MixHash.Hex()
	block.Number = (*uint256.Int)(blk.Number).Uint64()
	block.Miner.BlockNum = block.Number
	block.Parent = &model.Block{Hash: blk.ParentHash.Hex()}
	block.ReceiptsRoot = blk.ReceiptsRoot.Hex()
	block.StateRoot = blk.StateRoot.Hex()
	block.Timestamp = hexutil.Uint64(blk.Timestamp).String()
	block.TransactionsRoot = blk.TransactionsRoot.Hex()
	block.OmmerHash = blk.Sha3Uncles.Hex()
	if blk.LogsBloom != nil {
		block.LogsBloom = hexutil.Encode(blk.LogsBloom[:])
	}
	if blk.BaseFeePerGas != nil {
		block.BaseFeePerGas = ptr(blk.BaseFeePerGas.String())
	}
	if blk.WithdrawalsRoot != nil {
		block.WithdrawalsRoot = ptr(blk.WithdrawalsRoot.Hex())
	}
	if blk.BlobGasUsed != nil {
		block.BlobGasUsed = ptr(uint64(*blk.BlobGasUsed))
	}
	if blk.ExcessBlobGas != nil {
		block.ExcessBlobGas = ptr(uint64(*blk.ExcessBlobGas))
	}
	if n, ok := blk.TransactionCount.(hexutil.Uint64); ok {
		block.TransactionCount = ptr(uint64(n))
	}

	uncles := blk.Uncles
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

func addressesFromModel(addrs []string) ([]common.Address, error) {
	result := make([]common.Address, 0, len(addrs))
	for _, a := range addrs {
		if !common.IsHexAddress(a) {
			return nil, fmt.Errorf("invalid address: %s", a)
		}
		result = append(result, common.HexToAddress(a))
	}
	return result, nil
}

func topicsFromModel(topicSets [][]string) ([][]common.Hash, error) {
	result := make([][]common.Hash, len(topicSets))
	for i, set := range topicSets {
		result[i] = make([]common.Hash, 0, len(set))
		for _, t := range set {
			b, err := hexutil.Decode(t)
			if err != nil {
				return nil, fmt.Errorf("invalid topic %s: %w", t, err)
			}
			result[i] = append(result[i], common.BytesToHash(b))
		}
	}
	return result, nil
}

func rpcLogsToModel(logs types.RPCLogs) []*model.Log {
	result := make([]*model.Log, 0, len(logs))
	for _, l := range logs {
		ml := &model.Log{
			Index: uint64(l.Index),
			Data:  hexutil.Encode(l.Data),
		}
		ml.Account = &model.Account{
			Address:  strings.ToLower(l.Address.Hex()),
			BlockNum: uint64(l.BlockNumber),
		}
		ml.Topics = make([]string, len(l.Topics))
		for i, t := range l.Topics {
			ml.Topics[i] = t.Hex()
		}
		ml.Transaction = &model.Transaction{
			Hash: l.TxHash.Hex(),
			Block: &model.Block{
				Number: uint64(l.BlockNumber),
				Hash:   l.BlockHash.Hex(),
			},
		}
		result = append(result, ml)
	}
	return result
}

func decodeOptionalAddress(s *string, fieldName string) (*common.Address, error) {
	if s == nil {
		return nil, nil
	}
	if !common.IsHexAddress(*s) {
		return nil, fmt.Errorf("invalid %s address: %s", fieldName, *s)
	}
	addr := common.HexToAddress(*s)
	return &addr, nil
}

func decodeOptionalU256(s *string, fieldName string) (*hexutil.U256, error) {
	if s == nil {
		return nil, nil
	}
	u, err := hexutil.DecodeU256(*s)
	if err != nil {
		return nil, fmt.Errorf("invalid %s: %w", fieldName, err)
	}
	return (*hexutil.U256)(&u), nil
}

func callDataToArgs(data model.CallData) (ethapi.CallArgs, error) {
	var (
		args ethapi.CallArgs
		err  error
	)
	if args.From, err = decodeOptionalAddress(data.From, "from"); err != nil {
		return args, err
	}
	if args.To, err = decodeOptionalAddress(data.To, "to"); err != nil {
		return args, err
	}
	if data.Gas != nil {
		gas := hexutil.Uint64(*data.Gas)
		args.Gas = &gas
	}
	if args.GasPrice, err = decodeOptionalU256(data.GasPrice, "gasPrice"); err != nil {
		return args, err
	}
	if args.MaxFeePerGas, err = decodeOptionalU256(data.MaxFeePerGas, "maxFeePerGas"); err != nil {
		return args, err
	}
	if args.MaxPriorityFeePerGas, err = decodeOptionalU256(data.MaxPriorityFeePerGas, "maxPriorityFeePerGas"); err != nil {
		return args, err
	}
	if args.Value, err = decodeOptionalU256(data.Value, "value"); err != nil {
		return args, err
	}
	if data.Data != nil {
		b, err := hexutil.Decode(*data.Data)
		if err != nil {
			return args, fmt.Errorf("invalid data: %w", err)
		}
		input := hexutil.Bytes(b)
		args.Input = &input
	}
	return args, nil
}
