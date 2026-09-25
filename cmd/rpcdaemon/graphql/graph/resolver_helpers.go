package graph

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/99designs/gqlgen/graphql"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/cmd/rpcdaemon/graphql/graph/model"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	ethapi "github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/jsonrpc"
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

func blockTxsRequested(ctx context.Context) bool {
	return graphql.AnyFieldRequested(ctx, "transactions", "transactionAt")
}

func (r *queryResolver) block(ctx context.Context, number *string, hash *string, withTxs bool) (*model.Block, error) {
	if number != nil && hash != nil {
		return nil, &rpc.InvalidParamsError{Message: "Invalid params"}
	}

	if hash != nil {
		blockHash := common.HexToHash(*hash)
		res, err := r.GraphQLAPI.GetBlockDetailsByHashWithTxs(ctx, blockHash, withTxs)
		if err != nil {
			return nil, err
		}
		if res == nil {
			return nil, nil
		}
		return r.buildBlock(res, withTxs)
	}

	var blockNumber rpc.BlockNumber

	if number != nil {
		bNum, err := strconv.ParseUint(*number, 10, 64)
		if err == nil {
			blockNumber = rpc.BlockNumber(bNum)
		} else {
			bNum, err := hexutil.DecodeUint64(*number)
			if err == nil {
				blockNumber = rpc.BlockNumber(bNum)
			} else {
				return nil, fmt.Errorf("invalid block number: %s", *number)
			}
		}
	} else {
		blockNumber = rpc.LatestBlockNumber
	}

	res, err := r.GraphQLAPI.GetBlockDetailsWithTxs(ctx, blockNumber, withTxs)
	if err != nil {
		return nil, err
	}
	if res == nil {
		return nil, nil
	}
	return r.buildBlock(res, withTxs)
}

func (r *queryResolver) buildBlock(res map[string]any, withTxs bool) (*model.Block, error) {
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
		block.Miner.Address = hexutil.Encode(blk.Miner[:])
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
	if blk.TransactionCount != nil {
		block.TransactionCount = ptr(*blk.TransactionCount)
	}

	uncles := blk.Uncles
	block.Ommers = make([]*model.Block, 0, len(uncles))
	for _, ommerHash := range uncles {
		block.Ommers = append(block.Ommers, &model.Block{Hash: ommerHash.String()})
	}
	ommerCount := uint64(len(block.Ommers))
	block.OmmerCount = &ommerCount

	if withTxs {
		rcp, ok := res["receipts"].([]*jsonrpc.GraphQLReceipt)
		if !ok {
			return nil, fmt.Errorf("unexpected receipts type %T", res["receipts"])
		}
		block.Transactions = make([]*model.Transaction, 0, len(rcp))
		for _, transReceipt := range rcp {
			block.Transactions = append(block.Transactions, r.buildTransaction(block, transReceipt))
		}
	}

	if block.WithdrawalsRoot != nil {
		withdrawals, ok := res["withdrawals"].([]jsonrpc.GraphQLWithdrawal)
		if !ok {
			return nil, fmt.Errorf("unexpected withdrawals type %T", res["withdrawals"])
		}
		block.Withdrawals = make([]*model.Withdrawal, 0, len(withdrawals))
		for _, withdrawal := range withdrawals {
			block.Withdrawals = append(block.Withdrawals, &model.Withdrawal{
				Index:     uint64(withdrawal.Index),
				Validator: uint64(withdrawal.Validator),
				Address:   hexutil.Encode(withdrawal.Address[:]),
				Amount:    withdrawal.Amount.String(),
			})
		}
	}

	return block, nil
}

func (r *queryResolver) buildTransaction(block *model.Block, receipt *jsonrpc.GraphQLReceipt) *model.Transaction {
	trans := &model.Transaction{
		Block:             block,
		CumulativeGasUsed: ptr(uint64(receipt.CumulativeGasUsed)),
		Gas:               receipt.Gas,
		InputData:         hexutil.Encode(receipt.Data),
		GasUsed:           ptr(uint64(receipt.GasUsed)),
		Hash:              receipt.TransactionHash.String(),
		Index:             ptr(uint64(receipt.TransactionIndex)),
		Nonce:             hexutil.EncodeUint64(receipt.Nonce),
		Type:              ptr(uint64(receipt.Type)),
		Value:             receipt.Value.Hex(),
	}
	if receipt.EffectiveGasPrice != nil {
		trans.EffectiveGasPrice = ptr(receipt.EffectiveGasPrice.String())
		trans.GasPrice = *trans.EffectiveGasPrice
	}
	if receipt.MaxFeePerGas != nil {
		trans.MaxFeePerGas = ptr(receipt.MaxFeePerGas.Hex())
	}
	if receipt.MaxPriorityFeePerGas != nil {
		trans.MaxPriorityFeePerGas = ptr(receipt.MaxPriorityFeePerGas.Hex())
	}
	if receipt.MaxFeePerBlobGas != nil {
		trans.MaxFeePerBlobGas = ptr(receipt.MaxFeePerBlobGas.String())
	}
	if receipt.BlobGasUsed != nil {
		trans.BlobGasUsed = ptr(uint64(*receipt.BlobGasUsed))
	}
	if receipt.BlobGasPrice != nil {
		trans.BlobGasPrice = ptr(receipt.BlobGasPrice.String())
	}
	if receipt.Status != nil {
		trans.Status = ptr(uint64(*receipt.Status))
	}

	trans.Logs = make([]*model.Log, 0, len(receipt.Logs))
	for _, rlog := range receipt.Logs {
		tlog := model.Log{
			Index: uint64(rlog.Index),
			Data:  hexutil.Encode(rlog.Data),
		}
		tlog.Account = model.NewAccountAtBlock(block.Number)
		tlog.Account.Address = hexutil.Encode(rlog.Address[:])
		tlog.Topics = make([]string, 0, len(rlog.Topics))
		for _, rtopic := range rlog.Topics {
			tlog.Topics = append(tlog.Topics, rtopic.String())
		}
		trans.Logs = append(trans.Logs, &tlog)
	}

	trans.From = model.NewAccountAtBlock(block.Number)
	if receipt.From != nil {
		trans.From.Address = hexutil.Encode(receipt.From[:])
	}

	if receipt.To != nil {
		trans.To = model.NewAccountAtBlock(block.Number)
		trans.To.Address = hexutil.Encode(receipt.To[:])
	}

	if receipt.ContractAddress != nil {
		trans.CreatedContract = model.NewAccountAtBlock(block.Number)
		trans.CreatedContract.Address = hexutil.Encode(receipt.ContractAddress[:])
	}

	trans.AccessList = make([]*model.AccessTuple, len(receipt.AccessList))
	for i, entry := range receipt.AccessList {
		keys := make([]string, len(entry.StorageKeys))
		for j, k := range entry.StorageKeys {
			keys[j] = k.Hex()
		}
		trans.AccessList[i] = &model.AccessTuple{
			Address:     hexutil.Encode(entry.Address[:]),
			StorageKeys: keys,
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
			Address:  hexutil.Encode(l.Address[:]),
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
