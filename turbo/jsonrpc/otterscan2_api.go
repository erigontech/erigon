package jsonrpc

import (
	"context"
	"fmt"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/accounts/abi"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/adapter/ethapi"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/log/v3"
)

type Otterscan2API interface {
	GetAllContractsList(ctx context.Context, idx, count uint64) (*ContractListResult, error)
	GetAllContractsCount(ctx context.Context) (uint64, error)
	GetERC20List(ctx context.Context, idx, count uint64) (*ContractListResult, error)
	GetERC20Count(ctx context.Context) (uint64, error)
	GetERC721List(ctx context.Context, idx, count uint64) (*ContractListResult, error)
	GetERC721Count(ctx context.Context) (uint64, error)
	GetERC1155List(ctx context.Context, idx, count uint64) (*ContractListResult, error)
	GetERC1155Count(ctx context.Context) (uint64, error)
	GetERC1167List(ctx context.Context, idx, count uint64) (*ContractListResult, error)
	GetERC1167Count(ctx context.Context) (uint64, error)
	GetERC4626List(ctx context.Context, idx, count uint64) (*ContractListResult, error)
	GetERC4626Count(ctx context.Context) (uint64, error)

	GetERC1167Impl(ctx context.Context, addr common.Address) (common.Address, error)

	GetAddressAttributes(ctx context.Context, addr common.Address) (*AddrAttributes, error)

	GetERC20TransferList(ctx context.Context, addr common.Address, idx, count uint64) (*TransactionListResult, error)
	GetERC20TransferCount(ctx context.Context, addr common.Address) (uint64, error)
	GetERC721TransferList(ctx context.Context, addr common.Address, idx, count uint64) (*TransactionListResult, error)
	GetERC721TransferCount(ctx context.Context, addr common.Address) (uint64, error)
	GetERC20Holdings(ctx context.Context, addr common.Address) ([]*HoldingMatch, error)
	GetERC721Holdings(ctx context.Context, addr common.Address) ([]*HoldingMatch, error)

	GetWithdrawalsList(ctx context.Context, addr common.Address, idx, count uint64) (*WithdrawalsListResult, error)
	GetWithdrawalsCount(ctx context.Context, addr common.Address) (uint64, error)

	TransferIntegrityChecker(ctx context.Context) error
	HoldingsIntegrityChecker(ctx context.Context) error
}

type Otterscan2APIImpl struct {
	*BaseAPI
	db kv.RoDB
}

func NewOtterscan2API(base *BaseAPI, db kv.RoDB) *Otterscan2APIImpl {
	return &Otterscan2APIImpl{
		BaseAPI: base,
		db:      db,
	}
}

// Max results that can be requested by genericMatchingList callers to avoid node DoS
const MAX_MATCH_COUNT = uint64(500)

type BlockSummary struct {
	Block hexutil.Uint64 `json:"blockNumber"`
	Time  uint64         `json:"timestamp"`
}

type AddrMatch struct {
	Block   *hexutil.Uint64 `json:"blockNumber"`
	Address *common.Address `json:"address"`
}

func ToBlockSlice(addrMatches []AddrMatch) []hexutil.Uint64 {
	res := make([]hexutil.Uint64, 0, len(addrMatches))
	for _, m := range addrMatches {
		res = append(res, *m.Block)
	}
	return res
}

func (api *Otterscan2APIImpl) newBlocksSummaryFromResults(ctx context.Context, tx kv.Tx, res []hexutil.Uint64) (map[hexutil.Uint64]*BlockSummary, error) {
	ret := make(map[hexutil.Uint64]*BlockSummary, 0)

	for _, m := range res {
		if _, ok := ret[m]; ok {
			continue
		}

		header, err := api._blockReader.HeaderByNumber(ctx, tx, uint64(m))
		if err != nil {
			return nil, err
		}
		ret[m] = &BlockSummary{m, header.Time}
	}

	return ret, nil
}

// Given an address search match, extract some extra data by running EVM against it
type ExtraDataExtractor func(tx kv.Tx, res *AddrMatch, addr common.Address, evm *vm.EVM, header *types.Header, chainConfig *chain.Config, ibs *state.IntraBlockState, stateReader state.StateReader) (interface{}, error)

func (api *Otterscan2APIImpl) genericExtraData(ctx context.Context, tx kv.Tx, res []AddrMatch, extraData ExtraDataExtractor) ([]interface{}, error) {
	newRes := make([]interface{}, 0, len(res))

	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()

	var evm *vm.EVM
	stateReader := state.NewPlainStateReader(tx)
	// var ibs *state.IntraBlockState
	ibs := state.New(stateReader)
	prevBlock := uint64(0)

	blockReader := api._blockReader
	chainConfig, err := api.chainConfig(tx)
	if err != nil {
		return nil, err
	}
	getHeader := func(hash common.Hash, number uint64) *types.Header {
		h, e := blockReader.Header(ctx, tx, hash, number)
		if e != nil {
			log.Error("getHeader error", "number", number, "hash", hash, "err", e)
		}
		return h
	}
	engine := api.engine()

	blockNumber, hash, _, err := rpchelper.GetCanonicalBlockNumber(rpc.BlockNumberOrHashWithNumber(rpc.LatestExecutedBlockNumber), tx, nil)
	if err != nil {
		return nil, err
	}
	block, err := api.blockWithSenders(tx, hash, blockNumber)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}
	header := block.HeaderNoCopy()

	for _, r := range res {
		// TODO: this is failing when block is the tip, hence block + 1 header can't be found!
		/////////////////////
		originalBlockNum := uint64(*r.Block + 1)

		// var header *types.Header
		// header, err = blockReader.HeaderByNumber(ctx, tx, originalBlockNum)
		// if err != nil {
		// 	return nil, err
		// }
		if header == nil {
			return nil, fmt.Errorf("couldn't find header for block %d", originalBlockNum)
		}

		if stateReader == nil {
			// stateReader = state.NewPlainStateReader(tx)
			// stateReader = state.NewPlainState(tx, originalBlockNum+1, systemcontracts.SystemContractCodeLookup[chainConfig.ChainName])
			// ibs = state.New(stateReader)
		} else if originalBlockNum != prevBlock {
			// stateReader.SetBlockNr(originalBlockNum + 1)
			// ibs.Reset()
		}

		if evm == nil {
			blockCtx := core.NewEVMBlockContext(header, core.GetHashFn(header, getHeader), engine, nil /* author */)
			evm = vm.NewEVM(blockCtx, evmtypes.TxContext{}, ibs, chainConfig, vm.Config{NoBaseFee: true})
		} else {
			if originalBlockNum != prevBlock {
				// reset block
				blockCtx := core.NewEVMBlockContext(header, core.GetHashFn(header, getHeader), engine, nil /* author */)
				evm.ResetBetweenBlocks(blockCtx, evmtypes.TxContext{}, ibs, vm.Config{NoBaseFee: true}, chainConfig.Rules(blockCtx.BlockNumber, blockCtx.Time))
			}
		}
		prevBlock = originalBlockNum
		/////////////////////

		addr := r.Address
		ibs.Reset()
		extra, err := extraData(tx, &r, *addr, evm, header, chainConfig, ibs, stateReader)
		if err != nil {
			return nil, err
		}
		newRes = append(newRes, extra)

		select {
		default:
		case <-ctx.Done():
			return nil, common.ErrStopped
		}
	}

	return newRes, nil
}

func decodeReturnData(ctx context.Context, addr *common.Address, data []byte, methodName string, header *types.Header, evm *vm.EVM, chainConfig *chain.Config, ibs *state.IntraBlockState, contractABI *abi.ABI) (interface{}, error) {
	gas := hexutil.Uint64(header.GasLimit)
	args := ethapi.CallArgs{
		To:   addr,
		Data: (*hexutility.Bytes)(&data),
		Gas:  &gas,
	}
	ret, err := probeContract(ctx, evm, header, chainConfig, ibs, args)
	if err != nil {
		// internal error
		return nil, err
	}

	if ret.Err != nil {
		// ignore on purpose; i.e., out of gas signals error here
		log.Warn(fmt.Sprintf("error while trying to unpack %s: %v", methodName, ret.Err))
		return nil, nil
	}

	retVal, err := contractABI.Unpack(methodName, ret.ReturnData)
	if err != nil {
		// ignore on purpose; untrusted contract doesn't comply to expected ABI
		log.Warn(fmt.Sprintf("error while trying to unpack %s: %v", methodName, err))
		return nil, nil
	}

	return retVal[0], nil
}

func probeContract(ctx context.Context, evm *vm.EVM, header *types.Header, chainConfig *chain.Config, state *state.IntraBlockState, args ethapi.CallArgs) (*core.ExecutionResult, error) {
	var baseFee *uint256.Int
	if header != nil && header.BaseFee != nil {
		var overflow bool
		baseFee, overflow = uint256.FromBig(header.BaseFee)
		if overflow {
			return nil, fmt.Errorf("header.BaseFee uint256 overflow")
		}
	}
	msg, err := args.ToMessage(0, baseFee)
	if err != nil {
		return nil, err
	}

	txCtx := core.NewEVMTxContext(msg)
	state.Reset()
	evm.Reset(txCtx, state)

	gp := new(core.GasPool).AddGas(msg.Gas())
	result, err := core.ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */)
	if err != nil {
		return nil, err
	}

	// If the timer caused an abort, return an appropriate error message
	if evm.Cancelled() {
		return nil, fmt.Errorf("execution aborted (timeout = )")
	}
	return result, nil
}
