package gevm

import (
	"context"
	"fmt"

	gevmhost "github.com/Giulio2002/gevm/host"
	gevmspec "github.com/Giulio2002/gevm/spec"
	gevmstate "github.com/Giulio2002/gevm/state"
	gevmtypes "github.com/Giulio2002/gevm/types"
	gevmvm "github.com/Giulio2002/gevm/vm"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules"
	erigonstate "github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/erigontech/erigon/node/shards"
)

type BlockExecutor struct {
	evm         *gevmhost.Evm
	reader      *erigonState
	stateWriter erigonstate.StateWriter
	outBuf      []byte
	logsBuf     []gevmstate.Log
}

type TxInput struct {
	Tx      types.Transaction
	Sender  accounts.Address
	TxIndex int
	Hooks   *tracing.Hooks
}

type TxOutput struct {
	Result evmtypes.ExecutionResult
	Logs   []*types.Log
}

func NewBlockExecutor(ctx context.Context, tx kv.TemporalTx, domains *execctx.SharedDomains, blockReader interface {
	Header(context.Context, kv.Getter, common.Hash, uint64) (*types.Header, error)
}, chainConfig *chain.Config, engine rules.Engine, header *types.Header, blockCtx evmtypes.BlockContext, stateWriter erigonstate.StateWriter, initTxNum uint64, accumulator *shards.Accumulator) (*BlockExecutor, error) {
	return newBlockExecutor(ctx, tx, domains, blockReader, chainConfig, engine, header, blockCtx, stateWriter, initTxNum, accumulator, true)
}

func NewBlockExecutorAtCurrentState(ctx context.Context, tx kv.TemporalTx, domains *execctx.SharedDomains, blockReader interface {
	Header(context.Context, kv.Getter, common.Hash, uint64) (*types.Header, error)
}, chainConfig *chain.Config, header *types.Header, blockCtx evmtypes.BlockContext, stateWriter erigonstate.StateWriter) *BlockExecutor {
	be, _ := newBlockExecutor(ctx, tx, domains, blockReader, chainConfig, nil, header, blockCtx, stateWriter, 0, nil, false)
	return be
}

func newBlockExecutor(ctx context.Context, tx kv.TemporalTx, domains *execctx.SharedDomains, blockReader interface {
	Header(context.Context, kv.Getter, common.Hash, uint64) (*types.Header, error)
}, chainConfig *chain.Config, engine rules.Engine, header *types.Header, blockCtx evmtypes.BlockContext, stateWriter erigonstate.StateWriter, initTxNum uint64, accumulator *shards.Accumulator, initialize bool) (*BlockExecutor, error) {
	reader := erigonstate.NewReaderV3(domains.AsGetter(tx))
	stateDB := &erigonState{reader: reader, tx: tx, blockReader: blockReader, header: header, ctx: ctx}
	block := gevmhost.BlockEnv{
		Beneficiary:  toGevmAddress(blockCtx.Coinbase),
		Timestamp:    *uint256.NewInt(blockCtx.Time),
		Number:       *uint256.NewInt(blockCtx.BlockNumber),
		Difficulty:   blockCtx.Difficulty,
		GasLimit:     *uint256.NewInt(blockCtx.GasLimit),
		BaseFee:      blockCtx.BaseFee,
		BlobGasPrice: blockCtx.BlobBaseFee,
		SlotNum:      *uint256.NewInt(blockCtx.SlotNumber),
	}
	if blockCtx.PrevRanDao != nil {
		v := uint256.NewInt(0).SetBytes32(blockCtx.PrevRanDao[:])
		block.Prevrandao = v
	}
	block.GetHash = stateDB.BlockHash
	cfg := gevmhost.CfgEnv{}
	if chainConfig != nil && chainConfig.ChainID != nil {
		cfg.ChainId.SetFromBig(chainConfig.ChainID)
	}
	e := gevmhost.NewEvm(stateDB, forkID(blockCtx.Rules(chainConfig), blockCtx.PrevRanDao != nil), block, cfg)
	be := &BlockExecutor{evm: e, reader: stateDB, stateWriter: stateWriter}
	if initialize {
		if err := be.initializeBlock(chainConfig, engine, header, initTxNum, accumulator); err != nil {
			e.ReleaseEvm()
			return nil, err
		}
	}
	return be, nil
}

func (be *BlockExecutor) Close() {
	if be != nil && be.evm != nil {
		be.evm.ReleaseEvm()
		be.evm = nil
	}
}

func (be *BlockExecutor) ExecuteTx(input TxInput) (*TxOutput, error) {
	tx, err := makeTransaction(input.Tx, input.Sender)
	if err != nil {
		return nil, err
	}
	if input.Hooks != nil {
		be.evm.SetHooks(makeHooks(input.Hooks))
	} else {
		be.evm.SetHooks(nil)
	}
	result, outBuf, logsBuf := be.evm.TransactInPlace(tx, be.outBuf, be.logsBuf)
	be.outBuf = outBuf
	be.logsBuf = logsBuf
	if result.ValidationError {
		return nil, fmt.Errorf("gevm transaction validation failed: %s", result.Reason)
	}
	be.evm.Journal.CommitTx()
	evmResult := evmtypes.ExecutionResult{
		ReceiptGasUsed: result.GasUsed,
		BlockGasUsed:   result.GasUsed,
		ReturnData:     common.Copy(result.Output),
		Reverted:       result.IsRevert(),
	}
	if result.IsRevert() {
		evmResult.Err = vm.ErrExecutionReverted
	} else if result.IsHalt() {
		evmResult.Err = result.Reason
	}
	return &TxOutput{Result: evmResult, Logs: makeLogs(result.Logs, input)}, nil
}

func (be *BlockExecutor) FinalizeBlock(chainConfig *chain.Config, engine rules.Engine, header *types.Header, receipts types.Receipts, withdrawals types.Withdrawals, chainReader rules.ChainReader, txNum uint64, accumulator *shards.Accumulator) error {
	rules := headerRules(chainConfig, header)
	if engine != nil {
		rewards, err := engine.CalculateRewards(chainConfig, header, nil, func(contract accounts.Address, data []byte) ([]byte, error) {
			return be.systemCall(contract, data, header, chainConfig)
		})
		if err != nil {
			return err
		}
		for _, reward := range rewards {
			if err := be.evm.Journal.BalanceIncr(toGevmAddress(reward.Beneficiary), reward.Amount); err != nil {
				return err
			}
		}
	}
	for _, withdrawal := range withdrawals {
		amount := new(uint256.Int).Mul(uint256.NewInt(withdrawal.Amount), uint256.NewInt(common.GWei))
		if err := be.evm.Journal.BalanceIncr(toGevmAddress(accounts.InternAddress(withdrawal.Address)), *amount); err != nil {
			return err
		}
	}
	if chainConfig != nil && chainConfig.IsPrague(header.Time) && header.RequestsHash != nil {
		requests := make(types.FlatRequests, 0, 3)
		var allLogs types.Logs
		for _, receipt := range receipts {
			if receipt == nil {
				return fmt.Errorf("nil receipt: block %d", header.Number.Uint64())
			}
			allLogs = append(allLogs, receipt.Logs...)
		}
		depositReqs, err := parseDepositLogs(allLogs, chainConfig)
		if err != nil {
			return err
		}
		requests = append(requests, depositReqs...)
		withdrawalReq, err := be.dequeueRequest(params.WithdrawalRequestAddress, types.WithdrawalRequestType, header, chainConfig)
		if err != nil {
			return err
		}
		if withdrawalReq != nil {
			requests = append(requests, *withdrawalReq)
		}
		consolidationReq, err := be.dequeueRequest(params.ConsolidationRequestAddress, types.ConsolidationRequestType, header, chainConfig)
		if err != nil {
			return err
		}
		if consolidationReq != nil {
			requests = append(requests, *consolidationReq)
		}
		rh := requests.Hash()
		if *header.RequestsHash != *rh {
			return fmt.Errorf("invalid requests root hash in header, expected: %v, got:%v", header.RequestsHash, rh)
		}
	}
	return be.applyState(rules, txNum, accumulator)
}

func (be *BlockExecutor) dequeueRequest(contract accounts.Address, requestType byte, header *types.Header, chainConfig *chain.Config) (*types.FlatRequest, error) {
	code, err := be.reader.Code(toGevmAddress(contract))
	if err != nil || len(code) == 0 {
		return nil, err
	}
	result, err := be.systemCall(contract, nil, header, chainConfig)
	if err != nil || result == nil {
		return nil, err
	}
	return &types.FlatRequest{Type: requestType, RequestData: result}, nil
}

func (be *BlockExecutor) initializeBlock(chainConfig *chain.Config, engine rules.Engine, header *types.Header, txNum uint64, accumulator *shards.Accumulator) error {
	if header == nil || header.Number.Sign() == 0 {
		return nil
	}
	if chainConfig != nil && chainConfig.IsCancun(header.Time) && header.ParentBeaconBlockRoot != nil {
		if _, err := be.systemCall(params.BeaconRootsAddress, header.ParentBeaconBlockRoot.Bytes(), header, chainConfig); err != nil {
			return err
		}
	}
	if chainConfig != nil && chainConfig.IsPrague(header.Time) {
		if err := be.storeBlockHash(header); err != nil {
			return err
		}
	}
	return be.applyState(headerRules(chainConfig, header), txNum, accumulator)
}

func (be *BlockExecutor) systemCall(contract accounts.Address, data []byte, header *types.Header, chainConfig *chain.Config) ([]byte, error) {
	result := be.evm.SystemCall(toGevmAddress(params.SystemAddress), toGevmAddress(contract), data)
	if result.IsHalt() {
		return nil, result.Reason
	}
	return result.Output, nil
}

func (be *BlockExecutor) storeBlockHash(header *types.Header) error {
	code, err := be.reader.Code(toGevmAddress(params.HistoryStorageAddress))
	if err != nil || len(code) == 0 || header.Number.Sign() == 0 {
		return err
	}
	slot := uint256.NewInt((header.Number.Uint64() - 1) % params.BlockHashHistoryServeWindow)
	value := uint256.NewInt(0).SetBytes32(header.ParentHash.Bytes())
	_, err = be.evm.Journal.SStore(toGevmAddress(params.HistoryStorageAddress), *slot, *value)
	return err
}

func (be *BlockExecutor) applyState(rules *chain.Rules, txNum uint64, accumulator *shards.Accumulator) error {
	if setter, ok := be.stateWriter.(interface{ SetTxNum(uint64) }); ok {
		setter.SetTxNum(txNum)
	}
	evmState := be.evm.Journal.Finalize()
	for address, account := range evmState {
		if !accountDirty(account) {
			continue
		}
		addr := toErigonAddress(address)
		original := toErigonAccount(account.BlockOriginalInfo)
		current := toErigonAccount(account.Info)
		if account.IsCreated() && current.Incarnation <= original.Incarnation {
			current.Incarnation = original.Incarnation + 1
		}
		if account.IsSelfdestructed() || (rules != nil && rules.IsSpuriousDragon && account.Info.IsEmpty() && !account.IsPreserveEmpty()) {
			if err := be.stateWriter.DeleteAccount(addr, &original); err != nil {
				return err
			}
			continue
		}
		if account.Info.Code != nil {
			if err := be.stateWriter.UpdateAccountCode(addr, current.Incarnation, current.CodeHash, account.Info.Code); err != nil {
				return err
			}
		}
		if account.IsCreated() {
			if err := be.stateWriter.CreateContract(addr); err != nil {
				return err
			}
		}
		for key, slot := range account.Storage {
			if slot.BlockOriginalValue == slot.PresentValue {
				continue
			}
			if err := be.stateWriter.WriteAccountStorage(addr, current.Incarnation, accounts.InternKey(common.Hash(gevmtypes.B256FromU256(key))), slot.BlockOriginalValue, slot.PresentValue); err != nil {
				return err
			}
		}
		if err := be.stateWriter.UpdateAccountData(addr, &original, &current); err != nil {
			return err
		}
	}
	return nil
}

func accountDirty(account *gevmstate.Account) bool {
	if account == nil {
		return false
	}
	if account.IsTouched() || account.IsCreated() || account.IsSelfdestructed() || account.IsPreserveEmpty() || account.IsStorageCleared() {
		return true
	}
	if accountInfoChanged(account.BlockOriginalInfo, account.Info) {
		return true
	}
	for _, slot := range account.Storage {
		if slot != nil && slot.BlockOriginalValue != slot.PresentValue {
			return true
		}
	}
	return false
}

func accountInfoChanged(original, current gevmstate.AccountInfo) bool {
	return original.Balance != current.Balance ||
		original.Nonce != current.Nonce ||
		original.Root != current.Root ||
		original.Incarnation != current.Incarnation ||
		original.CodeHash != current.CodeHash
}

type erigonState struct {
	reader      erigonstate.StateReader
	tx          kv.TemporalTx
	blockReader interface {
		Header(context.Context, kv.Getter, common.Hash, uint64) (*types.Header, error)
	}
	header *types.Header
	ctx    context.Context
}

func (s *erigonState) Basic(address gevmtypes.Address) (gevmstate.AccountInfo, bool, error) {
	acc, err := s.reader.ReadAccountData(toErigonAddress(address))
	if err != nil || acc == nil {
		return gevmstate.AccountInfo{}, false, err
	}
	return gevmstate.AccountInfo{
		Balance:     acc.Balance,
		Nonce:       acc.Nonce,
		Root:        gevmtypes.B256(acc.Root),
		Incarnation: acc.Incarnation,
		CodeHash:    gevmtypes.B256(acc.CodeHash.Value()),
	}, true, nil
}

func (s *erigonState) CodeByHash(codeHash gevmtypes.B256) (gevmtypes.Bytes, error) {
	return nil, fmt.Errorf("gevm code hash lookup unavailable for hash %x", codeHash)
}

func (s *erigonState) Code(address gevmtypes.Address) (gevmtypes.Bytes, error) {
	code, err := s.reader.ReadAccountCode(toErigonAddress(address))
	return gevmtypes.Bytes(code), err
}

func (s *erigonState) Storage(address gevmtypes.Address, index uint256.Int) (uint256.Int, error) {
	value, _, err := s.reader.ReadAccountStorage(toErigonAddress(address), accounts.InternKey(common.Hash(gevmtypes.B256FromU256(index))))
	return value, err
}

func (s *erigonState) HasStorage(address gevmtypes.Address) (bool, error) {
	return s.reader.HasStorage(toErigonAddress(address))
}

func (s *erigonState) BlockHash(number uint64) (gevmtypes.B256, error) {
	if s.header == nil || s.blockReader == nil {
		return gevmtypes.B256Zero, nil
	}
	getHash := protocol.GetHashFn(s.header, func(hash common.Hash, n uint64) (*types.Header, error) {
		return s.blockReader.Header(s.ctx, s.tx, hash, n)
	})
	hash, err := getHash(number)
	return gevmtypes.B256(hash), err
}

func makeTransaction(tx types.Transaction, sender accounts.Address) (*gevmhost.Transaction, error) {
	out := &gevmhost.Transaction{
		Kind:                 gevmhost.TxKindCall,
		TxType:               txType(tx.Type()),
		Caller:               toGevmAddress(sender),
		Value:                *tx.GetValue(),
		Input:                tx.GetData(),
		GasLimit:             tx.GetGasLimit(),
		GasPrice:             *tx.GetFeeCap(),
		MaxFeePerGas:         *tx.GetFeeCap(),
		MaxPriorityFeePerGas: *tx.GetTipCap(),
		Nonce:                tx.GetNonce(),
	}
	if to := tx.GetTo(); to == nil {
		out.Kind = gevmhost.TxKindCreate
	} else {
		out.To = gevmtypes.Address(*to)
	}
	if blobTx, ok := tx.(*types.BlobTx); ok {
		out.MaxFeePerBlobGas = *blobTx.MaxFeePerBlobGas
	}
	for _, item := range tx.GetAccessList() {
		gevmItem := gevmhost.AccessListItem{Address: gevmtypes.Address(item.Address)}
		for _, key := range item.StorageKeys {
			gevmItem.StorageKeys = append(gevmItem.StorageKeys, *uint256.NewInt(0).SetBytes32(key[:]))
		}
		out.AccessList = append(out.AccessList, gevmItem)
	}
	for _, hash := range tx.GetBlobHashes() {
		out.BlobHashes = append(out.BlobHashes, *uint256.NewInt(0).SetBytes32(hash[:]))
	}
	for _, auth := range tx.GetAuthorizations() {
		out.AuthorizationList = append(out.AuthorizationList, gevmhost.Authorization{
			ChainId: auth.ChainID,
			Address: gevmtypes.Address(auth.Address),
			Nonce:   auth.Nonce,
			YParity: auth.YParity,
			R:       gevmtypes.B256(auth.R.Bytes32()),
			S:       gevmtypes.B256(auth.S.Bytes32()),
		})
	}
	return out, nil
}

func txType(t byte) gevmhost.TxType {
	switch t {
	case types.AccessListTxType:
		return gevmhost.TxTypeEIP2930
	case types.DynamicFeeTxType:
		return gevmhost.TxTypeEIP1559
	case types.BlobTxType:
		return gevmhost.TxTypeEIP4844
	case types.SetCodeTxType:
		return gevmhost.TxTypeEIP7702
	default:
		return gevmhost.TxTypeLegacy
	}
}

func forkID(rules *chain.Rules, hasPrevRandao bool) gevmspec.ForkID {
	switch {
	case rules == nil:
		return gevmspec.Frontier
	case rules.IsOsaka:
		return gevmspec.Osaka
	case rules.IsPrague:
		return gevmspec.Prague
	case rules.IsCancun:
		return gevmspec.Cancun
	case rules.IsShanghai:
		return gevmspec.Shanghai
	case rules.IsLondon && hasPrevRandao:
		return gevmspec.Merge
	case rules.IsLondon:
		return gevmspec.London
	case rules.IsBerlin:
		return gevmspec.Berlin
	case rules.IsIstanbul:
		return gevmspec.Istanbul
	case rules.IsPetersburg:
		return gevmspec.Petersburg
	case rules.IsConstantinople:
		return gevmspec.Constantinople
	case rules.IsByzantium:
		return gevmspec.Byzantium
	case rules.IsSpuriousDragon:
		return gevmspec.SpuriousDragon
	case rules.IsTangerineWhistle:
		return gevmspec.Tangerine
	case rules.IsHomestead:
		return gevmspec.Homestead
	default:
		return gevmspec.Frontier
	}
}

func makeLogs(logs []gevmstate.Log, input TxInput) []*types.Log {
	out := make([]*types.Log, len(logs))
	for i := range logs {
		topics := make([]common.Hash, logs[i].NumTopics)
		for j := range topics {
			topics[j] = common.Hash(logs[i].Topics[j])
		}
		out[i] = &types.Log{
			Address: common.Address(logs[i].Address),
			Topics:  topics,
			Data:    common.Copy(logs[i].Data),
			TxIndex: uint(input.TxIndex),
		}
	}
	return out
}

func makeHooks(hooks *tracing.Hooks) *gevmvm.Hooks {
	if hooks == nil {
		return nil
	}
	return &gevmvm.Hooks{
		OnTxStart: func(gasLimit uint64, from, to gevmtypes.Address, value uint256.Int, input []byte, isCreate bool) {
			if hooks.OnTxStart != nil {
				hooks.OnTxStart(&tracing.VMContext{}, nil, toErigonAddress(from))
			}
		},
		OnTxEnd: func(gasUsed uint64, output []byte, err error) {},
		OnEnter: func(depth int, opType byte, from, to gevmtypes.Address, input []byte, gas uint64, value uint256.Int) {
			if hooks.OnEnter != nil {
				hooks.OnEnter(depth, opType, toErigonAddress(from), toErigonAddress(to), false, input, gas, value, nil)
			}
		},
		OnExit: func(depth int, output []byte, gasUsed uint64, err error, reverted bool) {
			if hooks.OnExit != nil {
				hooks.OnExit(depth, output, gasUsed, err, reverted)
			}
		},
	}
}

func byteToOp(op byte) vm.OpCode {
	return vm.OpCode(op)
}

func toGevmAddress(address accounts.Address) gevmtypes.Address {
	return gevmtypes.Address(address.Value())
}

func toErigonAddress(address gevmtypes.Address) accounts.Address {
	return accounts.InternAddress(common.Address(address))
}

func toErigonAccount(info gevmstate.AccountInfo) accounts.Account {
	acc := accounts.NewAccount()
	acc.Balance = info.Balance
	acc.Nonce = info.Nonce
	if info.Root != gevmtypes.B256Zero {
		acc.Root = common.Hash(info.Root)
	}
	acc.Incarnation = info.Incarnation
	acc.CodeHash = accounts.InternCodeHash(common.Hash(info.CodeHash))
	return acc
}

func headerRules(chainConfig *chain.Config, header *types.Header) *chain.Rules {
	if chainConfig == nil || header == nil {
		return &chain.Rules{}
	}
	blockCtx := evmtypes.BlockContext{BlockNumber: header.Number.Uint64(), Time: header.Time}
	return blockCtx.Rules(chainConfig)
}

func parseDepositLogs(logs types.Logs, chainConfig *chain.Config) (types.FlatRequests, error) {
	if chainConfig == nil {
		return nil, nil
	}
	request, err := misc.ParseDepositLogs(logs, chainConfig.DepositContract)
	if err != nil || request == nil {
		return nil, err
	}
	return types.FlatRequests{*request}, nil
}
