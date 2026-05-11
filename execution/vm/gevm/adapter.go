package gevm

import (
	"errors"
	"fmt"
	"sync"

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
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

const jumpDestCacheEntries = 1 << 20

var jumpDestCacheOnce sync.Once

type Executor struct {
	evm        *gevmhost.Evm
	writer     state.StateWriter
	hooks      *tracing.Hooks
	vmContext  tracing.VMContext
	outBuf     []byte
	logsBuf    []gevmstate.Log
	tx         gevmhost.Transaction
	accessList []gevmhost.AccessListItem
	blobHashes []uint256.Int
	authList   []gevmhost.Authorization
	txContext  evmtypes.TxContext
	logArena   []types.Log
	logPtrs    []*types.Log
	topicArena []common.Hash
	dataArena  []byte
}

func NewExecutor(reader *state.ReaderV3, writer state.StateWriter, blockCtx evmtypes.BlockContext, chainConfig *chain.Config, hooks *tracing.Hooks) *Executor {
	jumpDestCacheOnce.Do(func() {
		gevmvm.ResizeGlobalJumpDestCache(jumpDestCacheEntries)
	})
	forkID := forkID(blockCtx.Rules(chainConfig))
	block := gevmhost.BlockEnv{
		Beneficiary:  toAddress(blockCtx.Coinbase.Value()),
		Timestamp:    *uint256.NewInt(blockCtx.Time),
		Number:       *uint256.NewInt(blockCtx.BlockNumber),
		Difficulty:   blockCtx.Difficulty,
		GasLimit:     *uint256.NewInt(blockCtx.GasLimit),
		BaseFee:      blockCtx.BaseFee,
		BlobGasPrice: blockCtx.BlobBaseFee,
		SlotNum:      *uint256.NewInt(blockCtx.SlotNumber),
		GetHash: func(n uint64) (gevmtypes.B256, error) {
			h, err := blockCtx.GetHash(n)
			return toB256(h), err
		},
	}
	if blockCtx.PrevRanDao != nil {
		v := *new(uint256.Int).SetBytes32(blockCtx.PrevRanDao[:])
		block.Prevrandao = &v
		if block.Difficulty.IsZero() {
			block.Difficulty = v
		}
	}
	var chainID uint256.Int
	if chainConfig != nil && chainConfig.ChainID != nil {
		chainID.SetFromBig(chainConfig.ChainID)
	}
	e := gevmhost.NewEvm(reader.GevmDatabase(), forkID, block, gevmhost.CfgEnv{ChainId: chainID})
	gevmHooks := convertHooks(hooks)
	if gevmHooks != nil {
		e.SetHooks(gevmHooks)
	}
	return &Executor{
		evm:    e,
		writer: writer,
		hooks:  hooks,
		vmContext: tracing.VMContext{
			Coinbase:    blockCtx.Coinbase,
			BlockNumber: blockCtx.BlockNumber,
			Time:        blockCtx.Time,
			Random:      blockCtx.PrevRanDao,
			ChainConfig: chainConfig,
		},
	}
}

func NewExecutorForDomains(tx kv.TemporalTx, domains *execctx.SharedDomains, writer state.StateWriter, blockCtx evmtypes.BlockContext, chainConfig *chain.Config, hooks *tracing.Hooks) *Executor {
	reader := state.NewReaderV3(domains.AsGetter(tx))
	reader.SetBlockHashReader(blockCtx.GetHash)
	return NewExecutor(reader, writer, blockCtx, chainConfig, hooks)
}

func (e *Executor) Close() {
	if e.evm != nil {
		e.evm.ReleaseEvm()
		e.evm = nil
	}
}

func (e *Executor) SetWriter(writer state.StateWriter) {
	e.writer = writer
}

func (e *Executor) Execute(erigonTx types.Transaction, message protocol.Message) (evmtypes.ExecutionResult, []*types.Log, error) {
	tx := e.transaction(erigonTx, message)
	e.txContext = evmtypes.TxContext{
		Origin:     message.From(),
		GasPrice:   *message.GasPrice(),
		BlobHashes: message.BlobHashes(),
	}
	result, outBuf, logsBuf := e.evm.TransactInPlace(tx, e.outBuf, e.logsBuf)
	e.outBuf, e.logsBuf = outBuf, logsBuf
	logs := e.convertLogs(result.Logs)
	err := resultErr(result)
	var applyErr error
	if result.ValidationError {
		applyErr = err
		e.evm.Journal.DiscardTx()
	} else {
		e.evm.Journal.CommitTx()
	}
	return evmtypes.ExecutionResult{
		ReceiptGasUsed:      result.GasUsed,
		BlockRegularGasUsed: result.GasUsed,
		MaxGasUsed:          message.Gas(),
		Err:                 err,
		Reverted:            result.Kind == gevmhost.ResultRevert,
		ReturnData:          result.Output,
	}, logs, applyErr
}

func (e *Executor) ExecuteTransaction(erigonTx types.Transaction, signer types.Signer) (evmtypes.ExecutionResult, []*types.Log, error) {
	tx, err := e.transactionFromErigon(erigonTx, signer)
	if err != nil {
		return evmtypes.ExecutionResult{}, nil, err
	}
	if e.hooks != nil && e.hooks.OnTxStart != nil {
		from, _ := erigonTx.Sender(signer)
		e.vmContext.TxHash = erigonTx.Hash()
		e.vmContext.GasPrice = erigonTx.GetEffectiveGasTip(&e.evm.Block.BaseFee)
		e.hooks.OnTxStart(&e.vmContext, erigonTx, from)
	}
	result, outBuf, logsBuf := e.evm.TransactInPlace(tx, e.outBuf, e.logsBuf)
	e.outBuf, e.logsBuf = outBuf, logsBuf
	logs := e.convertLogs(result.Logs)
	err = resultErr(result)
	var applyErr error
	if result.ValidationError {
		applyErr = err
		e.evm.Journal.DiscardTx()
	} else {
		e.evm.Journal.CommitTx()
	}
	return evmtypes.ExecutionResult{
		ReceiptGasUsed:      result.GasUsed,
		BlockRegularGasUsed: result.GasUsed,
		MaxGasUsed:          erigonTx.GetGasLimit(),
		Err:                 err,
		Reverted:            result.Kind == gevmhost.ResultRevert,
		ReturnData:          result.Output,
	}, logs, applyErr
}

func (e *Executor) SystemCall(contract accounts.Address, data []byte) ([]byte, error) {
	result := e.evm.SystemCall(toAddress(params.SystemAddress.Value()), toAddress(contract.Value()), data)
	if result.Kind == gevmhost.ResultSuccess {
		e.evm.Journal.CommitTx()
		return result.Output, nil
	}
	e.evm.Journal.DiscardTx()
	return result.Output, resultErr(result)
}

func (e *Executor) HasCode(address accounts.Address) (bool, error) {
	load, err := e.evm.Journal.LoadAccount(toAddress(address.Value()))
	if err != nil {
		return false, err
	}
	info := load.Data.Info
	return !info.CodeHash.IsZero() && info.CodeHash != gevmtypes.KeccakEmpty, nil
}

func (e *Executor) StoreBlockHash(header *types.Header) error {
	hasCode, err := e.HasCode(params.HistoryStorageAddress)
	if err != nil || !hasCode {
		return err
	}
	headerNum := header.Number.Uint64()
	if headerNum == 0 {
		return nil
	}
	slot := *uint256.NewInt((headerNum - 1) % params.BlockHashHistoryServeWindow)
	value := *uint256.NewInt(0).SetBytes32(header.ParentHash.Bytes())
	_, err = e.evm.Journal.SStore(toAddress(params.HistoryStorageAddress.Value()), slot, value)
	if err == nil {
		e.evm.Journal.CommitTx()
	}
	return err
}

func (e *Executor) AddBalance(address accounts.Address, amount uint256.Int) error {
	if err := e.evm.Journal.BalanceIncr(toAddress(address.Value()), amount); err != nil {
		return err
	}
	e.evm.Journal.CommitTx()
	return nil
}

func (e *Executor) FinalizePostMerge(config *chain.Config, header *types.Header, receipts types.Receipts, withdrawals []*types.Withdrawal) error {
	_, err := e.FinalizePostMergeRequests(config, header, receipts, withdrawals)
	return err
}

func (e *Executor) FinalizePostMergeRequests(config *chain.Config, header *types.Header, receipts types.Receipts, withdrawals []*types.Withdrawal) (types.FlatRequests, error) {
	if withdrawals != nil {
		for _, w := range withdrawals {
			var amountInWei uint256.Int
			amountInWei.Mul(uint256.NewInt(w.Amount), uint256.NewInt(common.GWei))
			if err := e.AddBalance(accounts.InternAddress(w.Address), amountInWei); err != nil {
				return nil, err
			}
		}
	}
	if !config.IsPrague(header.Time) {
		return nil, nil
	}

	requests := make(types.FlatRequests, 0, 3)
	allLogs := make(types.Logs, 0, len(receipts)*4)
	for i, receipt := range receipts {
		if receipt == nil {
			return nil, fmt.Errorf("nil receipt: block %d, txId %d, receipts %s", header.Number, i, receipts)
		}
		allLogs = append(allLogs, receipt.Logs...)
	}
	depositReqs, err := misc.ParseDepositLogs(allLogs, config.DepositContract)
	if err != nil {
		return nil, fmt.Errorf("error: could not parse requests logs: %v", err)
	}
	if depositReqs != nil {
		requests = append(requests, *depositReqs)
	}

	withdrawalReq, err := e.dequeueRequest(config.GetWithdrawalRequestContract(), types.WithdrawalRequestType)
	if err != nil {
		return nil, err
	}
	if withdrawalReq != nil {
		requests = append(requests, *withdrawalReq)
	}
	consolidationReq, err := e.dequeueRequest(config.GetConsolidationRequestContract(), types.ConsolidationRequestType)
	if err != nil {
		return nil, err
	}
	if consolidationReq != nil {
		requests = append(requests, *consolidationReq)
	}
	if header.RequestsHash != nil {
		rh := requests.Hash()
		if *header.RequestsHash != *rh {
			return nil, fmt.Errorf("error: invalid requests root hash in header, expected: %v, got:%v", header.RequestsHash, rh)
		}
	}
	return requests, nil
}

func (e *Executor) dequeueRequest(address accounts.Address, requestType byte) (*types.FlatRequest, error) {
	hasCode, err := e.HasCode(address)
	if err != nil {
		return nil, err
	}
	if !hasCode {
		return nil, fmt.Errorf("system request contract has no code: %x", address)
	}
	out, err := e.SystemCall(address, nil)
	if err != nil {
		return nil, err
	}
	if out == nil {
		return nil, nil
	}
	return &types.FlatRequest{Type: requestType, RequestData: out}, nil
}

func (e *Executor) CommitBlock(rules *chain.Rules) error {
	if e.evm == nil || e.writer == nil {
		return nil
	}
	return applyState(e.evm.Journal.Finalize(), e.writer, rules)
}

func (e *Executor) transaction(erigonTx types.Transaction, message protocol.Message) *gevmhost.Transaction {
	tx := &e.tx
	*tx = gevmhost.Transaction{
		Kind:                 gevmhost.TxKindCall,
		TxType:               messageTxType(message),
		Caller:               toAddress(message.From().Value()),
		Value:                *message.Value(),
		Input:                gevmtypes.Bytes(message.Data()),
		GasLimit:             message.Gas(),
		GasPrice:             *message.GasPrice(),
		MaxFeePerGas:         *message.FeeCap(),
		MaxPriorityFeePerGas: *message.TipCap(),
		Nonce:                message.Nonce(),
	}
	if !message.To().IsNil() {
		tx.To = toAddress(message.To().Value())
	} else {
		tx.Kind = gevmhost.TxKindCreate
	}
	if maxBlob := message.MaxFeePerBlobGas(); maxBlob != nil {
		tx.MaxFeePerBlobGas = *maxBlob
	}
	e.accessList = e.accessList[:0]
	for _, item := range message.AccessList() {
		out := gevmhost.AccessListItem{Address: toAddress(item.Address)}
		if len(e.accessList) < cap(e.accessList) {
			out.StorageKeys = e.accessList[:len(e.accessList)+1][len(e.accessList)].StorageKeys[:0]
		}
		for _, key := range item.StorageKeys {
			var storageKey uint256.Int
			storageKey.SetBytes32(key[:])
			out.StorageKeys = append(out.StorageKeys, storageKey)
		}
		e.accessList = append(e.accessList, out)
	}
	tx.AccessList = e.accessList
	e.blobHashes = e.blobHashes[:0]
	for _, h := range message.BlobHashes() {
		var blobHash uint256.Int
		blobHash.SetBytes32(h[:])
		e.blobHashes = append(e.blobHashes, blobHash)
	}
	tx.BlobHashes = e.blobHashes
	e.authList = e.authList[:0]
	for _, auth := range message.Authorizations() {
		e.authList = append(e.authList, gevmhost.Authorization{
			ChainId: auth.ChainID,
			Address: toAddress(auth.Address),
			Nonce:   auth.Nonce,
			YParity: auth.YParity,
			R:       toB256(auth.R.Bytes32()),
			S:       toB256(auth.S.Bytes32()),
		})
	}
	tx.AuthorizationList = e.authList
	if erigonTx != nil {
		tx.TxType = txType(erigonTx.Type())
	}
	return tx
}

func messageTxType(message protocol.Message) gevmhost.TxType {
	switch {
	case len(message.Authorizations()) > 0:
		return gevmhost.TxTypeEIP7702
	case len(message.BlobHashes()) > 0:
		return gevmhost.TxTypeEIP4844
	case len(message.AccessList()) > 0:
		return gevmhost.TxTypeEIP2930
	case !message.FeeCap().IsZero() || !message.TipCap().IsZero():
		return gevmhost.TxTypeEIP1559
	default:
		return gevmhost.TxTypeLegacy
	}
}

func (e *Executor) transactionFromErigon(erigonTx types.Transaction, signer types.Signer) (*gevmhost.Transaction, error) {
	from, err := erigonTx.Sender(signer)
	if err != nil {
		return nil, err
	}
	tx := &e.tx
	*tx = gevmhost.Transaction{
		Kind:                 gevmhost.TxKindCall,
		TxType:               txType(erigonTx.Type()),
		Caller:               toAddress(from.Value()),
		Value:                *erigonTx.GetValue(),
		Input:                gevmtypes.Bytes(erigonTx.GetData()),
		GasLimit:             erigonTx.GetGasLimit(),
		GasPrice:             *erigonTx.GetFeeCap(),
		MaxFeePerGas:         *erigonTx.GetFeeCap(),
		MaxPriorityFeePerGas: *erigonTx.GetTipCap(),
		Nonce:                erigonTx.GetNonce(),
	}
	if to := erigonTx.GetTo(); to != nil {
		tx.To = toAddress(*to)
	} else {
		tx.Kind = gevmhost.TxKindCreate
	}
	if maxBlob := maxFeePerBlobGas(erigonTx); maxBlob != nil {
		tx.MaxFeePerBlobGas = *maxBlob
	}
	e.accessList = e.accessList[:0]
	for _, item := range erigonTx.GetAccessList() {
		out := gevmhost.AccessListItem{Address: toAddress(item.Address)}
		if len(e.accessList) < cap(e.accessList) {
			out.StorageKeys = e.accessList[:len(e.accessList)+1][len(e.accessList)].StorageKeys[:0]
		}
		for _, key := range item.StorageKeys {
			var storageKey uint256.Int
			storageKey.SetBytes32(key[:])
			out.StorageKeys = append(out.StorageKeys, storageKey)
		}
		e.accessList = append(e.accessList, out)
	}
	tx.AccessList = e.accessList
	e.blobHashes = e.blobHashes[:0]
	for _, h := range erigonTx.GetBlobHashes() {
		var blobHash uint256.Int
		blobHash.SetBytes32(h[:])
		e.blobHashes = append(e.blobHashes, blobHash)
	}
	tx.BlobHashes = e.blobHashes
	e.authList = e.authList[:0]
	for _, auth := range erigonTx.GetAuthorizations() {
		e.authList = append(e.authList, gevmhost.Authorization{
			ChainId: auth.ChainID,
			Address: toAddress(auth.Address),
			Nonce:   auth.Nonce,
			YParity: auth.YParity,
			R:       toB256(auth.R.Bytes32()),
			S:       toB256(auth.S.Bytes32()),
		})
	}
	tx.AuthorizationList = e.authList
	return tx, nil
}

func maxFeePerBlobGas(tx types.Transaction) *uint256.Int {
	if blob, ok := tx.(*types.BlobTx); ok {
		return &blob.MaxFeePerBlobGas
	}
	if blob, ok := tx.(*types.BlobTxWrapper); ok {
		return &blob.Tx.MaxFeePerBlobGas
	}
	return nil
}

func txType(erigonType byte) gevmhost.TxType {
	switch erigonType {
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

func applyState(evmState gevmstate.EvmState, writer state.StateWriter, rules *chain.Rules) error {
	defer clear(evmState)
	rawWriter, _ := writer.(interface {
		DeleteAccountRaw(common.Address, *accounts.Account) error
		UpdateAccountDataRaw(common.Address, *accounts.Account, *accounts.Account) error
		UpdateAccountDataRawWithPrev(common.Address, *accounts.Account, *accounts.Account, []byte) error
		WriteAccountStorageRaw(common.Address, uint64, common.Hash, uint256.Int, uint256.Int) error
		WriteAccountStorageRawWithPrev(common.Address, uint64, common.Hash, uint256.Int, uint256.Int, []byte) error
		CreateContractRaw(common.Address) error
	})
	var emptyPrev []byte
	for addr, acc := range evmState {
		if acc.IsLoadedAsNotExistingNotTouched() {
			continue
		}
		if !needsWrite(acc, rules) {
			continue
		}
		addressValue := toCommonAddress(addr)
		var address accounts.Address
		erigonAddress := func() accounts.Address {
			if address.IsNil() {
				address = accounts.InternAddress(addressValue)
			}
			return address
		}
		accountIsEmpty := acc.Info.Nonce == 0 && acc.Info.Balance.IsZero() && isEmptyCodeHash(acc.Info.CodeHash)
		touchedEmptyPreSpurious := acc.IsTouched() && acc.IsLoadedAsNotExisting() && accountIsEmpty && (rules == nil || !rules.IsSpuriousDragon)
		storageIncarnation := acc.Info.Incarnation
		var updatedCodeHash accounts.CodeHash
		hasCodeUpdate := acc.Info.Code != nil && (acc.IsCreated() || acc.Info.CodeHash != acc.BlockOriginalInfo.CodeHash)
		if acc.IsSelfdestructed() || shouldDeleteEmptyAccount(acc, rules, accountIsEmpty) {
			original := toAccount(acc.BlockOriginalInfo)
			var err error
			if rawWriter != nil {
				err = rawWriter.DeleteAccountRaw(addressValue, &original)
			} else {
				err = writer.DeleteAccount(erigonAddress(), &original)
			}
			if err != nil {
				return err
			}
			continue
		}
		if acc.IsCreated() {
			var err error
			if rawWriter != nil {
				err = rawWriter.CreateContractRaw(addressValue)
			} else {
				err = writer.CreateContract(erigonAddress())
			}
			if err != nil {
				return err
			}
		}
		if hasCodeUpdate {
			updatedCodeHash = accounts.InternCodeHash(toHash(acc.Info.CodeHash))
			codeIncarnation := acc.Info.Incarnation
			if acc.IsCreated() && codeIncarnation == 0 {
				codeIncarnation = state.FirstContractIncarnation
			}
			storageIncarnation = codeIncarnation
			if err := writer.UpdateAccountCode(erigonAddress(), codeIncarnation, updatedCodeHash, acc.Info.Code); err != nil {
				return err
			}
		}
		if touchedEmptyPreSpurious || accountInfoChanged(acc.BlockOriginalInfo, acc.Info) {
			original := toAccount(acc.BlockOriginalInfo)
			current := toAccount(acc.Info)
			if hasCodeUpdate {
				current.CodeHash = updatedCodeHash
				current.Incarnation = storageIncarnation
			}
			var err error
			if rawWriter != nil {
				err = rawWriter.UpdateAccountDataRawWithPrev(addressValue, &original, &current, accountPrevValue(acc, &original, &emptyPrev))
			} else {
				err = writer.UpdateAccountData(erigonAddress(), &original, &current)
			}
			if err != nil {
				return err
			}
		}
		for key, slot := range acc.Storage {
			if slot.BlockOriginalValue == slot.PresentValue {
				continue
			}
			var err error
			if rawWriter != nil {
				err = rawWriter.WriteAccountStorageRawWithPrev(addressValue, storageIncarnation, toHash(gevmtypes.B256FromU256(key)), slot.BlockOriginalValue, slot.PresentValue, storagePrevValue(slot.BlockOriginalValue, &emptyPrev))
			} else {
				err = writer.WriteAccountStorage(erigonAddress(), storageIncarnation, toErigonKey(gevmtypes.B256FromU256(key)), slot.BlockOriginalValue, slot.PresentValue)
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func accountPrevValue(acc *gevmstate.Account, original *accounts.Account, emptyPrev *[]byte) []byte {
	if acc.IsLoadedAsNotExisting() {
		return nonNilEmpty(emptyPrev)
	}
	return accounts.SerialiseV3(original)
}

func storagePrevValue(original uint256.Int, emptyPrev *[]byte) []byte {
	if original.IsZero() {
		return nonNilEmpty(emptyPrev)
	}
	return original.Bytes()
}

func nonNilEmpty(emptyPrev *[]byte) []byte {
	if *emptyPrev == nil {
		*emptyPrev = make([]byte, 0)
	}
	return *emptyPrev
}

func needsWrite(acc *gevmstate.Account, rules *chain.Rules) bool {
	if acc.IsSelfdestructed() || acc.IsCreated() || acc.IsStorageCleared() || accountInfoChanged(acc.BlockOriginalInfo, acc.Info) {
		return true
	}
	if acc.IsTouched() && acc.IsLoadedAsNotExisting() && (rules == nil || !rules.IsSpuriousDragon) {
		return true
	}
	if shouldDeleteEmptyAccount(acc, rules, acc.Info.Nonce == 0 && acc.Info.Balance.IsZero() && isEmptyCodeHash(acc.Info.CodeHash)) {
		return true
	}
	for _, slot := range acc.Storage {
		if slot.BlockOriginalValue != slot.PresentValue {
			return true
		}
	}
	return false
}

func shouldDeleteEmptyAccount(acc *gevmstate.Account, rules *chain.Rules, accountIsEmpty bool) bool {
	return rules != nil && rules.IsSpuriousDragon && accountIsEmpty && acc.IsTouched()
}

func accountInfoChanged(original, current gevmstate.AccountInfo) bool {
	return original.Nonce != current.Nonce ||
		original.Balance != current.Balance ||
		original.Root != current.Root ||
		original.Incarnation != current.Incarnation ||
		original.CodeHash != current.CodeHash
}

func isEmptyCodeHash(hash gevmtypes.B256) bool {
	return hash == gevmtypes.KeccakEmpty || hash.IsZero()
}

func (e *Executor) convertLogs(logs []gevmstate.Log) []*types.Log {
	if len(logs) == 0 {
		return nil
	}
	ptrStart := len(e.logPtrs)
	for _, l := range logs {
		gevmTopics := l.TopicSlice()
		topicStart := len(e.topicArena)
		for _, topic := range gevmTopics {
			e.topicArena = append(e.topicArena, toHash(topic))
		}
		dataStart := len(e.dataArena)
		e.dataArena = append(e.dataArena, l.Data...)
		e.logArena = append(e.logArena, types.Log{
			Address: toCommonAddress(l.Address),
			Topics:  e.topicArena[topicStart:len(e.topicArena):len(e.topicArena)],
			Data:    e.dataArena[dataStart:len(e.dataArena):len(e.dataArena)],
		})
		e.logPtrs = append(e.logPtrs, &e.logArena[len(e.logArena)-1])
	}
	return e.logPtrs[ptrStart:len(e.logPtrs):len(e.logPtrs)]
}

func convertHooks(hooks *tracing.Hooks) *gevmvm.Hooks {
	if hooks == nil {
		return nil
	}
	return &gevmvm.Hooks{
		OnTxStart: func(gasLimit uint64, from, to gevmtypes.Address, value uint256.Int, input []byte, isCreate bool) {},
		OnTxEnd:   func(gasUsed uint64, output []byte, err error) {},
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

func resultErr(result gevmhost.ExecutionResult) error {
	switch result.Kind {
	case gevmhost.ResultSuccess:
		return nil
	case gevmhost.ResultRevert:
		return vm.ErrExecutionReverted
	case gevmhost.ResultHalt:
		return errors.New(result.Reason.String())
	default:
		return fmt.Errorf("unknown GEVM result kind %d", result.Kind)
	}
}

func forkID(rules *chain.Rules) gevmspec.ForkID {
	if rules == nil {
		return gevmspec.Frontier
	}
	switch {
	case rules.IsOsaka:
		return gevmspec.Osaka
	case rules.IsPrague:
		return gevmspec.Prague
	case rules.IsCancun:
		return gevmspec.Cancun
	case rules.IsShanghai:
		return gevmspec.Shanghai
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

func toAccount(info gevmstate.AccountInfo) accounts.Account {
	codeHash := accounts.EmptyCodeHash
	if !isEmptyCodeHash(info.CodeHash) {
		codeHash = accounts.InternCodeHash(toHash(info.CodeHash))
	}
	return accounts.Account{
		Nonce:       info.Nonce,
		Balance:     info.Balance,
		Root:        toHash(info.Root),
		CodeHash:    codeHash,
		Incarnation: info.Incarnation,
	}
}

func toAddress(a common.Address) gevmtypes.Address       { return gevmtypes.Address(a) }
func toCommonAddress(a gevmtypes.Address) common.Address { return common.Address(a) }
func toErigonAddress(a gevmtypes.Address) accounts.Address {
	return accounts.InternAddress(toCommonAddress(a))
}
func toHash(h gevmtypes.B256) common.Hash              { return common.Hash(h) }
func toB256(h [32]byte) gevmtypes.B256                 { return gevmtypes.B256(h) }
func toErigonKey(h gevmtypes.B256) accounts.StorageKey { return accounts.InternKey(toHash(h)) }
