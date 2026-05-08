package gevm

import (
	"fmt"

	gevmhost "github.com/Giulio2002/gevm/host"
	gevmspec "github.com/Giulio2002/gevm/spec"
	gevmstate "github.com/Giulio2002/gevm/state"
	gevmtypes "github.com/Giulio2002/gevm/types"
	gevmvm "github.com/Giulio2002/gevm/vm"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/params"
	consensusrules "github.com/erigontech/erigon/execution/protocol/rules"
	erigonstate "github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/tracing/calltracer"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

type BlockExecutor struct {
	evm            *gevmhost.Evm
	rules          *chain.Rules
	header         *types.Header
	tx             gevmhost.Transaction
	runner         gevmvm.TracingRunner
	gevmHooks      gevmvm.Hooks
	tracer         *calltracer.CallTracer
	tracingHooks   *tracing.Hooks
	accessList     []gevmhost.AccessListItem
	accessListKeys []uint256.Int
	blobHashes     []uint256.Int
	authorizations []gevmhost.Authorization
	traceAddresses map[gevmtypes.Address]accounts.Address
	traceFroms     map[accounts.Address]struct{}
	traceTos       map[accounts.Address]struct{}
}

type TxOutput struct {
	Result          evmtypes.ExecutionResult
	ValidationError bool
	Logs            []*types.Log
	TraceFroms      map[accounts.Address]struct{}
	TraceTos        map[accounts.Address]struct{}
}

func NewBlockExecutor(chainConfig *chain.Config, header *types.Header, blockCtx evmtypes.BlockContext, tx kv.TemporalTx, domains *execctx.SharedDomains) *BlockExecutor {
	reader := erigonstate.NewReaderV3(domains.AsGetter(tx))
	return NewBlockExecutorWithReader(chainConfig, header, blockCtx, reader)
}

func NewBlockExecutorWithReader(chainConfig *chain.Config, header *types.Header, blockCtx evmtypes.BlockContext, reader erigonstate.StateReader) *BlockExecutor {
	rules := blockCtx.Rules(chainConfig)
	fid := forkID(rules)
	if fid == gevmspec.London && blockCtx.PrevRanDao != nil {
		fid = gevmspec.Merge
	}
	evm := gevmhost.NewEvm(newGevmDatabase(reader), fid, blockEnv(blockCtx), cfgEnv(chainConfig))
	b := &BlockExecutor{evm: evm, rules: rules, header: header}
	b.runner.DebugGasTable = gevmvm.DebugGasTableForFork(evm.ForkID)
	b.gevmHooks.OnEnter = b.onEnter
	return b
}

type rawReader interface {
	ReadAccountDataRaw(common.Address) ([]byte, *accounts.Account, error)
	ReadAccountStorageRaw(common.Address, common.Hash) (uint256.Int, bool, error)
	HasStorageRaw(common.Address) (bool, error)
	ReadAccountCodeRaw(common.Address) ([]byte, error)
}

// gevmDatabase implements gevmstate.Database by forwarding to Erigon's
// StateReader (and its `*Raw` fast-path if the reader exposes it).
type gevmDatabase struct {
	reader erigonstate.StateReader
	raw    rawReader
	hasRaw bool
}

func newGevmDatabase(reader erigonstate.StateReader) *gevmDatabase {
	raw, hasRaw := reader.(rawReader)
	return &gevmDatabase{reader: reader, raw: raw, hasRaw: hasRaw}
}

func (d *gevmDatabase) Basic(address gevmtypes.Address) (gevmstate.AccountInfo, bool, error) {
	var acc *accounts.Account
	var err error
	if d.hasRaw {
		_, acc, err = d.raw.ReadAccountDataRaw(common.Address(address))
	} else {
		acc, err = d.reader.ReadAccountData(toErigonAddress(address))
	}
	if err != nil || acc == nil {
		return gevmstate.AccountInfo{}, false, err
	}
	info := gevmstate.AccountInfo{
		Balance:     acc.Balance,
		Nonce:       acc.Nonce,
		Root:        gevmtypes.B256(acc.Root),
		Incarnation: acc.Incarnation,
		CodeHash:    gevmtypes.B256(acc.CodeHash.Value()),
	}
	if info.Incarnation == 0 && info.CodeHash != gevmtypes.B256Zero && info.CodeHash != gevmtypes.KeccakEmpty {
		info.Incarnation = 1
	}
	return info, true, nil
}

func (d *gevmDatabase) Storage(address gevmtypes.Address, key uint256.Int) (uint256.Int, error) {
	if d.hasRaw {
		value, _, err := d.raw.ReadAccountStorageRaw(common.Address(address), common.Hash(key.Bytes32()))
		return value, err
	}
	value, _, err := d.reader.ReadAccountStorage(toErigonAddress(address), storageKey(key))
	return value, err
}

func (d *gevmDatabase) HasStorage(address gevmtypes.Address) (bool, error) {
	if d.hasRaw {
		return d.raw.HasStorageRaw(common.Address(address))
	}
	return d.reader.HasStorage(toErigonAddress(address))
}

func (d *gevmDatabase) Code(address gevmtypes.Address) (gevmtypes.Bytes, error) {
	var code []byte
	var err error
	if d.hasRaw {
		code, err = d.raw.ReadAccountCodeRaw(common.Address(address))
	} else {
		code, err = d.reader.ReadAccountCode(toErigonAddress(address))
	}
	return gevmtypes.Bytes(code), err
}

// CodeByHash is unused on the Erigon path: GEVM's host loads code via
// Code(address); BLOCKHASH-style code-hash-keyed lookups don't happen
// during stage_exec. Returning nil here is safe.
func (d *gevmDatabase) CodeByHash(codeHash gevmtypes.B256) (gevmtypes.Bytes, error) {
	return nil, nil
}

// BlockHash is unused on the Erigon path: BlockEnv.GetHash is set in
// blockEnv() and EvmHost.BlockHash prefers it over the Database path.
func (d *gevmDatabase) BlockHash(number uint64) (gevmtypes.B256, error) {
	return gevmtypes.B256Zero, nil
}

func (b *BlockExecutor) Release() {
	if b.evm != nil {
		b.evm.ReleaseEvm()
		b.evm = nil
	}
}

func (b *BlockExecutor) ExecuteTx(tx types.Transaction, msg protocol.Message, tracer *calltracer.CallTracer) (TxOutput, error) {
	if tx != nil && tx.Type() == types.AccountAbstractionTxType {
		return TxOutput{}, fmt.Errorf("account abstraction transaction is not supported by GEVM")
	}
	typ := txTypeFromMessage(msg, b.rules)
	if tx != nil {
		typ = txType(tx.Type())
	}
	return b.executeTx(typ, msg, tracer)
}

func (b *BlockExecutor) ExecuteMessage(txType byte, msg protocol.Message, tracer *calltracer.CallTracer) (TxOutput, error) {
	if txType == types.AccountAbstractionTxType {
		return TxOutput{}, fmt.Errorf("account abstraction transaction is not supported by GEVM")
	}
	return b.executeTx(gevmTxType(txType), msg, tracer)
}

func (b *BlockExecutor) executeTx(typ gevmhost.TxType, msg protocol.Message, tracer *calltracer.CallTracer) (TxOutput, error) {
	if tracer != nil {
		b.tracer = tracer
		b.tracingHooks = tracer.TracingHooks()
		if b.tracingHooks != nil && b.tracingHooks.OnExit != nil {
			b.gevmHooks.OnExit = b.onExit
		} else {
			b.gevmHooks.OnExit = nil
		}
		b.evm.SetHooks(&b.gevmHooks)
		b.evm.Set(nil)
	} else {
		b.tracer = nil
		b.tracingHooks = nil
		b.gevmHooks.OnExit = nil
		b.clearTraceMaps()
		b.evm.SetHooks(&b.gevmHooks)
		b.evm.Set(nil)
	}
	res := b.evm.TransactBorrowed(b.transaction(typ, msg))
	b.evm.Journal.CommitTx()
	out := TxOutput{
		Result: evmtypes.ExecutionResult{
			ReceiptGasUsed:      res.GasUsed,
			BlockRegularGasUsed: res.GasUsed,
			MaxGasUsed:          res.GasUsed + uint64(max(res.GasRefund, 0)),
			ReturnData:          []byte(res.Output),
			Reverted:            res.Kind == gevmhost.ResultRevert,
		},
		ValidationError: res.ValidationError,
		Logs:            logs(res.Logs),
		TraceFroms:      b.traceFroms,
		TraceTos:        b.traceTos,
	}
	switch {
	case res.Kind == gevmhost.ResultRevert:
		out.Result.Err = vm.ErrExecutionReverted
	case res.Kind == gevmhost.ResultHalt:
		out.Result.Err = res.Reason
	}
	return out, nil
}

func (b *BlockExecutor) onEnter(depth int, typ byte, from, to gevmtypes.Address, input []byte, gas uint64, value uint256.Int) {
	fromAddr := b.traceAddress(from)
	toAddr := b.traceAddress(to)
	if b.tracer != nil {
		b.tracer.OnEnter(depth, typ, fromAddr, toAddr, false, input, gas, value, nil)
	} else {
		if b.traceFroms == nil {
			b.clearTraceMaps()
		}
		b.traceFroms[fromAddr] = struct{}{}
		b.traceTos[toAddr] = struct{}{}
	}
	if b.tracingHooks != nil && b.tracingHooks.OnEnter != nil {
		b.tracingHooks.OnEnter(depth, typ, fromAddr, toAddr, false, input, gas, value, nil)
	}
}

func (b *BlockExecutor) clearTraceMaps() {
	if b.traceFroms == nil {
		b.traceFroms = make(map[accounts.Address]struct{}, 8)
		b.traceTos = make(map[accounts.Address]struct{}, 8)
		return
	}
	clear(b.traceFroms)
	clear(b.traceTos)
}

func (b *BlockExecutor) traceAddress(addr gevmtypes.Address) accounts.Address {
	if b.traceAddresses == nil {
		b.traceAddresses = make(map[gevmtypes.Address]accounts.Address, 64)
	} else if out, ok := b.traceAddresses[addr]; ok {
		return out
	}
	out := toErigonAddress(addr)
	b.traceAddresses[addr] = out
	return out
}

func (b *BlockExecutor) onExit(depth int, output []byte, gasUsed uint64, err error, reverted bool) {
	if b.tracingHooks != nil && b.tracingHooks.OnExit != nil {
		b.tracingHooks.OnExit(depth, output, gasUsed, err, reverted)
	}
}

func (b *BlockExecutor) ApplyState(writer erigonstate.StateWriter) error {
	return applyState(b.evm.Journal.State, b.evm.Journal.StateAddresses(), writer, b.rules)
}

func (b *BlockExecutor) ApplyGenesisState(genesis *types.Genesis, writer erigonstate.StateWriter) error {
	if genesis == nil {
		return nil
	}
	for address, account := range genesis.Alloc {
		if len(account.Constructor) > 0 {
			return fmt.Errorf("genesis constructor allocation is not supported by GEVM")
		}
		var balance uint256.Int
		if account.Balance != nil {
			overflow := balance.SetFromBig(account.Balance)
			if overflow {
				return fmt.Errorf("genesis balance overflow for %x", address)
			}
		}
		code := gevmtypes.Bytes(account.Code)
		codeHash := gevmtypes.KeccakEmpty
		incarnation := uint64(0)
		if len(code) > 0 {
			codeHash = gevmtypes.Keccak256(code)
			incarnation = erigonstate.FirstContractIncarnation
		}
		storage := make(map[uint256.Int]uint256.Int, len(account.Storage))
		for key, value := range account.Storage {
			var k, v uint256.Int
			k.SetBytes32(key.Bytes())
			v.SetBytes32(value.Bytes())
			storage[k] = v
			if incarnation == 0 {
				incarnation = erigonstate.FirstContractIncarnation
			}
		}
		b.evm.Journal.PutAccount(gevmtypes.Address(address), gevmstate.AccountInfo{
			Balance:     balance,
			Nonce:       account.Nonce,
			Incarnation: incarnation,
			CodeHash:    codeHash,
			Code:        code,
		}, storage)
	}
	return applyState(b.evm.Journal.State, b.evm.Journal.StateAddresses(), writer, &chain.Rules{})
}

func (b *BlockExecutor) CommitTx() {
	b.evm.Journal.CommitTx()
}

func (b *BlockExecutor) ApplyBeaconRoot(root *common.Hash) {
	if root == nil {
		return
	}
	_, _ = b.systemCall(params.BeaconRootsAddress, root.Bytes())
}

func (b *BlockExecutor) StoreParentHash(header *types.Header) error {
	codeSize, err := b.codeSize(params.HistoryStorageAddress)
	if err != nil || codeSize == 0 || header.Number.Sign() == 0 {
		return err
	}
	slot := uint256.NewInt((header.Number.Uint64() - 1) % params.BlockHashHistoryServeWindow)
	key := accounts.InternKey(common.BytesToHash(slot.Bytes()))
	value := *uint256.NewInt(0).SetBytes32(header.ParentHash.Bytes())
	return b.writeStorage(params.HistoryStorageAddress, key, value)
}

func (b *BlockExecutor) AddBalance(address common.Address, amount uint256.Int) error {
	return b.evm.Journal.BalanceIncr(toGevmAddress(accounts.InternAddress(address)), amount)
}

func (b *BlockExecutor) FinalizeBlock(engine consensusrules.Engine, chainConfig *chain.Config, header *types.Header, uncles []*types.Header, receipts types.Receipts, withdrawals types.Withdrawals) (types.FlatRequests, error) {
	if engine != nil {
		rewards, err := engine.CalculateRewards(chainConfig, header, uncles, b.systemCall)
		if err != nil {
			return nil, err
		}
		for _, reward := range rewards {
			if err := b.AddBalance(reward.Beneficiary.Value(), reward.Amount); err != nil {
				return nil, err
			}
		}
	}

	for _, w := range withdrawals {
		amount := new(uint256.Int).Mul(uint256.NewInt(w.Amount), uint256.NewInt(common.GWei))
		if err := b.AddBalance(w.Address, *amount); err != nil {
			return nil, err
		}
	}

	if !chainConfig.IsPrague(header.Time) {
		return nil, nil
	}

	requests := make(types.FlatRequests, 0, 3)
	var allLogs types.Logs
	for _, receipt := range receipts {
		if receipt == nil {
			return nil, fmt.Errorf("nil receipt: block %d", header.Number.Uint64())
		}
		allLogs = append(allLogs, receipt.Logs...)
	}
	deposits, err := misc.ParseDepositLogs(allLogs, chainConfig.DepositContract)
	if err != nil {
		return nil, fmt.Errorf("error: could not parse requests logs: %v", err)
	}
	if deposits != nil {
		requests = append(requests, *deposits)
	}

	withdrawalReq, err := b.dequeueRequest(chainConfig.GetWithdrawalRequestContract(), types.WithdrawalRequestType)
	if err != nil {
		return nil, err
	}
	if withdrawalReq != nil {
		requests = append(requests, *withdrawalReq)
	}
	consolidationReq, err := b.dequeueRequest(chainConfig.GetConsolidationRequestContract(), types.ConsolidationRequestType)
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

func (b *BlockExecutor) systemCall(contract accounts.Address, input []byte) ([]byte, error) {
	res := b.evm.SystemCall(toGevmAddress(params.SystemAddress), toGevmAddress(contract), input)
	if res.Kind == gevmhost.ResultHalt {
		return nil, res.Reason
	}
	if res.Kind == gevmhost.ResultRevert {
		return nil, fmt.Errorf("system contract call reverted")
	}
	return []byte(res.Output), nil
}

func (b *BlockExecutor) dequeueRequest(contract accounts.Address, requestType byte) (*types.FlatRequest, error) {
	codeSize, err := b.codeSize(contract)
	if err != nil {
		return nil, err
	}
	if codeSize == 0 {
		return nil, fmt.Errorf("request contract has empty code: %x", contract)
	}
	res, err := b.systemCall(contract, nil)
	if err != nil {
		return nil, err
	}
	if res != nil {
		return &types.FlatRequest{Type: requestType, RequestData: res}, nil
	}
	return nil, nil
}

func (b *BlockExecutor) codeSize(address accounts.Address) (int, error) {
	load, err := b.evm.Journal.LoadAccount(toGevmAddress(address))
	if err != nil {
		return 0, err
	}
	if load.Data.Info.Code == nil && load.Data.Info.CodeHash != gevmtypes.KeccakEmpty && !load.Data.Info.CodeHash.IsZero() {
		code, err := b.evm.Journal.ReadCode(toGevmAddress(address))
		if err != nil {
			return 0, err
		}
		load.Data.Info.Code = code
	}
	return len(load.Data.Info.Code), nil
}

func (b *BlockExecutor) writeStorage(address accounts.Address, key accounts.StorageKey, value uint256.Int) error {
	addr := toGevmAddress(address)
	k := *new(uint256.Int).SetBytes32(key.Value().Bytes())
	_, err := b.evm.Journal.SStore(addr, k, value)
	return err
}

func storageKey(index uint256.Int) accounts.StorageKey {
	return accounts.InternKey(common.Hash(index.Bytes32()))
}

func applyState(st gevmstate.EvmState, addrs []gevmtypes.Address, writer erigonstate.StateWriter, rules *chain.Rules) error {
	rawStorageWriter, hasRawStorageWriter := writer.(interface {
		WriteAccountStorageRaw(common.Address, uint64, common.Hash, uint256.Int, uint256.Int) error
	})
	for _, addr := range addrs {
		acc := st[addr]
		if acc == nil {
			continue
		}
		emptyRemoval := rules != nil && rules.IsSpuriousDragon && acc.IsEmpty() && (acc.IsTouched() || acc.IsCreated())
		if acc.IsSelfdestructed() || emptyRemoval {
			address := toErigonAddress(addr)
			original := erigonAccount(acc.BlockOriginalInfo)
			if err := writer.DeleteAccount(address, &original); err != nil {
				return err
			}
			if acc.IsSelfdestructed() && !acc.Info.Balance.IsZero() {
				current := erigonAccount(gevmstate.AccountInfo{
					Balance:  acc.Info.Balance,
					CodeHash: gevmtypes.KeccakEmpty,
				})
				if err := writer.UpdateAccountData(address, &accounts.Account{}, &current); err != nil {
					return err
				}
			}
			continue
		}
		if acc.IsStorageCleared() && !acc.IsCreated() {
			address := toErigonAddress(addr)
			original := erigonAccount(acc.BlockOriginalInfo)
			if err := writer.DeleteAccount(address, &original); err != nil {
				return err
			}
		}
		accountChanged := accountInfoChanged(acc.BlockOriginalInfo, acc.Info) || acc.IsCreated() ||
			(rules != nil && !rules.IsSpuriousDragon && acc.IsLoadedAsNotExisting() && acc.IsTouched()) ||
			acc.IsPreserveEmpty()
		if accountChanged {
			address := toErigonAddress(addr)
			original := erigonOriginalAccount(acc)
			current := erigonAccount(acc.Info)
			if err := writer.UpdateAccountData(address, &original, &current); err != nil {
				return err
			}
		}
		if (acc.BlockOriginalInfo.CodeHash != acc.Info.CodeHash || acc.IsCreated()) && len(acc.Info.Code) > 0 && gevmtypes.Keccak256(acc.Info.Code) == acc.Info.CodeHash {
			address := toErigonAddress(addr)
			current := erigonAccount(acc.Info)
			if err := writer.UpdateAccountCode(address, current.Incarnation, current.CodeHash, []byte(acc.Info.Code)); err != nil {
				return err
			}
		}
		if acc.IsCreated() {
			address := toErigonAddress(addr)
			if err := writer.CreateContract(address); err != nil {
				return err
			}
		}
		incarnation := erigonIncarnation(acc.Info)
		for key, slot := range acc.Storage {
			if slot.BlockOriginalValue == slot.PresentValue {
				continue
			}
			keyHash := common.Hash(key.Bytes32())
			if hasRawStorageWriter {
				if err := rawStorageWriter.WriteAccountStorageRaw(common.Address(addr), incarnation, keyHash, slot.BlockOriginalValue, slot.PresentValue); err != nil {
					return err
				}
			} else {
				address := toErigonAddress(addr)
				storageKey := accounts.InternKey(keyHash)
				if err := writer.WriteAccountStorage(address, incarnation, storageKey, slot.BlockOriginalValue, slot.PresentValue); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func accountInfoChanged(a, b gevmstate.AccountInfo) bool {
	return a.Nonce != b.Nonce ||
		a.Incarnation != b.Incarnation ||
		a.CodeHash != b.CodeHash ||
		a.Balance != b.Balance
}

func erigonIncarnation(info gevmstate.AccountInfo) uint64 {
	if info.Incarnation != 0 {
		return info.Incarnation
	}
	if info.CodeHash != gevmtypes.B256Zero && info.CodeHash != gevmtypes.KeccakEmpty {
		return 1
	}
	return 0
}

func erigonAccount(info gevmstate.AccountInfo) accounts.Account {
	root := empty.RootHash
	if info.Root != gevmtypes.B256Zero {
		root = common.Hash(info.Root)
	}
	return accounts.Account{
		Nonce:       info.Nonce,
		Balance:     info.Balance,
		Root:        root,
		CodeHash:    accounts.InternCodeHash(common.Hash(info.CodeHash)),
		Incarnation: erigonIncarnation(info),
	}
}

func erigonOriginalAccount(acc *gevmstate.Account) accounts.Account {
	if acc.IsLoadedAsNotExisting() {
		return accounts.Account{}
	}
	return erigonAccount(acc.BlockOriginalInfo)
}

func blockEnv(ctx evmtypes.BlockContext) gevmhost.BlockEnv {
	var prevrandao *uint256.Int
	if ctx.PrevRanDao != nil {
		prevrandao = new(uint256.Int).SetBytes32(ctx.PrevRanDao.Bytes())
	}
	return gevmhost.BlockEnv{
		Beneficiary:  toGevmAddress(ctx.Coinbase),
		Timestamp:    *uint256.NewInt(ctx.Time),
		Number:       *uint256.NewInt(ctx.BlockNumber),
		Difficulty:   ctx.Difficulty,
		Prevrandao:   prevrandao,
		GasLimit:     *uint256.NewInt(ctx.GasLimit),
		BaseFee:      ctx.BaseFee,
		BlobGasPrice: ctx.BlobBaseFee,
		SlotNum:      *uint256.NewInt(ctx.SlotNumber),
		GetHash: func(number uint64) (gevmtypes.B256, error) {
			h, err := ctx.GetHash(number)
			return toGevmHash(h), err
		},
	}
}

func cfgEnv(config *chain.Config) gevmhost.CfgEnv {
	var chainID uint256.Int
	if config != nil && config.ChainID != nil {
		chainID.SetFromBig(config.ChainID)
	}
	return gevmhost.CfgEnv{ChainId: chainID}
}

func (b *BlockExecutor) transaction(typ gevmhost.TxType, msg protocol.Message) *gevmhost.Transaction {
	gtx := &b.tx
	*gtx = gevmhost.Transaction{
		Kind:                 gevmhost.TxKindCall,
		TxType:               typ,
		Caller:               toGevmAddress(msg.From()),
		Value:                *msg.Value(),
		Input:                msg.Data(),
		GasLimit:             msg.Gas(),
		GasPrice:             *msg.GasPrice(),
		MaxFeePerGas:         *msg.FeeCap(),
		MaxPriorityFeePerGas: *msg.TipCap(),
		MaxFeePerBlobGas:     *msg.MaxFeePerBlobGas(),
		Nonce:                msg.Nonce(),
		AccessList:           b.convertAccessList(msg.AccessList()),
		BlobHashes:           b.convertHashes(msg.BlobHashes()),
		AuthorizationList:    b.convertAuthorizations(msg.Authorizations()),
	}
	if msg.To().IsNil() {
		gtx.Kind = gevmhost.TxKindCreate
	} else {
		gtx.To = toGevmAddress(msg.To())
	}
	if !b.rules.IsLondon {
		gtx.MaxFeePerGas = gtx.GasPrice
		gtx.MaxPriorityFeePerGas = gtx.GasPrice
	}
	return gtx
}

func txTypeFromMessage(msg protocol.Message, rules *chain.Rules) gevmhost.TxType {
	switch {
	case len(msg.Authorizations()) > 0:
		return gevmhost.TxTypeEIP7702
	case len(msg.BlobHashes()) > 0:
		return gevmhost.TxTypeEIP4844
	case rules.IsLondon && msg.FeeCap() != nil && msg.TipCap() != nil:
		return gevmhost.TxTypeEIP1559
	case len(msg.AccessList()) > 0:
		return gevmhost.TxTypeEIP2930
	default:
		return gevmhost.TxTypeLegacy
	}
}

func txType(typ byte) gevmhost.TxType {
	return gevmTxType(typ)
}

func gevmTxType(typ byte) gevmhost.TxType {
	switch typ {
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

func (b *BlockExecutor) convertAccessList(list types.AccessList) []gevmhost.AccessListItem {
	if len(list) == 0 {
		b.accessList = b.accessList[:0]
		b.accessListKeys = b.accessListKeys[:0]
		return nil
	}
	if cap(b.accessList) < len(list) {
		b.accessList = make([]gevmhost.AccessListItem, len(list))
	} else {
		b.accessList = b.accessList[:len(list)]
	}
	keys := 0
	for _, item := range list {
		keys += len(item.StorageKeys)
	}
	if cap(b.accessListKeys) < keys {
		b.accessListKeys = make([]uint256.Int, keys)
	} else {
		b.accessListKeys = b.accessListKeys[:keys]
	}
	keyOffset := 0
	for i, item := range list {
		b.accessList[i].Address = gevmtypes.Address(item.Address)
		b.accessList[i].StorageKeys = b.accessListKeys[keyOffset : keyOffset+len(item.StorageKeys)]
		for j, key := range item.StorageKeys {
			b.accessList[i].StorageKeys[j].SetBytes32(key.Bytes())
		}
		keyOffset += len(item.StorageKeys)
	}
	return b.accessList
}

func (b *BlockExecutor) convertHashes(in []common.Hash) []uint256.Int {
	if len(in) == 0 {
		b.blobHashes = b.blobHashes[:0]
		return nil
	}
	if cap(b.blobHashes) < len(in) {
		b.blobHashes = make([]uint256.Int, len(in))
	} else {
		b.blobHashes = b.blobHashes[:len(in)]
	}
	for i, h := range in {
		b.blobHashes[i].SetBytes32(h.Bytes())
	}
	return b.blobHashes
}

func (b *BlockExecutor) convertAuthorizations(in []types.Authorization) []gevmhost.Authorization {
	if len(in) == 0 {
		b.authorizations = b.authorizations[:0]
		return nil
	}
	if cap(b.authorizations) < len(in) {
		b.authorizations = make([]gevmhost.Authorization, len(in))
	} else {
		b.authorizations = b.authorizations[:len(in)]
	}
	for i, a := range in {
		b.authorizations[i] = gevmhost.Authorization{
			ChainId: a.ChainID,
			Address: gevmtypes.Address(a.Address),
			Nonce:   a.Nonce,
			YParity: a.YParity,
			R:       gevmtypes.B256(a.R.Bytes32()),
			S:       gevmtypes.B256(a.S.Bytes32()),
		}
	}
	return b.authorizations
}

func logs(in []gevmstate.Log) []*types.Log {
	if len(in) == 0 {
		return nil
	}
	out := make([]*types.Log, len(in))
	for i := range in {
		topics := make([]common.Hash, in[i].NumTopics)
		for j, topic := range in[i].TopicSlice() {
			topics[j] = common.Hash(topic)
		}
		out[i] = &types.Log{
			Address: common.Address(in[i].Address),
			Topics:  topics,
			Data:    append([]byte(nil), in[i].Data...),
		}
	}
	return out
}

func forkID(r *chain.Rules) gevmspec.ForkID {
	switch {
	case r.IsOsaka:
		return gevmspec.Osaka
	case r.IsPrague:
		return gevmspec.Prague
	case r.IsCancun:
		return gevmspec.Cancun
	case r.IsShanghai:
		return gevmspec.Shanghai
	case r.IsLondon:
		return gevmspec.London
	case r.IsBerlin:
		return gevmspec.Berlin
	case r.IsIstanbul:
		return gevmspec.Istanbul
	case r.IsPetersburg:
		return gevmspec.Petersburg
	case r.IsConstantinople:
		return gevmspec.Constantinople
	case r.IsByzantium:
		return gevmspec.Byzantium
	case r.IsSpuriousDragon:
		return gevmspec.SpuriousDragon
	case r.IsTangerineWhistle:
		return gevmspec.Tangerine
	case r.IsHomestead:
		return gevmspec.Homestead
	default:
		return gevmspec.Frontier
	}
}

func toGevmAddress(addr accounts.Address) gevmtypes.Address {
	return gevmtypes.Address(addr.Value())
}

func toErigonAddress(addr gevmtypes.Address) accounts.Address {
	return accounts.InternAddress(common.Address(addr))
}

func toGevmHash(h common.Hash) gevmtypes.B256 {
	return gevmtypes.B256(h)
}
