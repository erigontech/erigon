package gevm

import (
	"errors"
	"strings"

	gevmhost "github.com/Giulio2002/gevm/host"
	gevmspec "github.com/Giulio2002/gevm/spec"
	gevmstate "github.com/Giulio2002/gevm/state"
	gevmtypes "github.com/Giulio2002/gevm/types"
	gevmvm "github.com/Giulio2002/gevm/vm"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

// Instance owns the GEVM engine created for an Erigon EVM environment.
type Instance struct {
	EVM         *gevmhost.Evm
	reader      state.StateReader
	writer      state.StateWriter
	isAmsterdam bool
	stateClear  bool
}

// New constructs the external GEVM engine for the Erigon EVM context.
func New(blockCtx evmtypes.BlockContext, txCtx evmtypes.TxContext, reader state.StateReader, chainConfig *chain.Config, rules *chain.Rules, extraEips []int) *Instance {
	block := gevmhost.BlockEnv{
		Beneficiary:      toGevmAddress(blockCtx.Coinbase),
		Timestamp:        gevmtypes.U256From(blockCtx.Time),
		Number:           gevmtypes.U256From(blockCtx.BlockNumber),
		Difficulty:       toGevmUint256(blockCtx.Difficulty),
		GasLimit:         gevmtypes.U256From(blockCtx.GasLimit),
		BaseFee:          toGevmUint256(blockCtx.BaseFee),
		BlobGasPrice:     toGevmUint256(blockCtx.BlobBaseFee),
		SlotNum:          gevmtypes.U256From(blockCtx.SlotNumber),
		CostPerStateByte: blockCtx.CostPerStateByte,
	}
	if blockCtx.PrevRanDao != nil {
		prevRandao := gevmtypes.U256FromBytes32(*(*[32]byte)(blockCtx.PrevRanDao))
		block.Prevrandao = &prevRandao
	}
	cfg := gevmhost.CfgEnv{}
	if rules != nil && rules.ChainID != nil {
		cfg.ChainId = gevmtypes.U256FromBig(rules.ChainID)
	} else if chainConfig != nil && chainConfig.ChainID != nil {
		cfg.ChainId = gevmtypes.U256FromBig(chainConfig.ChainID)
	}
	evm := gevmhost.NewEvm(database{reader: reader, getHash: blockCtx.GetHash, blockNumber: blockCtx.BlockNumber}, forkID(rules, extraEips), block, cfg)
	evm.TxEnv = gevmhost.TxEnv{
		Caller:            toGevmAddress(txCtx.Origin),
		EffectiveGasPrice: toGevmUint256(txCtx.GasPrice),
	}
	return &Instance{
		EVM:         evm,
		reader:      reader,
		isAmsterdam: rules != nil && rules.IsAmsterdam,
		stateClear:  rules != nil && rules.IsSpuriousDragon,
	}
}

// Release returns the wrapped GEVM engine to its pools.
func (g *Instance) Release() {
	if g != nil && g.EVM != nil {
		g.EVM.ReleaseEvm()
	}
}

func (g *Instance) SetStateWriter(writer state.StateWriter) {
	if g != nil {
		g.writer = writer
	}
}

func (g *Instance) ResetTx(txCtx evmtypes.TxContext) {
	if g == nil || g.EVM == nil {
		return
	}
	g.EVM.TxEnv = gevmhost.TxEnv{
		Caller:            toGevmAddress(txCtx.Origin),
		EffectiveGasPrice: toGevmUint256(txCtx.GasPrice),
	}
}

// Transact executes a full transaction through GEVM and flushes GEVM's journal
// to the Erigon state writer without routing through IntraBlockState.
func (g *Instance) Transact(tx *gevmhost.Transaction) (gevmhost.ExecutionResult, error) {
	if g == nil || g.EVM == nil {
		return gevmhost.ExecutionResult{}, errors.New("gevm adapter is not initialized")
	}
	result := g.EVM.Transact(tx)
	if result.ValidationError {
		g.EVM.Journal.DiscardTx()
		return result, nil
	}
	if tx.Kind == gevmhost.TxKindCall {
		g.TouchAccount(toErigonAddress(tx.To))
	}
	g.TouchAccount(toErigonAddress(g.EVM.Block.Beneficiary))
	result.StateGasUsed = g.amsterdamExecutionStateGas(tx, result.CreatedAddr)
	if result.StateGasConsumed > result.StateGasUsed {
		result.StateGasUsed = result.StateGasConsumed
	}
	if err := g.applyChanges(); err != nil {
		return result, err
	}
	g.EVM.Journal.CommitTx()
	return result, nil
}

func (g *Instance) amsterdamExecutionStateGas(tx *gevmhost.Transaction, topLevelCreated *gevmtypes.Address) uint64 {
	if !g.isAmsterdam || g == nil || g.EVM == nil || g.EVM.Block.CostPerStateByte == 0 {
		return 0
	}
	var stateGas uint64
	for addr, acc := range g.EVM.Journal.State {
		if acc.IsCreated() && isDelegationCode(acc.Info.Code) {
			continue
		}
		if acc.IsCreated() && !(tx != nil && tx.Kind == gevmhost.TxKindCreate && topLevelCreated != nil && addr == *topLevelCreated) {
			stateGas += 112 * g.EVM.Block.CostPerStateByte
		}
		if acc.Info.CodeHash != acc.OriginalInfo.CodeHash && len(acc.Info.Code) > 0 && !isDelegationCode(acc.Info.Code) {
			stateGas += uint64(len(acc.Info.Code)) * g.EVM.Block.CostPerStateByte
		}
		for _, slot := range acc.Storage {
			if slot == nil || !slot.IsChanged() || slot.OriginalValue != gevmtypes.U256Zero || slot.PresentValue == gevmtypes.U256Zero {
				continue
			}
			stateGas += 32 * g.EVM.Block.CostPerStateByte
		}
	}
	stateGas += g.amsterdamTransferStateGas(tx)
	return stateGas
}

func isDelegationCode(code []byte) bool {
	return len(code) == 23 && code[0] == 0xef && code[1] == 0x01 && code[2] == 0x00
}

func (g *Instance) amsterdamTransferStateGas(tx *gevmhost.Transaction) uint64 {
	if tx == nil {
		return 0
	}
	var stateGas uint64
	counted := make(map[gevmtypes.Address]struct{})
	for _, entry := range g.EVM.Journal.Entries {
		if entry.Kind != gevmstate.JournalBalanceTransfer || entry.Address == entry.Target || entry.Balance == gevmtypes.U256Zero {
			continue
		}
		if tx.Kind == gevmhost.TxKindCall && entry.Address == tx.Caller && entry.Target == tx.To {
			continue
		}
		acc := g.EVM.Journal.State[entry.Target]
		if acc == nil || acc.IsCreated() || !acc.IsLoadedAsNotExisting() || acc.Info.IsEmpty() {
			continue
		}
		if isDelegationCode(acc.Info.Code) {
			continue
		}
		if _, ok := counted[entry.Target]; ok {
			continue
		}
		counted[entry.Target] = struct{}{}
		stateGas += 112 * g.EVM.Block.CostPerStateByte
	}
	return stateGas
}

func (g *Instance) TouchAccount(address accounts.Address) {
	if g == nil || g.EVM == nil {
		return
	}
	addr := toGevmAddress(address)
	if _, err := g.EVM.Journal.LoadAccount(addr); err != nil {
		return
	}
	g.EVM.Journal.Touch(addr)
}

// Call executes a CALL-family frame through GEVM.
func (g *Instance) Call(typ string, caller accounts.Address, callerAddress accounts.Address, addr accounts.Address, input []byte, gas mdgas.MdGas, value uint256.Int, readOnly bool) ([]byte, mdgas.MdGas, error) {
	if g == nil || g.EVM == nil {
		return nil, gas, errors.New("gevm adapter is not initialized")
	}
	scheme := gevmvm.CallSchemeCall
	target := toGevmAddress(addr)
	callValue := gevmvm.NewCallValueTransfer(toGevmUint256(value))
	switch typ {
	case "CALLCODE":
		scheme = gevmvm.CallSchemeCallCode
		target = toGevmAddress(caller)
	case "DELEGATECALL":
		scheme = gevmvm.CallSchemeDelegateCall
		callValue = gevmvm.NewCallValueApparent(toGevmUint256(value))
		target = toGevmAddress(caller)
	case "STATICCALL":
		scheme = gevmvm.CallSchemeStaticCall
		readOnly = true
	}
	outcome := g.executeFrame(gevmvm.NewFrameInputCall(gevmvm.CallInputs{
		Input:              gevmtypes.Bytes(input),
		ReturnMemoryOffset: gevmvm.MemoryRange{},
		GasLimit:           gas.Regular,
		StateGasLimit:      gas.State,
		BytecodeAddress:    toGevmAddress(addr),
		TargetAddress:      target,
		Caller:             toGevmAddress(callerAddress),
		Value:              callValue,
		Scheme:             scheme,
		IsStatic:           readOnly,
	}))
	if outcome.Kind != gevmvm.FrameResultCall {
		return nil, gas, errors.New("gevm returned non-call frame result")
	}
	return g.finishCall(outcome.Call.Result, gas)
}

func (g *Instance) RunCode(caller accounts.Address, addr accounts.Address, code []byte, input []byte, gas mdgas.MdGas, value uint256.Int, readOnly bool) ([]byte, mdgas.MdGas, error) {
	if g == nil || g.EVM == nil {
		return nil, gas, errors.New("gevm adapter is not initialized")
	}
	target := toGevmAddress(addr)
	if _, err := g.EVM.Journal.LoadAccount(target); err != nil {
		return nil, mdgas.MdGas{}, err
	}
	g.EVM.Journal.SetCodeWithHash(target, gevmtypes.Bytes(code), gevmtypes.Keccak256(code))
	outcome := g.executeFrame(gevmvm.NewFrameInputCall(gevmvm.CallInputs{
		Input:              gevmtypes.Bytes(input),
		ReturnMemoryOffset: gevmvm.MemoryRange{},
		GasLimit:           gas.Regular,
		StateGasLimit:      gas.State,
		BytecodeAddress:    target,
		TargetAddress:      target,
		Caller:             toGevmAddress(caller),
		Value:              gevmvm.NewCallValueTransfer(toGevmUint256(value)),
		Scheme:             gevmvm.CallSchemeCall,
		IsStatic:           readOnly,
	}))
	if outcome.Kind != gevmvm.FrameResultCall {
		return nil, gas, errors.New("gevm returned non-call frame result")
	}
	return g.finishCall(outcome.Call.Result, gas)
}

// Create executes a CREATE/CREATE2 frame through GEVM.
func (g *Instance) Create(caller accounts.Address, code []byte, gas mdgas.MdGas, value uint256.Int, salt *uint256.Int) ([]byte, accounts.Address, mdgas.MdGas, error) {
	if g == nil || g.EVM == nil {
		return nil, accounts.NilAddress, gas, errors.New("gevm adapter is not initialized")
	}
	scheme := gevmvm.NewCreateSchemeCreate()
	if salt != nil {
		scheme = gevmvm.NewCreateSchemeCreate2(toGevmUint256(*salt))
	}
	outcome := g.executeFrame(gevmvm.NewFrameInputCreate(gevmvm.CreateInputs{
		Caller:        toGevmAddress(caller),
		Scheme:        scheme,
		Value:         toGevmUint256(value),
		InitCode:      gevmtypes.Bytes(code),
		GasLimit:      gas.Regular,
		StateGasLimit: gas.State,
	}))
	if outcome.Kind != gevmvm.FrameResultCreate {
		return nil, accounts.NilAddress, gas, errors.New("gevm returned non-create frame result")
	}
	ret, left, err := g.finishCall(outcome.Create.Result, gas)
	if outcome.Create.Address == nil {
		return ret, accounts.NilAddress, left, err
	}
	return ret, toErigonAddress(*outcome.Create.Address), left, err
}

func (g *Instance) executeFrame(input gevmvm.FrameInput) gevmvm.FrameResult {
	host := gevmhost.NewEvmHost(g.EVM.Journal, &g.EVM.Block, g.EVM.TxEnv, &g.EVM.Cfg)
	rootMemory := gevmvm.NewMemory()
	handler := gevmhost.NewHandler(host, rootMemory)
	handler.Runner = gevmvm.DefaultRunner{}
	handler.ReturnAlloc = &g.EVM.ReturnAlloc
	if g.EVM.JumpTableCache == nil {
		g.EVM.JumpTableCache = make(map[gevmtypes.B256][]byte)
	}
	handler.JumpTableCache = g.EVM.JumpTableCache
	return handler.ExecuteFrame(&input, 0, rootMemory)
}

func (g *Instance) finishCall(result gevmvm.InterpreterResult, gas mdgas.MdGas) ([]byte, mdgas.MdGas, error) {
	gas.Regular = result.Gas.Remaining()
	gas.State = result.Gas.StateRemaining()
	switch {
	case result.Result.IsOk():
		if err := g.applyChanges(); err != nil {
			return result.Output, gas, err
		}
		g.EVM.Journal.CommitTx()
		return result.Output, gas, nil
	case result.Result.IsRevert():
		return result.Output, gas, errExecutionReverted{}
	default:
		gas.Regular = 0
		return result.Output, gas, result.Result
	}
}

func (g *Instance) applyChanges() error {
	if g.writer == nil || g.EVM == nil {
		return nil
	}
	for address, acc := range g.EVM.Journal.State {
		addr := toErigonAddress(address)
		original, err := g.originalErigonAccount(addr, acc.OriginalInfo)
		if err != nil {
			return err
		}
		if acc.IsSelfdestructed() {
			if err := g.writer.DeleteAccount(addr, original); err != nil {
				return err
			}
			continue
		}
		if acc.IsLoadedAsNotExistingNotTouched() {
			continue
		}
		account := toErigonAccount(acc.Info, acc.IsCreated(), original)
		if g.stateClear && acc.IsTouched() && account.Balance.IsZero() && account.Nonce == 0 && account.CodeHash.IsEmpty() {
			if err := g.writer.DeleteAccount(addr, original); err != nil {
				return err
			}
			continue
		}
		if acc.Info.CodeHash != acc.OriginalInfo.CodeHash && acc.Info.Code != nil {
			if err := g.writer.UpdateAccountCode(addr, account.GetIncarnation(), account.CodeHash, []byte(acc.Info.Code)); err != nil {
				return err
			}
		}
		if acc.IsCreated() {
			if err := g.writer.CreateContract(addr); err != nil {
				return err
			}
		}
		for key, slot := range acc.Storage {
			if slot == nil || !slot.IsChanged() {
				continue
			}
			storageKey := accounts.InternKey(common.Hash(key.ToBytes32()))
			if err := g.writer.WriteAccountStorage(addr, account.GetIncarnation(), storageKey, fromGevmUint256(slot.OriginalValue), fromGevmUint256(slot.PresentValue)); err != nil {
				return err
			}
		}
		if accountInfoChanged(acc.Info, acc.OriginalInfo) || acc.IsCreated() || (!g.stateClear && acc.IsTouched() && acc.IsLoadedAsNotExisting()) {
			if err := g.writer.UpdateAccountData(addr, original, &account); err != nil {
				return err
			}
		}
	}
	return nil
}

func (g *Instance) originalErigonAccount(addr accounts.Address, info gevmstate.AccountInfo) (*accounts.Account, error) {
	if g.reader != nil {
		account, err := g.reader.ReadAccountData(addr)
		if err != nil {
			return nil, err
		}
		if account != nil {
			return account, nil
		}
	}
	account := toErigonAccount(info, false, nil)
	return &account, nil
}

func accountInfoChanged(a, b gevmstate.AccountInfo) bool {
	return a.Balance != b.Balance || a.Nonce != b.Nonce || a.CodeHash != b.CodeHash
}

func forkID(rules *chain.Rules, extraEips []int) gevmspec.ForkID {
	fork := forkIDFromRules(rules)
	hasEIP2200 := false
	hasEIP2929 := false
	for _, eip := range extraEips {
		switch eip {
		case 2200:
			hasEIP2200 = true
		case 2929, 2930:
			hasEIP2929 = true
		}
	}
	if hasEIP2200 && !hasEIP2929 {
		fork = gevmspec.Istanbul
	}
	for _, eip := range extraEips {
		switch eip {
		case 1884:
			if fork < gevmspec.Istanbul {
				fork = gevmspec.Istanbul
			}
		case 2929, 2930:
			if fork < gevmspec.Berlin {
				fork = gevmspec.Berlin
			}
		case 3529:
			if fork < gevmspec.London {
				fork = gevmspec.London
			}
		case 3860, 3651, 3855:
			if fork < gevmspec.Shanghai {
				fork = gevmspec.Shanghai
			}
		}
	}
	return fork
}

func forkIDFromRules(rules *chain.Rules) gevmspec.ForkID {
	switch {
	case rules == nil:
		return gevmspec.LatestForkID
	case rules.IsAmsterdam:
		return gevmspec.Amsterdam
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

type database struct {
	reader  state.StateReader
	getHash func(uint64) (common.Hash, error)

	blockNumber uint64
}

var _ gevmstate.Database = database{}

func (db database) Basic(address gevmtypes.Address) (gevmstate.AccountInfo, bool, error) {
	addr := toErigonAddress(address)
	if db.reader == nil {
		return gevmstate.AccountInfo{}, false, nil
	}
	acc, err := db.reader.ReadAccountData(addr)
	if err != nil {
		return gevmstate.AccountInfo{}, false, err
	}
	if acc == nil {
		return gevmstate.AccountInfo{}, false, nil
	}
	var code []byte
	if !acc.CodeHash.IsEmpty() {
		code, err = db.reader.ReadAccountCode(addr)
		if err != nil {
			return gevmstate.AccountInfo{}, true, err
		}
	}
	return gevmstate.AccountInfo{
		Balance:  toGevmUint256(acc.Balance),
		Nonce:    acc.Nonce,
		CodeHash: toGevmB256(acc.CodeHash.Value()),
		Code:     gevmtypes.Bytes(code),
	}, true, nil
}

func (db database) CodeByHash(gevmtypes.B256) (gevmtypes.Bytes, error) {
	return nil, nil
}

func (db database) Storage(address gevmtypes.Address, index gevmtypes.Uint256) (gevmtypes.Uint256, error) {
	if db.reader == nil {
		return gevmtypes.Uint256{}, nil
	}
	value, _, err := db.reader.ReadAccountStorage(toErigonAddress(address), accounts.InternKey(common.Hash(index.ToBytes32())))
	if err != nil {
		return gevmtypes.Uint256{}, err
	}
	return toGevmUint256(value), nil
}

func (db database) HasStorage(address gevmtypes.Address) (bool, error) {
	if db.reader == nil {
		return false, nil
	}
	return db.reader.HasStorage(toErigonAddress(address))
}

func (db database) BlockHash(number uint64) (gevmtypes.B256, error) {
	if db.getHash == nil {
		return gevmtypes.B256{}, nil
	}
	if number >= db.blockNumber || db.blockNumber-number > 256 {
		return gevmtypes.B256{}, nil
	}
	hash, err := db.getHash(number)
	if err != nil {
		return gevmtypes.B256{}, err
	}
	return toGevmB256(hash), nil
}

func toGevmAddress(addr accounts.Address) gevmtypes.Address {
	return gevmtypes.Address(addr.Value())
}

func toErigonAddress(addr gevmtypes.Address) accounts.Address {
	return accounts.InternAddress(common.Address(addr))
}

func toGevmB256(hash common.Hash) gevmtypes.B256 {
	return gevmtypes.B256(hash)
}

func toGevmUint256(v uint256.Int) gevmtypes.Uint256 {
	return gevmtypes.U256FromBig(v.ToBig())
}

func fromGevmUint256(v gevmtypes.Uint256) uint256.Int {
	var out uint256.Int
	out.SetFromBig(v.ToBig())
	return out
}

func toErigonCodeHash(hash gevmtypes.B256) accounts.CodeHash {
	return accounts.InternCodeHash(common.Hash(hash))
}

func toErigonAccount(info gevmstate.AccountInfo, created bool, original *accounts.Account) accounts.Account {
	account := accounts.NewAccount()
	account.Balance = fromGevmUint256(info.Balance)
	account.Nonce = info.Nonce
	account.CodeHash = toErigonCodeHash(info.CodeHash)
	switch {
	case created && !account.CodeHash.IsEmpty():
		if original != nil && original.Incarnation > 0 {
			account.Incarnation = original.Incarnation + 1
			account.PrevIncarnation = original.Incarnation
		} else {
			account.Incarnation = state.FirstContractIncarnation
		}
	case original != nil:
		account.Incarnation = original.Incarnation
		account.PrevIncarnation = original.PrevIncarnation
	}
	return account
}

func toGevmLog(log *types.Log) gevmstate.Log {
	var topics [4]gevmtypes.B256
	for i := 0; i < len(log.Topics) && i < len(topics); i++ {
		topics[i] = gevmtypes.B256(log.Topics[i])
	}
	return gevmstate.Log{
		Address:   gevmtypes.Address(log.Address),
		Topics:    topics,
		NumTopics: uint8(len(log.Topics)),
		Data:      gevmtypes.Bytes(common.Copy(log.Data)),
	}
}

type errExecutionReverted struct{}

func (errExecutionReverted) Error() string { return "execution reverted" }

func IsExecutionReverted(err error) bool {
	_, ok := err.(errExecutionReverted)
	return ok
}

func IsOutOfGas(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "OOG") || strings.Contains(msg, "OutOfGas") || msg == "out of gas"
}
