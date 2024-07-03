package exec3

import (
	"fmt"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"

	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/transactions"
)

type GenericTracer interface {
	vm.EVMLogger
	SetTransaction(tx types.Transaction)
	Found() bool
}

type Resetable interface {
	Reset()
}

type TraceWorker struct {
	stateReader  *state.HistoryReaderV3
	engine       consensus.EngineReader
	headerReader services.HeaderReader
	tx           kv.Getter
	chainConfig  *chain.Config
	tracer       GenericTracer
	ibs          *state.IntraBlockState
	evm          *vm.EVM

	// calculated by .changeBlock()
	blockHash common.Hash
	blockNum  uint64
	header    *types.Header
	blockCtx  *evmtypes.BlockContext
	rules     *chain.Rules
	signer    *types.Signer
	vmConfig  *vm.Config
}

func NewTraceWorker(tx kv.TemporalTx, cc *chain.Config, engine consensus.EngineReader, br services.HeaderReader, tracer GenericTracer) *TraceWorker {
	stateReader := state.NewHistoryReaderV3()
	stateReader.SetTx(tx)

	ie := &TraceWorker{
		tx:           tx,
		engine:       engine,
		chainConfig:  cc,
		headerReader: br,
		stateReader:  stateReader,
		tracer:       tracer,
		evm:          vm.NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, nil, cc, vm.Config{}),
		vmConfig:     &vm.Config{},
		ibs:          state.New(stateReader),
	}
	if tracer != nil {
		ie.vmConfig = &vm.Config{Debug: true, Tracer: tracer}
	}
	return ie
}

func (e *TraceWorker) ChangeBlock(header *types.Header) {
	e.blockNum = header.Number.Uint64()
	blockCtx := transactions.NewEVMBlockContext(e.engine, header, true /* requireCanonical */, e.tx, e.headerReader, e.evm.ChainConfig())
	e.blockCtx = &blockCtx
	e.blockHash = header.Hash()
	e.header = header
	e.rules = e.chainConfig.Rules(e.blockNum, header.Time)
	e.signer = types.MakeSigner(e.chainConfig, e.blockNum, header.Time)
	e.vmConfig.SkipAnalysis = core.SkipAnalysis(e.chainConfig, e.blockNum)
}

func (e *TraceWorker) GetLogs(txIdx int, txn types.Transaction) types.Logs {
	return e.ibs.GetLogs(txn.Hash())
}

func (e *TraceWorker) ExecTxn(txNum uint64, txIndex int, txn types.Transaction) (*evmtypes.ExecutionResult, error) {
	e.stateReader.SetTxNum(txNum)
	txHash := txn.Hash()
	e.ibs.Reset()
	e.ibs.SetTxContext(txHash, e.blockHash, txIndex)
	gp := new(core.GasPool).AddGas(txn.GetGas()).AddBlobGas(txn.GetBlobGas())
	msg, err := txn.AsMessage(*e.signer, e.header.BaseFee, e.rules)
	if err != nil {
		return nil, err
	}
	e.evm.ResetBetweenBlocks(*e.blockCtx, core.NewEVMTxContext(msg), e.ibs, *e.vmConfig, e.rules)
	if msg.FeeCap().IsZero() {
		// Only zero-gas transactions may be service ones
		syscall := func(contract common.Address, data []byte) ([]byte, error) {
			return core.SysCallContract(contract, data, e.chainConfig, e.ibs, e.header, e.engine, true /* constCall */)
		}
		msg.SetIsFree(e.engine.IsServiceTransaction(msg.From(), syscall))
	}
	res, err := core.ApplyMessage(e.evm, msg, gp, true /* refunds */, false /* gasBailout */)
	if err != nil {
		return nil, fmt.Errorf("%w: blockNum=%d, txNum=%d, %s", err, e.blockNum, txNum, e.ibs.Error())
	}
	if e.vmConfig.Tracer != nil {
		if e.tracer.Found() {
			e.tracer.SetTransaction(txn)
		}
	}
	return res, nil
}
