package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/holiman/uint256"
	"github.com/jackc/pgx/v4"
	"github.com/ledgerwatch/erigon-lib/common"
	ericommon "github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/zkevm/encoding"
	"github.com/ledgerwatch/erigon/zkevm/hex"
	"github.com/ledgerwatch/erigon/zkevm/log"
	"github.com/ledgerwatch/erigon/zkevm/state/runtime"
	"github.com/ledgerwatch/erigon/zkevm/state/runtime/executor"
	"github.com/ledgerwatch/erigon/zkevm/state/runtime/executor/pb"
	"github.com/ledgerwatch/erigon/zkevm/state/runtime/fakevm"
	"github.com/ledgerwatch/erigon/zkevm/state/runtime/instrumentation"
	"github.com/ledgerwatch/erigon/zkevm/state/runtime/instrumentation/js"
	"github.com/ledgerwatch/erigon/zkevm/state/runtime/instrumentation/tracers"
)

const (
	// Size of the memory in bytes reserved by the zkEVM
	zkEVMReservedMemorySize int  = 128
	two                     uint = 2
)

// GetSender gets the sender from the transaction's signature
func GetSender(tx types.Transaction) (common.Address, error) {
	// TODO: fix the hardcoded chain config for the l2
	signer := types.MakeSigner(params.HermezTestnetChainConfig, 0)

	sender, err := signer.Sender(tx)
	if err != nil {
		return common.Address{}, err
	}
	return sender, nil
}

// RlpFieldsToLegacyTx parses the rlp fields slice into a type.LegacyTx
// in this specific order:
//
// required fields:
// [0] Nonce    uint64
// [1] GasPrice *big.Int
// [2] Gas      uint64
// [3] To       *common.Address
// [4] Value    *big.Int
// [5] Data     []byte
//
// optional fields:
// [6] V        *big.Int
// [7] R        *big.Int
// [8] S        *big.Int
func RlpFieldsToLegacyTx(fields [][]byte, v, r, s []byte) (tx *types.LegacyTx, err error) {
	const (
		fieldsSizeWithoutChainID = 6
		fieldsSizeWithChainID    = 7
	)

	if len(fields) < fieldsSizeWithoutChainID {
		return nil, types.ErrTxTypeNotSupported
	}

	nonce := big.NewInt(0).SetBytes(fields[0]).Uint64()

	gasPriceI := &uint256.Int{}
	gasPriceI.SetBytes(fields[1])

	gas := big.NewInt(0).SetBytes(fields[2]).Uint64()
	var to *common.Address

	if fields[3] != nil && len(fields[3]) != 0 {
		tmp := common.BytesToAddress(fields[3])
		to = &tmp
	}
	value := big.NewInt(0).SetBytes(fields[4])
	data := fields[5]

	txV := big.NewInt(0).SetBytes(v)
	if len(fields) >= fieldsSizeWithChainID {
		chainID := big.NewInt(0).SetBytes(fields[6])

		// a = chainId * 2
		// b = v - 27
		// c = a + 35
		// v = b + c
		//
		// same as:
		// v = v-27+chainId*2+35
		a := new(big.Int).Mul(chainID, big.NewInt(double))
		b := new(big.Int).Sub(new(big.Int).SetBytes(v), big.NewInt(ether155V))
		c := new(big.Int).Add(a, big.NewInt(etherPre155V))
		txV = new(big.Int).Add(b, c)
	}

	txVi := uint256.Int{}
	txVi.SetBytes(txV.Bytes())

	txRi := uint256.Int{}
	txRi.SetBytes(r)

	txSi := uint256.Int{}
	txSi.SetBytes(s)

	valueI := &uint256.Int{}
	valueI.SetBytes(value.Bytes())

	return &types.LegacyTx{
		types.CommonTx{
			Nonce: nonce,
			Gas:   gas,
			To:    to,
			Value: valueI,
			Data:  data,
			V:     txVi,
			R:     txRi,
			S:     txSi,
		},
		gasPriceI,
	}, nil
}

/*

// StoreTransactions is used by the sequencer to add processed transactions into
// an open batch. If the batch already has txs, the processedTxs must be a super
// set of the existing ones, preserving order.
func (s *State) StoreTransactions(ctx context.Context, batchNumber uint64, processedTxs []*ProcessTransactionResponse, dbTx pgx.Tx) error {
	if dbTx == nil {
		return ErrDBTxNil
	}

	// check existing txs vs parameter txs
	existingTxs, err := s.GetTxsHashesByBatchNumber(ctx, batchNumber, dbTx)
	if err != nil {
		return err
	}
	if err := CheckSupersetBatchTransactions(existingTxs, processedTxs); err != nil {
		return err
	}

	// Check if last batch is closed. Note that it's assumed that only the latest batch can be open
	isBatchClosed, err := s.PostgresStorage.IsBatchClosed(ctx, batchNumber, dbTx)
	if err != nil {
		return err
	}
	if isBatchClosed {
		return ErrBatchAlreadyClosed
	}

	processingContext, err := s.GetProcessingContext(ctx, batchNumber, dbTx)
	if err != nil {
		return err
	}

	firstTxToInsert := len(existingTxs)

	for i := firstTxToInsert; i < len(processedTxs); i++ {
		processedTx := processedTxs[i]
		// if the transaction has an intrinsic invalid tx error it means
		// the transaction has not changed the state, so we don't store it
		// and just move to the next
		if executor.IsIntrinsicError(executor.RomErrorCode(processedTx.RomError)) {
			continue
		}

		lastL2Block, err := s.GetLastL2Block(ctx, dbTx)
		if err != nil {
			return err
		}

		header := &types.Header{
			Number:     new(big.Int).SetUint64(lastL2Block.Number().Uint64() + 1),
			ParentHash: lastL2Block.Hash(),
			Coinbase:   processingContext.Coinbase,
			Root:       processedTx.StateRoot,
			GasUsed:    processedTx.GasUsed,
			GasLimit:   s.cfg.MaxCumulativeGasUsed,
			Time:       uint64(processingContext.Timestamp.Unix()),
		}
		transactions := []types.Transaction{processedTx.Tx}

		receipt := generateReceipt(header.Number, processedTx)
		receipts := []*types.Receipt{receipt}

		// Create block to be able to calculate its hash
		block := types.NewBlock(header, transactions, []*types.Header{}, receipts, nil)
		block.ReceivedAt = processingContext.Timestamp

		receipt.BlockHash = block.Hash()

		// Store L2 block and its transaction
		if err := s.AddL2Block(ctx, batchNumber, block, receipts, dbTx); err != nil {
			return err
		}
	}
	return nil
}
*/

// DebugTransaction re-executes a tx to generate its trace
func (s *State) DebugTransaction(ctx context.Context, transactionHash common.Hash, traceConfig TraceConfig, dbTx pgx.Tx) (*runtime.ExecutionResult, error) {
	if s.executorClient == nil {
		return nil, ErrExecutorNil
	}

	result := new(runtime.ExecutionResult)

	// gets the transaction
	tx, err := s.GetTransactionByHash(ctx, transactionHash, dbTx)
	if err != nil {
		return nil, err
	}

	// gets the tx receipt
	receipt, err := s.GetTransactionReceipt(ctx, transactionHash, dbTx)
	if err != nil {
		return nil, err
	}

	// gets the l2 block including the transaction
	block, err := s.GetL2BlockByNumber(ctx, receipt.BlockNumber.Uint64(), dbTx)
	if err != nil {
		return nil, err
	}

	// get the previous L2 Block
	previousBlockNumber := uint64(0)
	if receipt.BlockNumber.Uint64() > 0 {
		previousBlockNumber = receipt.BlockNumber.Uint64() - 1
	}
	previousBlock, err := s.GetL2BlockByNumber(ctx, previousBlockNumber, dbTx)
	if err != nil {
		return nil, err
	}

	// generate batch l2 data for the transaction
	batchL2Data, err := EncodeTransactions([]types.Transaction{tx})
	if err != nil {
		return nil, err
	}

	// gets batch that including the l2 block
	batch, err := s.GetBatchByL2BlockNumber(ctx, block.NumberU64(), dbTx)
	if err != nil {
		return nil, err
	}

	// gets batch that including the previous l2 block
	previousBatch, err := s.GetBatchByL2BlockNumber(ctx, previousBlock.NumberU64(), dbTx)
	if err != nil {
		return nil, err
	}

	forkId := s.GetForkIDByBatchNumber(batch.BatchNumber)

	// Create Batch
	traceConfigRequest := &pb.TraceConfig{
		TxHashToGenerateCallTrace:    transactionHash.Bytes(),
		TxHashToGenerateExecuteTrace: transactionHash.Bytes(),
	}

	if traceConfig.DisableStorage {
		traceConfigRequest.DisableStorage = cTrue
	}
	if traceConfig.DisableStack {
		traceConfigRequest.DisableStack = cTrue
	}
	if traceConfig.EnableMemory {
		traceConfigRequest.EnableMemory = cTrue
	}
	if traceConfig.EnableReturnData {
		traceConfigRequest.EnableReturnData = cTrue
	}

	processBatchRequest := &pb.ProcessBatchRequest{
		OldBatchNum:     batch.BatchNumber - 1,
		OldStateRoot:    previousBlock.Root().Bytes(),
		OldAccInputHash: previousBatch.AccInputHash.Bytes(),

		BatchL2Data:      batchL2Data,
		GlobalExitRoot:   batch.GlobalExitRoot.Bytes(),
		EthTimestamp:     uint64(batch.Timestamp.Unix()),
		Coinbase:         batch.Coinbase.String(),
		UpdateMerkleTree: cFalse,
		ChainId:          s.cfg.ChainID,
		ForkId:           forkId,
		TraceConfig:      traceConfigRequest,
	}

	// Send Batch to the Executor
	startTime := time.Now()
	processBatchResponse, err := s.executorClient.ProcessBatch(ctx, processBatchRequest)
	if err != nil {
		return nil, err
	} else if processBatchResponse.Error != executor.EXECUTOR_ERROR_NO_ERROR {
		err = executor.ExecutorErr(processBatchResponse.Error)
		//s.eventLog.LogExecutorError(ctx, processBatchResponse.Error, processBatchRequest)
		return nil, err
	}
	endTime := time.Now()

	// //save process batch response file
	// b, err := json.Marshal(processBatchResponse)
	// if err != nil {
	// 	return nil, err
	// }
	// filePath := "./processBatchResponse.json"
	// err = os.WriteFile(filePath, b, 0644)
	// if err != nil {
	// 	return nil, err
	// }

	txs, _, _, err := DecodeTxs(batchL2Data, 5)
	if err != nil && !errors.Is(err, ErrInvalidData) {
		return nil, err
	}

	for _, tx := range txs {
		log.Debugf(tx.Hash().String())
	}

	convertedResponse, err := s.convertToProcessBatchResponse(txs, processBatchResponse)
	if err != nil {
		return nil, err
	}

	// Sanity check
	response := convertedResponse.Responses[0]
	log.Debugf(response.TxHash.String())
	if response.TxHash != transactionHash {
		return nil, fmt.Errorf("tx hash not found in executor response")
	}

	result.CreateAddress = response.CreateAddress
	result.GasLeft = response.GasLeft
	result.GasUsed = response.GasUsed
	result.ReturnValue = response.ReturnValue
	result.StateRoot = response.StateRoot.Bytes()
	result.StructLogs = response.ExecutionTrace

	if traceConfig.Tracer == nil || *traceConfig.Tracer == "" {
		return result, nil
	}

	// Parse the executor-like trace using the FakeEVM
	jsTracer, err := js.NewJsTracer(*traceConfig.Tracer, new(tracers.Context))
	if err != nil {
		log.Errorf("debug transaction: failed to create jsTracer, err: %v", err)
		return nil, fmt.Errorf("failed to create jsTracer, err: %v", err)
	}

	context := instrumentation.Context{}

	// Fill trace context
	if tx.GetTo() == nil {
		context.Type = "CREATE"
		context.To = result.CreateAddress.Hex()
	} else {
		context.Type = "CALL"
		context.To = tx.GetTo().Hex()
	}

	senderAddress, err := GetSender(tx)
	if err != nil {
		return nil, err
	}

	context.From = senderAddress.String()
	context.Input = "0x" + hex.EncodeToString(tx.GetData())
	context.Gas = strconv.FormatUint(tx.GetGas(), encoding.Base10)
	context.Value = tx.GetValue().String()
	context.Output = "0x" + hex.EncodeToString(result.ReturnValue)
	context.GasPrice = tx.GetPrice().String()
	context.OldStateRoot = batch.StateRoot.String()
	context.Time = uint64(endTime.Sub(startTime))
	context.GasUsed = strconv.FormatUint(result.GasUsed, encoding.Base10)

	result.ExecutorTrace.Context = context

	gasPrice, ok := new(big.Int).SetString(context.GasPrice, encoding.Base10)
	if !ok {
		log.Errorf("debug transaction: failed to parse gasPrice")
		return nil, fmt.Errorf("failed to parse gasPrice")
	}

	gasPriceI := &uint256.Int{}
	gasPriceI.SetBytes(gasPrice.Bytes())

	env := fakevm.NewFakeEVM(evmtypes.BlockContext{BlockNumber: 1}, evmtypes.TxContext{GasPrice: gasPriceI}, params.TestChainConfig, fakevm.Config{Debug: true, Tracer: jsTracer})
	fakeDB := &FakeDB{State: s, stateRoot: batch.StateRoot.Bytes()}
	env.SetStateDB(fakeDB)

	traceResult, err := s.ParseTheTraceUsingTheTracer(env, result.ExecutorTrace, jsTracer)
	if err != nil {
		log.Errorf("debug transaction: failed parse the trace using the tracer: %v", err)
		return nil, fmt.Errorf("failed parse the trace using the tracer: %v", err)
	}

	result.ExecutorTraceResult = traceResult

	return result, nil
}

// ParseTheTraceUsingTheTracer parses the given trace with the given tracer.
func (s *State) ParseTheTraceUsingTheTracer(env *fakevm.FakeEVM, trace instrumentation.ExecutorTrace, jsTracer tracers.Tracer) (json.RawMessage, error) {
	var previousDepth int
	var previousOpcode string
	var stateRoot []byte

	contextGas, ok := new(big.Int).SetString(trace.Context.Gas, encoding.Base10)
	if !ok {
		log.Debugf("error while parsing contextGas")
		return nil, ErrParsingExecutorTrace
	}
	value, ok := new(big.Int).SetString(trace.Context.Value, encoding.Base10)
	if !ok {
		log.Debugf("error while parsing value")
		return nil, ErrParsingExecutorTrace
	}

	jsTracer.CaptureTxStart(contextGas.Uint64())
	jsTracer.CaptureStart(env, common.HexToAddress(trace.Context.From), common.HexToAddress(trace.Context.To), trace.Context.Type == "CREATE", ericommon.Hex2Bytes(strings.TrimLeft(trace.Context.Input, "0x")), contextGas.Uint64(), value)

	stack := fakevm.Newstack()
	memory := fakevm.NewMemory()

	bigStateRoot, ok := new(big.Int).SetString(trace.Context.OldStateRoot, 0)
	if !ok {
		log.Debugf("error while parsing context oldStateRoot")
		return nil, ErrParsingExecutorTrace
	}
	stateRoot = bigStateRoot.Bytes()
	env.StateDB.SetStateRoot(stateRoot)

	for i, step := range trace.Steps {
		gas, ok := new(big.Int).SetString(step.Gas, encoding.Base10)
		if !ok {
			log.Debugf("error while parsing step gas")
			return nil, ErrParsingExecutorTrace
		}

		gasCost, ok := new(big.Int).SetString(step.GasCost, encoding.Base10)
		if !ok {
			log.Debugf("error while parsing step gasCost")
			return nil, ErrParsingExecutorTrace
		}

		value, ok := new(big.Int).SetString(step.Contract.Value, encoding.Base10)
		if !ok {
			log.Debugf("error while parsing step value")
			return nil, ErrParsingExecutorTrace
		}

		op, ok := new(big.Int).SetString(step.Op, 0)
		if !ok {
			log.Debugf("error while parsing step op")
			return nil, ErrParsingExecutorTrace
		}

		valueI := &uint256.Int{}
		valueI.SetBytes(value.Bytes())

		scope := &fakevm.ScopeContext{
			Contract: vm.NewContract(fakevm.NewAccount(common.HexToAddress(step.Contract.Caller)), fakevm.NewAccount(common.HexToAddress(step.Contract.Address)), valueI, gas.Uint64(), false),
			Memory:   memory,
			Stack:    stack,
		}

		codeAddr := common.HexToAddress(step.Contract.Address)
		scope.Contract.CodeAddr = &codeAddr

		opcode := vm.OpCode(op.Uint64()).String()

		if previousOpcode == "CALL" && step.Pc != 0 {
			jsTracer.CaptureExit(ericommon.Hex2Bytes(step.ReturnData), gasCost.Uint64(), fmt.Errorf(step.Error))
		}

		if opcode != "CALL" || trace.Steps[i+1].Pc == 0 {
			if step.Error != "" {
				err := fmt.Errorf(step.Error)
				jsTracer.CaptureFault(step.Pc, vm.OpCode(op.Uint64()), gas.Uint64(), gasCost.Uint64(), scope, step.Depth, err)
			} else {
				jsTracer.CaptureState(step.Pc, vm.OpCode(op.Uint64()), gas.Uint64(), gasCost.Uint64(), scope, ericommon.Hex2Bytes(strings.TrimLeft(step.ReturnData, "0x")), step.Depth, nil)
			}
		}

		if opcode == "CREATE" || opcode == "CREATE2" || opcode == "CALL" || opcode == "CALLCODE" || opcode == "DELEGATECALL" || opcode == "STATICCALL" || opcode == "SELFDESTRUCT" {
			jsTracer.CaptureEnter(vm.OpCode(op.Uint64()), common.HexToAddress(step.Contract.Caller), common.HexToAddress(step.Contract.Address), ericommon.Hex2Bytes(strings.TrimLeft(step.Contract.Input, "0x")), gas.Uint64(), value)
			if step.OpCode == "SELFDESTRUCT" {
				jsTracer.CaptureExit(ericommon.Hex2Bytes(step.ReturnData), gasCost.Uint64(), fmt.Errorf(step.Error))
			}
		}

		// Set Memory
		if len(step.Memory) > 0 {
			memory.Resize(uint64(fakevm.MemoryItemSize*len(step.Memory) + zkEVMReservedMemorySize))
			for offset, memoryContent := range step.Memory {
				memory.Set(uint64((offset*fakevm.MemoryItemSize)+zkEVMReservedMemorySize), uint64(fakevm.MemoryItemSize), ericommon.Hex2Bytes(memoryContent))
			}
		} else {
			memory = fakevm.NewMemory()
		}

		// Set Stack
		stack = fakevm.Newstack()
		for _, stackContent := range step.Stack {
			valueBigInt, ok := new(big.Int).SetString(stackContent, 0)
			if !ok {
				log.Debugf("error while parsing stack valueBigInt")
				return nil, ErrParsingExecutorTrace
			}
			value, _ := uint256.FromBig(valueBigInt)
			stack.Push(value)
		}

		// Returning from a call or create
		if previousDepth > step.Depth {
			jsTracer.CaptureExit(ericommon.Hex2Bytes(step.ReturnData), gasCost.Uint64(), fmt.Errorf(step.Error))
		}

		// Set StateRoot
		bigStateRoot, ok := new(big.Int).SetString(step.StateRoot, 0)
		if !ok {
			log.Debugf("error while parsing step stateRoot")
			return nil, ErrParsingExecutorTrace
		}

		stateRoot = bigStateRoot.Bytes()
		env.StateDB.SetStateRoot(stateRoot)
		previousDepth = step.Depth
		previousOpcode = step.OpCode
	}

	gasUsed, ok := new(big.Int).SetString(trace.Context.GasUsed, encoding.Base10)
	if !ok {
		log.Debugf("error while parsing gasUsed")
		return nil, ErrParsingExecutorTrace
	}

	jsTracer.CaptureTxEnd(gasUsed.Uint64())
	jsTracer.CaptureEnd(ericommon.Hex2Bytes(trace.Context.Output), gasUsed.Uint64(), time.Duration(trace.Context.Time), nil)

	return jsTracer.GetResult()
}

// PreProcessTransaction processes the transaction in order to calculate its zkCounters before adding it to the pool
func (s *State) PreProcessTransaction(ctx context.Context, tx types.Transaction, dbTx pgx.Tx) (*ProcessBatchResponse, error) {
	sender, err := GetSender(tx)
	if err != nil {
		return nil, err
	}

	response, err := s.internalProcessUnsignedTransaction(ctx, tx, sender, nil, false, dbTx)
	if err != nil {
		return nil, err
	}

	return response, nil
}

// ProcessUnsignedTransaction processes the given unsigned transaction.
func (s *State) ProcessUnsignedTransaction(ctx context.Context, tx types.Transaction, senderAddress common.Address, l2BlockNumber *uint64, noZKEVMCounters bool, dbTx pgx.Tx) (*runtime.ExecutionResult, error) {
	result := new(runtime.ExecutionResult)
	response, err := s.internalProcessUnsignedTransaction(ctx, tx, senderAddress, l2BlockNumber, noZKEVMCounters, dbTx)
	if err != nil {
		return nil, err
	}

	r := response.Responses[0]
	result.ReturnValue = r.ReturnValue
	result.GasLeft = r.GasLeft
	result.GasUsed = r.GasUsed
	result.CreateAddress = r.CreateAddress
	result.StateRoot = r.StateRoot.Bytes()

	if errors.Is(r.RomError, runtime.ErrExecutionReverted) {
		result.Err = constructErrorFromRevert(r.RomError, r.ReturnValue)
	} else {
		result.Err = r.RomError
	}

	return result, nil
}

// ProcessUnsignedTransaction processes the given unsigned transaction.
func (s *State) internalProcessUnsignedTransaction(ctx context.Context, tx types.Transaction, senderAddress common.Address, l2BlockNumber *uint64, noZKEVMCounters bool, dbTx pgx.Tx) (*ProcessBatchResponse, error) {
	if s.executorClient == nil {
		return nil, ErrExecutorNil
	}
	if s.tree == nil {
		return nil, ErrStateTreeNil
	}
	lastBatches, l2BlockStateRoot, err := s.PostgresStorage.GetLastNBatchesByL2BlockNumber(ctx, l2BlockNumber, two, dbTx)
	if err != nil {
		return nil, err
	}

	stateRoot := l2BlockStateRoot
	if l2BlockNumber != nil {
		l2Block, err := s.GetL2BlockByNumber(ctx, *l2BlockNumber, dbTx)
		if err != nil {
			return nil, err
		}
		stateRoot = l2Block.Root()
	}

	loadedNonce, err := s.tree.GetNonce(ctx, senderAddress, stateRoot.Bytes())
	if err != nil {
		return nil, err
	}
	nonce := loadedNonce.Uint64()

	// Get latest batch from the database to get globalExitRoot and Timestamp
	lastBatch := lastBatches[0]

	// Get batch before latest to get state root and local exit root
	previousBatch := lastBatches[0]
	if len(lastBatches) > 1 {
		previousBatch = lastBatches[1]
	}

	timestamp := uint64(lastBatch.Timestamp.Unix())

	if l2BlockNumber != nil {
		latestL2BlockNumber, err := s.PostgresStorage.GetLastL2BlockNumber(ctx, dbTx)
		if err != nil {
			return nil, err
		}

		if *l2BlockNumber == latestL2BlockNumber {
			timestamp = uint64(time.Now().Unix())
		}
	}

	batchL2Data, err := EncodeUnsignedTransaction(tx, s.cfg.ChainID, &nonce)
	if err != nil {
		log.Errorf("error encoding unsigned transaction ", err)
		return nil, err
	}

	forkID := GetForkIDByBatchNumber(s.cfg.ForkIDIntervals, lastBatch.BatchNumber)
	// Create Batch
	processBatchRequest := &pb.ProcessBatchRequest{
		OldBatchNum:      lastBatch.BatchNumber,
		BatchL2Data:      batchL2Data,
		From:             senderAddress.String(),
		OldStateRoot:     stateRoot.Bytes(),
		GlobalExitRoot:   lastBatch.GlobalExitRoot.Bytes(),
		OldAccInputHash:  previousBatch.AccInputHash.Bytes(),
		EthTimestamp:     timestamp,
		Coinbase:         lastBatch.Coinbase.String(),
		UpdateMerkleTree: cFalse,
		ChainId:          s.cfg.ChainID,
		ForkId:           forkID,
	}

	if noZKEVMCounters {
		processBatchRequest.NoCounters = cTrue
	}

	log.Debugf("internalProcessUnsignedTransaction[processBatchRequest.OldBatchNum]: %v", processBatchRequest.OldBatchNum)
	log.Debugf("internalProcessUnsignedTransaction[processBatchRequest.From]: %v", processBatchRequest.From)
	log.Debugf("internalProcessUnsignedTransaction[processBatchRequest.OldStateRoot]: %v", hex.EncodeToHex(processBatchRequest.OldStateRoot))
	log.Debugf("internalProcessUnsignedTransaction[processBatchRequest.globalExitRoot]: %v", hex.EncodeToHex(processBatchRequest.GlobalExitRoot))
	log.Debugf("internalProcessUnsignedTransaction[processBatchRequest.OldAccInputHash]: %v", hex.EncodeToHex(processBatchRequest.OldAccInputHash))
	log.Debugf("internalProcessUnsignedTransaction[processBatchRequest.EthTimestamp]: %v", processBatchRequest.EthTimestamp)
	log.Debugf("internalProcessUnsignedTransaction[processBatchRequest.Coinbase]: %v", processBatchRequest.Coinbase)
	log.Debugf("internalProcessUnsignedTransaction[processBatchRequest.UpdateMerkleTree]: %v", processBatchRequest.UpdateMerkleTree)
	log.Debugf("internalProcessUnsignedTransaction[processBatchRequest.ChainId]: %v", processBatchRequest.ChainId)
	log.Debugf("internalProcessUnsignedTransaction[processBatchRequest.ForkId]: %v", processBatchRequest.ForkId)

	// Send Batch to the Executor
	processBatchResponse, err := s.executorClient.ProcessBatch(ctx, processBatchRequest)
	if err != nil {
		// Log this error as an executor unspecified error
		//s.eventLog.LogExecutorError(ctx, pb.ExecutorError_EXECUTOR_ERROR_UNSPECIFIED, processBatchRequest)
		log.Errorf("error processing unsigned transaction ", err)
		return nil, err
	} else if processBatchResponse.Error != executor.EXECUTOR_ERROR_NO_ERROR {
		err = executor.ExecutorErr(processBatchResponse.Error)
		//s.eventLog.LogExecutorError(ctx, processBatchResponse.Error, processBatchRequest)
		return nil, err
	}

	response, err := s.convertToProcessBatchResponse([]types.Transaction{tx}, processBatchResponse)
	if err != nil {
		return nil, err
	}

	if processBatchResponse.Responses[0].Error != pb.RomError(executor.ROM_ERROR_NO_ERROR) {
		err := executor.RomErr(processBatchResponse.Responses[0].Error)
		if !isEVMRevertError(err) {
			return response, err
		}
	}

	return response, nil
}

// isContractCreation checks if the tx is a contract creation
func (s *State) isContractCreation(tx types.Transaction) bool {
	return tx.GetTo() == nil && len(tx.GetData()) > 0
}

// DetermineProcessedTransactions splits the given tx process responses
// returning a slice with only processed and a map unprocessed txs
// respectively.
func DetermineProcessedTransactions(responses []*ProcessTransactionResponse) (
	[]*ProcessTransactionResponse, []string, map[string]*ProcessTransactionResponse, []string) {
	processedTxResponses := []*ProcessTransactionResponse{}
	processedTxsHashes := []string{}
	unprocessedTxResponses := map[string]*ProcessTransactionResponse{}
	unprocessedTxsHashes := []string{}
	for _, response := range responses {
		if response.IsProcessed {
			processedTxResponses = append(processedTxResponses, response)
			processedTxsHashes = append(processedTxsHashes, response.TxHash.String())
		} else {
			log.Infof("Tx %s has not been processed", response.TxHash)
			unprocessedTxResponses[response.TxHash.String()] = response
			unprocessedTxsHashes = append(unprocessedTxsHashes, response.TxHash.String())
		}
	}
	return processedTxResponses, processedTxsHashes, unprocessedTxResponses, unprocessedTxsHashes
}

// StoreTransaction is used by the sequencer to add process a transaction
/*
func (s *State) StoreTransaction(ctx context.Context, batchNumber uint64, processedTx *ProcessTransactionResponse, coinbase common.Address, timestamp uint64, dbTx pgx.Tx) error {
	if dbTx == nil {
		return ErrDBTxNil
	}

	// Check if last batch is closed. Note that it's assumed that only the latest batch can be open
			isBatchClosed, err := s.PostgresStorage.IsBatchClosed(ctx, batchNumber, dbTx)
			if err != nil {
				return err
			}
			if isBatchClosed {
				return ErrBatchAlreadyClosed
			}

		processingContext, err := s.GetProcessingContext(ctx, batchNumber, dbTx)
		if err != nil {
			return err
		}
	// if the transaction has an intrinsic invalid tx error it means
	// the transaction has not changed the state, so we don't store it
	if executor.IsIntrinsicError(executor.RomErrorCode(processedTx.RomError)) {
		return nil
	}

	lastL2Block, err := s.GetLastL2Block(ctx, dbTx)
	if err != nil {
		return err
	}

	header := &types.Header{
		Number:     new(big.Int).SetUint64(lastL2Block.Number().Uint64() + 1),
		ParentHash: lastL2Block.Hash(),
		Coinbase:   coinbase,
		Root:       processedTx.StateRoot,
		GasUsed:    processedTx.GasUsed,
		GasLimit:   s.cfg.MaxCumulativeGasUsed,
		Time:       timestamp,
	}
	transactions := []types.Transaction{&processedTx.Tx}

	receipt := generateReceipt(header.Number, processedTx)
	receipts := []*types.Receipt{receipt}

	// Create block to be able to calculate its hash
	block := types.NewBlock(header, transactions, []*types.Header{}, receipts, &trie.StackTrie{})
	block.ReceivedAt = time.Unix(int64(timestamp), 0)

	receipt.BlockHash = block.Hash()

	// Store L2 block and its transaction
	if err := s.AddL2Block(ctx, batchNumber, block, receipts, dbTx); err != nil {
		return err
	}

	return nil
}
*/

// CheckSupersetBatchTransactions verifies that processedTransactions is a
// superset of existingTxs and that the existing txs have the same order,
// returns a non-nil error if that is not the case.
func CheckSupersetBatchTransactions(existingTxHashes []common.Hash, processedTxs []*ProcessTransactionResponse) error {
	if len(existingTxHashes) > len(processedTxs) {
		return ErrExistingTxGreaterThanProcessedTx
	}
	for i, existingTxHash := range existingTxHashes {
		if existingTxHash != processedTxs[i].TxHash {
			return ErrOutOfOrderProcessedTx
		}
	}
	return nil
}

func b2i(s *big.Int) *uint256.Int {
	iii := &uint256.Int{}
	iii.SetBytes(s.Bytes())
	return iii
}

// EstimateGas for a transaction
func (s *State) EstimateGas(transaction types.Transaction, senderAddress common.Address, l2BlockNumber *uint64, dbTx pgx.Tx) (uint64, error) {
	const ethTransferGas = 21000
	var lowEnd uint64
	var highEnd uint64

	if s.executorClient == nil {
		return 0, ErrExecutorNil
	}
	if s.tree == nil {
		return 0, ErrStateTreeNil
	}

	ctx := context.Background()

	lastBatches, l2BlockStateRoot, err := s.PostgresStorage.GetLastNBatchesByL2BlockNumber(ctx, l2BlockNumber, two, dbTx)
	if err != nil {
		return 0, err
	}

	// Get latest batch from the database to get globalExitRoot and Timestamp
	lastBatch := lastBatches[0]

	// Get batch before latest to get state root and local exit root
	previousBatch := lastBatches[0]
	if len(lastBatches) > 1 {
		previousBatch = lastBatches[1]
	}

	timestamp := uint64(lastBatch.Timestamp.Unix())

	if l2BlockNumber != nil {
		latestL2BlockNumber, err := s.PostgresStorage.GetLastL2BlockNumber(ctx, dbTx)
		if err != nil {
			return 0, err
		}

		if *l2BlockNumber == latestL2BlockNumber {
			timestamp = uint64(time.Now().Unix())
		}
	}

	lowEnd, err = core.IntrinsicGas(transaction.GetData(), transaction.GetAccessList(), s.isContractCreation(transaction), true, false, false)
	if err != nil {
		return 0, err
	}

	if lowEnd == ethTransferGas && transaction.GetTo() != nil {
		code, err := s.tree.GetCode(ctx, *transaction.GetTo(), l2BlockStateRoot.Bytes())
		if err != nil {
			log.Warnf("error while getting transaction.to() code %v", err)
		} else if len(code) == 0 {
			return lowEnd, nil
		}
	}

	if transaction.GetGas() != 0 && transaction.GetGas() > lowEnd {
		highEnd = transaction.GetGas()
	} else {
		highEnd = s.cfg.MaxCumulativeGasUsed
	}

	var availableBalance *big.Int

	if senderAddress != ZeroAddress {
		senderBalance, err := s.tree.GetBalance(ctx, senderAddress, l2BlockStateRoot.Bytes())
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				senderBalance = big.NewInt(0)
			} else {
				return 0, err
			}
		}

		availableBalance = new(big.Int).Set(senderBalance)

		if transaction.GetValue() != nil {
			if transaction.GetValue().Cmp(b2i(availableBalance)) > 0 {
				return 0, ErrInsufficientFunds
			}

			availableBalance.Sub(availableBalance, transaction.GetValue().ToBig())
		}
	}

	if transaction.GetPrice().BitLen() != 0 && // Gas price has been set
		availableBalance != nil && // Available balance is found
		availableBalance.Cmp(big.NewInt(0)) > 0 { // Available balance > 0
		gasAllowance := new(big.Int).Div(availableBalance, transaction.GetPrice().ToBig())

		// Check the gas allowance for this account, make sure high end is capped to it
		if gasAllowance.IsUint64() && highEnd > gasAllowance.Uint64() {
			log.Debugf("Gas estimation high-end capped by allowance [%d]", gasAllowance.Uint64())
			highEnd = gasAllowance.Uint64()
		}
	}

	// Run the transaction with the specified gas value.
	// Returns a status indicating if the transaction failed, if it was reverted and the accompanying error
	testTransaction := func(gas uint64, shouldOmitErr bool) (bool, bool, uint64, error) {
		var gasUsed uint64
		tx := &types.LegacyTx{
			types.CommonTx{
				Nonce: transaction.GetNonce(),
				To:    transaction.GetTo(),
				Value: transaction.GetValue(),
				Gas:   gas,
				Data:  transaction.GetData(),
			},
			transaction.GetPrice(),
		}

		batchL2Data, err := EncodeUnsignedTransaction(tx, s.cfg.ChainID, nil)
		if err != nil {
			log.Errorf("error encoding unsigned transaction ", err)
			return false, false, gasUsed, err
		}

		forkID := GetForkIDByBatchNumber(s.cfg.ForkIDIntervals, lastBatch.BatchNumber)
		// Create a batch to be sent to the executor
		processBatchRequest := &pb.ProcessBatchRequest{
			OldBatchNum:      lastBatch.BatchNumber,
			BatchL2Data:      batchL2Data,
			From:             senderAddress.String(),
			OldStateRoot:     l2BlockStateRoot.Bytes(),
			GlobalExitRoot:   lastBatch.GlobalExitRoot.Bytes(),
			OldAccInputHash:  previousBatch.AccInputHash.Bytes(),
			EthTimestamp:     timestamp,
			Coinbase:         lastBatch.Coinbase.String(),
			UpdateMerkleTree: cFalse,
			ChainId:          s.cfg.ChainID,
			ForkId:           forkID,
		}

		log.Debugf("EstimateGas[processBatchRequest.OldBatchNum]: %v", processBatchRequest.OldBatchNum)
		// log.Debugf("EstimateGas[processBatchRequest.BatchL2Data]: %v", hex.EncodeToHex(processBatchRequest.BatchL2Data))
		log.Debugf("EstimateGas[processBatchRequest.From]: %v", processBatchRequest.From)
		log.Debugf("EstimateGas[processBatchRequest.OldStateRoot]: %v", hex.EncodeToHex(processBatchRequest.OldStateRoot))
		log.Debugf("EstimateGas[processBatchRequest.globalExitRoot]: %v", hex.EncodeToHex(processBatchRequest.GlobalExitRoot))
		log.Debugf("EstimateGas[processBatchRequest.OldAccInputHash]: %v", hex.EncodeToHex(processBatchRequest.OldAccInputHash))
		log.Debugf("EstimateGas[processBatchRequest.EthTimestamp]: %v", processBatchRequest.EthTimestamp)
		log.Debugf("EstimateGas[processBatchRequest.Coinbase]: %v", processBatchRequest.Coinbase)
		log.Debugf("EstimateGas[processBatchRequest.UpdateMerkleTree]: %v", processBatchRequest.UpdateMerkleTree)
		log.Debugf("EstimateGas[processBatchRequest.ChainId]: %v", processBatchRequest.ChainId)
		log.Debugf("EstimateGas[processBatchRequest.ForkId]: %v", processBatchRequest.ForkId)

		txExecutionOnExecutorTime := time.Now()
		processBatchResponse, err := s.executorClient.ProcessBatch(ctx, processBatchRequest)
		log.Debugf("executor time: %vms", time.Since(txExecutionOnExecutorTime).Milliseconds())
		if err != nil {
			log.Errorf("error estimating gas: %v", err)
			return false, false, gasUsed, err
		}
		gasUsed = processBatchResponse.Responses[0].GasUsed
		if processBatchResponse.Error != executor.EXECUTOR_ERROR_NO_ERROR {
			err = executor.ExecutorErr(processBatchResponse.Error)
			//s.eventLog.LogExecutorError(ctx, processBatchResponse.Error, processBatchRequest)
			return false, false, gasUsed, err
		}

		// Check if an out of gas error happened during EVM execution
		if processBatchResponse.Responses[0].Error != pb.RomError(executor.ROM_ERROR_NO_ERROR) {
			err := executor.RomErr(processBatchResponse.Responses[0].Error)

			if (isGasEVMError(err) || isGasApplyError(err)) && shouldOmitErr {
				// Specifying the transaction failed, but not providing an error
				// is an indication that a valid error occurred due to low gas,
				// which will increase the lower bound for the search
				return true, false, gasUsed, nil
			}

			if isEVMRevertError(err) {
				// The EVM reverted during execution, attempt to extract the
				// error message and return it
				return true, true, gasUsed, constructErrorFromRevert(err, processBatchResponse.Responses[0].ReturnValue)
			}

			return true, false, gasUsed, err
		}

		return false, false, gasUsed, nil
	}

	txExecutions := []time.Duration{}
	var totalExecutionTime time.Duration

	// Check if the highEnd is a good value to make the transaction pass
	failed, reverted, gasUsed, err := testTransaction(highEnd, false)
	log.Debugf("Estimate gas. Trying to execute TX with %v gas", highEnd)
	if failed {
		if reverted {
			return 0, err
		}

		// The transaction shouldn't fail, for whatever reason, at highEnd
		return 0, fmt.Errorf(
			"unable to apply transaction even for the highest gas limit %d: %w",
			highEnd,
			err,
		)
	}

	if lowEnd < gasUsed {
		lowEnd = gasUsed
	}

	// Start the binary search for the lowest possible gas price
	for (lowEnd < highEnd) && (highEnd-lowEnd) > 4096 {
		txExecutionStart := time.Now()
		mid := (lowEnd + highEnd) / uint64(two)

		log.Debugf("Estimate gas. Trying to execute TX with %v gas", mid)

		failed, reverted, _, testErr := testTransaction(mid, true)
		executionTime := time.Since(txExecutionStart)
		totalExecutionTime += executionTime
		txExecutions = append(txExecutions, executionTime)
		if testErr != nil && !reverted {
			// Reverts are ignored in the binary search, but are checked later on
			// during the execution for the optimal gas limit found
			return 0, testErr
		}

		if failed {
			// If the transaction failed => increase the gas
			lowEnd = mid + 1
		} else {
			// If the transaction didn't fail => make this ok value the high end
			highEnd = mid
		}
	}

	executions := int64(len(txExecutions))
	if executions > 0 {
		log.Debugf("EstimateGas executed the TX %v times", executions)
		averageExecutionTime := totalExecutionTime.Milliseconds() / executions
		log.Debugf("EstimateGas tx execution average time is %v milliseconds", averageExecutionTime)
	} else {
		log.Error("Estimate gas. Tx not executed")
	}
	return highEnd, nil
}

// Checks if executor level valid gas errors occurred
func isGasApplyError(err error) bool {
	return errors.Is(err, ErrNotEnoughIntrinsicGas)
}

// Checks if EVM level valid gas errors occurred
func isGasEVMError(err error) bool {
	return errors.Is(err, runtime.ErrOutOfGas)
}

// Checks if the EVM reverted during execution
func isEVMRevertError(err error) bool {
	return errors.Is(err, runtime.ErrExecutionReverted)
}
