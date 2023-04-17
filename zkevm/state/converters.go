package state

import (
	"fmt"
	"math/big"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	ericommon "github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/zkevm/encoding"
	"github.com/ledgerwatch/erigon/zkevm/hex"
	"github.com/ledgerwatch/erigon/zkevm/log"
	"github.com/ledgerwatch/erigon/zkevm/state/runtime/executor"
	"github.com/ledgerwatch/erigon/zkevm/state/runtime/executor/pb"
	"github.com/ledgerwatch/erigon/zkevm/state/runtime/fakevm"
	"github.com/ledgerwatch/erigon/zkevm/state/runtime/instrumentation"
)

// ConvertToCounters extracts ZKCounters from a ProcessBatchResponse
func ConvertToCounters(resp *pb.ProcessBatchResponse) ZKCounters {
	return ZKCounters{
		CumulativeGasUsed:    resp.CumulativeGasUsed,
		UsedKeccakHashes:     resp.CntKeccakHashes,
		UsedPoseidonHashes:   resp.CntPoseidonHashes,
		UsedPoseidonPaddings: resp.CntPoseidonPaddings,
		UsedMemAligns:        resp.CntMemAligns,
		UsedArithmetics:      resp.CntArithmetics,
		UsedBinaries:         resp.CntBinaries,
		UsedSteps:            resp.CntSteps,
	}
}

// TestConvertToProcessBatchResponse for test purposes
func (s *State) TestConvertToProcessBatchResponse(txs []types.Transaction, response *pb.ProcessBatchResponse) (*ProcessBatchResponse, error) {
	return s.convertToProcessBatchResponse(txs, response)
}

func (s *State) convertToProcessBatchResponse(txs []types.Transaction, response *pb.ProcessBatchResponse) (*ProcessBatchResponse, error) {
	responses, err := s.convertToProcessTransactionResponse(txs, response.Responses)
	if err != nil {
		return nil, err
	}

	readWriteAddresses, err := convertToReadWriteAddresses(response.ReadWriteAddresses)
	if err != nil {
		return nil, err
	}

	isBatchProcessed := response.Error == executor.EXECUTOR_ERROR_NO_ERROR
	if isBatchProcessed && len(response.Responses) > 0 {
		// Check out of counters
		errorToCheck := response.Responses[len(response.Responses)-1].Error
		isBatchProcessed = !executor.IsROMOutOfCountersError(errorToCheck)
	}

	return &ProcessBatchResponse{
		NewStateRoot:       common.BytesToHash(response.NewStateRoot),
		NewAccInputHash:    common.BytesToHash(response.NewAccInputHash),
		NewLocalExitRoot:   common.BytesToHash(response.NewLocalExitRoot),
		NewBatchNumber:     response.NewBatchNum,
		UsedZkCounters:     convertToCounters(response),
		Responses:          responses,
		ExecutorError:      executor.ExecutorErr(response.Error),
		IsBatchProcessed:   isBatchProcessed,
		ReadWriteAddresses: readWriteAddresses,
	}, nil
}

func isProcessed(err pb.RomError) bool {
	return !executor.IsIntrinsicError(err) && !executor.IsROMOutOfCountersError(err)
}

func convertToReadWriteAddresses(addresses map[string]*pb.InfoReadWrite) (map[common.Address]*InfoReadWrite, error) {
	results := make(map[common.Address]*InfoReadWrite, len(addresses))

	for addr, addrInfo := range addresses {
		var nonce *uint64 = nil
		var balance *big.Int = nil
		var ok bool

		address := common.HexToAddress(addr)

		if addrInfo.Nonce != "" {
			bigNonce, ok := new(big.Int).SetString(addrInfo.Nonce, encoding.Base10)
			if !ok {
				log.Debugf("received nonce as string: %v", addrInfo.Nonce)
				return nil, fmt.Errorf("error while parsing address nonce")
			}
			nonceNp := bigNonce.Uint64()
			nonce = &nonceNp
		}

		if addrInfo.Balance != "" {
			balance, ok = new(big.Int).SetString(addrInfo.Balance, encoding.Base10)
			if !ok {
				log.Debugf("received balance as string: %v", addrInfo.Balance)
				return nil, fmt.Errorf("error while parsing address balance")
			}
		}

		results[address] = &InfoReadWrite{Address: address, Nonce: nonce, Balance: balance}
	}

	return results, nil
}

func (s *State) convertToProcessTransactionResponse(txs []types.Transaction, responses []*pb.ProcessTransactionResponse) ([]*ProcessTransactionResponse, error) {
	results := make([]*ProcessTransactionResponse, 0, len(responses))
	for i, response := range responses {
		trace, err := convertToStructLogArray(response.ExecutionTrace)
		if err != nil {
			return nil, err
		}

		result := new(ProcessTransactionResponse)
		result.TxHash = common.BytesToHash(response.TxHash)
		result.Type = response.Type
		result.ReturnValue = response.ReturnValue
		result.GasLeft = response.GasLeft
		result.GasUsed = response.GasUsed
		result.GasRefunded = response.GasRefunded
		result.RomError = executor.RomErr(response.Error)
		result.CreateAddress = common.HexToAddress(response.CreateAddress)
		result.StateRoot = common.BytesToHash(response.StateRoot)
		result.Logs = convertToLog(response.Logs)
		result.IsProcessed = isProcessed(response.Error)
		result.ExecutionTrace = *trace
		result.CallTrace = convertToExecutorTrace(response.CallTrace)
		result.Tx = txs[i]

		_, err = DecodeTx(ericommon.Bytes2Hex(response.GetRlpTx()))
		if err != nil {
			timestamp := time.Now()
			log.Errorf("error decoding rlp returned by executor %v at %v", err, timestamp)
			panic("error log isn't available -- igorm")
			/*

				event := &event.Event{
					ReceivedAt: timestamp,
					Source:     event.Source_Node,
					Level:      event.Level_Error,
					EventID:    event.EventID_ExecutorRLPError,
					Json:       string(response.GetRlpTx()),
				}

				err = s.eventLog.LogEvent(context.Background(), event)
				if err != nil {
					log.Errorf("error storing payload: %v", err)
				}
			*/
		}

		results = append(results, result)

		log.Debugf("ProcessTransactionResponse[TxHash]: %v", result.TxHash)
		log.Debugf("ProcessTransactionResponse[Nonce]: %v", result.Tx.GetNonce())
		log.Debugf("ProcessTransactionResponse[StateRoot]: %v", result.StateRoot.String())
		log.Debugf("ProcessTransactionResponse[Error]: %v", result.RomError)
		log.Debugf("ProcessTransactionResponse[GasUsed]: %v", result.GasUsed)
		log.Debugf("ProcessTransactionResponse[GasLeft]: %v", result.GasLeft)
		log.Debugf("ProcessTransactionResponse[GasRefunded]: %v", result.GasRefunded)
		log.Debugf("ProcessTransactionResponse[IsProcessed]: %v", result.IsProcessed)
	}

	return results, nil
}

func convertToLog(protoLogs []*pb.Log) []*types.Log {
	logs := make([]*types.Log, 0, len(protoLogs))

	for _, protoLog := range protoLogs {
		log := new(types.Log)
		log.Address = common.HexToAddress(protoLog.Address)
		log.Topics = convertToTopics(protoLog.Topics)
		log.Data = protoLog.Data
		log.BlockNumber = protoLog.BatchNumber
		log.TxHash = common.BytesToHash(protoLog.TxHash)
		log.TxIndex = uint(protoLog.TxIndex)
		log.BlockHash = common.BytesToHash(protoLog.BatchHash)
		log.Index = uint(protoLog.Index)
		logs = append(logs, log)
	}

	return logs
}

func convertToTopics(responses [][]byte) []common.Hash {
	results := make([]common.Hash, 0, len(responses))

	for _, response := range responses {
		results = append(results, common.BytesToHash(response))
	}
	return results
}

func convertToStructLogArray(responses []*pb.ExecutionTraceStep) (*[]instrumentation.StructLog, error) {
	results := make([]instrumentation.StructLog, 0, len(responses))

	for _, response := range responses {
		convertedStack, err := convertToBigIntArray(response.Stack)
		if err != nil {
			return nil, err
		}
		result := new(instrumentation.StructLog)
		result.Pc = response.Pc
		result.Op = response.Op
		result.Gas = response.RemainingGas
		result.GasCost = response.GasCost
		result.Memory = response.Memory
		result.MemorySize = int(response.MemorySize)
		result.Stack = convertedStack
		result.ReturnData = response.ReturnData
		result.Storage = convertToProperMap(response.Storage)
		result.Depth = int(response.Depth)
		result.RefundCounter = response.GasRefund
		result.Err = executor.RomErr(response.Error)

		results = append(results, *result)
	}
	return &results, nil
}

func convertToBigIntArray(responses []string) ([]*big.Int, error) {
	results := make([]*big.Int, 0, len(responses))

	for _, response := range responses {
		result, ok := new(big.Int).SetString(response, hex.Base)
		if ok {
			results = append(results, result)
		} else {
			return nil, fmt.Errorf("string %s is not valid", response)
		}
	}
	return results, nil
}

func convertToProperMap(responses map[string]string) map[common.Hash]common.Hash {
	results := make(map[common.Hash]common.Hash, len(responses))
	for key, response := range responses {
		results[common.HexToHash(key)] = common.HexToHash(response)
	}
	return results
}

func convertToExecutorTrace(callTrace *pb.CallTrace) instrumentation.ExecutorTrace {
	trace := new(instrumentation.ExecutorTrace)
	if callTrace != nil {
		trace.Context = convertToContext(callTrace.Context)
		trace.Steps = convertToInstrumentationSteps(callTrace.Steps)
	}

	return *trace
}

func convertToContext(context *pb.TransactionContext) instrumentation.Context {
	return instrumentation.Context{
		Type:         context.Type,
		From:         context.From,
		To:           context.To,
		Input:        string(context.Data),
		Gas:          fmt.Sprint(context.Gas),
		Value:        context.Value,
		Output:       string(context.Output),
		GasPrice:     context.GasPrice,
		OldStateRoot: string(context.OldStateRoot),
		Time:         uint64(context.ExecutionTime),
		GasUsed:      fmt.Sprint(context.GasUsed),
	}
}

func convertToInstrumentationSteps(responses []*pb.TransactionStep) []instrumentation.Step {
	results := make([]instrumentation.Step, 0, len(responses))
	for _, response := range responses {
		step := new(instrumentation.Step)
		step.StateRoot = string(response.StateRoot)
		step.Depth = int(response.Depth)
		step.Pc = response.Pc
		step.Gas = fmt.Sprint(response.Gas)
		step.OpCode = fakevm.OpCode(response.Op).String()
		step.Refund = fmt.Sprint(response.GasRefund)
		step.Op = fmt.Sprint(response.Op)
		err := executor.RomErr(response.Error)
		if err != nil {
			step.Error = err.Error()
		}
		step.Contract = convertToInstrumentationContract(response.Contract)
		step.GasCost = fmt.Sprint(response.GasCost)
		step.Stack = response.Stack
		step.Memory = convertByteArrayToStringArray(response.Memory)
		step.ReturnData = string(response.ReturnData)

		results = append(results, *step)
	}
	return results
}

func convertToInstrumentationContract(response *pb.Contract) instrumentation.Contract {
	return instrumentation.Contract{
		Address: response.Address,
		Caller:  response.Caller,
		Value:   response.Value,
		Input:   string(response.Data),
		Gas:     fmt.Sprint(response.Gas),
	}
}

func convertByteArrayToStringArray(responses []byte) []string {
	results := make([]string, 0, len(responses))
	for _, response := range responses {
		results = append(results, string(response))
	}
	return results
}

func convertToCounters(resp *pb.ProcessBatchResponse) ZKCounters {
	return ZKCounters{
		CumulativeGasUsed:    resp.CumulativeGasUsed,
		UsedKeccakHashes:     resp.CntKeccakHashes,
		UsedPoseidonHashes:   resp.CntPoseidonHashes,
		UsedPoseidonPaddings: resp.CntPoseidonPaddings,
		UsedMemAligns:        resp.CntMemAligns,
		UsedArithmetics:      resp.CntArithmetics,
		UsedBinaries:         resp.CntBinaries,
		UsedSteps:            resp.CntSteps,
	}
}
