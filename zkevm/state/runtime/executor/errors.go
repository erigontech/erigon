package executor

import (
	"fmt"
	"math"

	"github.com/ledgerwatch/erigon/zkevm/state/runtime"
	"github.com/ledgerwatch/erigon/zkevm/state/runtime/executor/pb"
)

const (
	// ROM_ERROR_UNSPECIFIED indicates the execution ended successfully
	ROM_ERROR_UNSPECIFIED int32 = iota
	// ROM_ERROR_NO_ERROR indicates the execution ended successfully
	ROM_ERROR_NO_ERROR
	// ROM_ERROR_OUT_OF_GAS indicates there is not enough balance to continue the execution
	ROM_ERROR_OUT_OF_GAS
	// ROM_ERROR_STACK_OVERFLOW indicates a stack overflow has happened
	ROM_ERROR_STACK_OVERFLOW
	// ROM_ERROR_STACK_UNDERFLOW indicates a stack overflow has happened
	ROM_ERROR_STACK_UNDERFLOW
	// ROM_ERROR_MAX_CODE_SIZE_EXCEEDED indicates the code size is beyond the maximum
	ROM_ERROR_MAX_CODE_SIZE_EXCEEDED
	// ROM_ERROR_CONTRACT_ADDRESS_COLLISION there is a collision regarding contract addresses
	ROM_ERROR_CONTRACT_ADDRESS_COLLISION
	// ROM_ERROR_EXECUTION_REVERTED indicates the execution has been reverted
	ROM_ERROR_EXECUTION_REVERTED
	// ROM_ERROR_OUT_OF_COUNTERS_STEP indicates there is not enough step counters to continue the execution
	ROM_ERROR_OUT_OF_COUNTERS_STEP
	// ROM_ERROR_OUT_OF_COUNTERS_KECCAK indicates there is not enough keccak counters to continue the execution
	ROM_ERROR_OUT_OF_COUNTERS_KECCAK
	// ROM_ERROR_OUT_OF_COUNTERS_BINARY indicates there is not enough binary counters to continue the execution
	ROM_ERROR_OUT_OF_COUNTERS_BINARY
	// ROM_ERROR_OUT_OF_COUNTERS_MEM indicates there is not enough memory aligncounters to continue the execution
	ROM_ERROR_OUT_OF_COUNTERS_MEM
	// ROM_ERROR_OUT_OF_COUNTERS_ARITH indicates there is not enough arith counters to continue the execution
	ROM_ERROR_OUT_OF_COUNTERS_ARITH
	// ROM_ERROR_OUT_OF_COUNTERS_PADDING indicates there is not enough padding counters to continue the execution
	ROM_ERROR_OUT_OF_COUNTERS_PADDING
	// ROM_ERROR_OUT_OF_COUNTERS_POSEIDON indicates there is not enough poseidon counters to continue the execution
	ROM_ERROR_OUT_OF_COUNTERS_POSEIDON
	// ROM_ERROR_INVALID_JUMP indicates there is an invalid jump opcode
	ROM_ERROR_INVALID_JUMP
	// ROM_ERROR_INVALID_OPCODE indicates there is an invalid opcode
	ROM_ERROR_INVALID_OPCODE
	// ROM_ERROR_INVALID_STATIC indicates there is an invalid static call
	ROM_ERROR_INVALID_STATIC
	// ROM_ERROR_INVALID_BYTECODE_STARTS_EF indicates there is a bytecode starting with 0xEF
	ROM_ERROR_INVALID_BYTECODE_STARTS_EF
	// ROM_ERROR_INTRINSIC_INVALID_SIGNATURE indicates the transaction is failing at the signature intrinsic check
	ROM_ERROR_INTRINSIC_INVALID_SIGNATURE
	// ROM_ERROR_INTRINSIC_INVALID_CHAIN_ID indicates the transaction is failing at the chain id intrinsic check
	ROM_ERROR_INTRINSIC_INVALID_CHAIN_ID
	// ROM_ERROR_INTRINSIC_INVALID_NONCE indicates the transaction is failing at the nonce intrinsic check
	ROM_ERROR_INTRINSIC_INVALID_NONCE
	// ROM_ERROR_INTRINSIC_INVALID_GAS_LIMIT indicates the transaction is failing at the gas limit intrinsic check
	ROM_ERROR_INTRINSIC_INVALID_GAS_LIMIT
	// ROM_ERROR_INTRINSIC_INVALID_BALANCE indicates the transaction is failing at balance intrinsic check
	ROM_ERROR_INTRINSIC_INVALID_BALANCE
	// ROM_ERROR_INTRINSIC_INVALID_BATCH_GAS_LIMIT indicates the batch is exceeding the batch gas limit
	ROM_ERROR_INTRINSIC_INVALID_BATCH_GAS_LIMIT
	// ROM_ERROR_INTRINSIC_INVALID_SENDER_CODE indicates the batch is exceeding the batch gas limit
	ROM_ERROR_INTRINSIC_INVALID_SENDER_CODE
	// ROM_ERROR_INTRINSIC_TX_GAS_OVERFLOW indicates the transaction gasLimit*gasPrice > MAX_UINT_256 - 1
	ROM_ERROR_INTRINSIC_TX_GAS_OVERFLOW
	// ROM_ERROR_BATCH_DATA_TOO_BIG indicates the batch_l2_data is too big to be processed
	ROM_ERROR_BATCH_DATA_TOO_BIG
	// ROM_ERROR_UNSUPPORTED_FORK_ID indicates that the fork id is not supported
	ROM_ERROR_UNSUPPORTED_FORK_ID
	// EXECUTOR_ERROR_UNSPECIFIED indicates the execution ended successfully
	EXECUTOR_ERROR_UNSPECIFIED = 0
	// EXECUTOR_ERROR_NO_ERROR indicates there was no error
	EXECUTOR_ERROR_NO_ERROR = 1
	// EXECUTOR_ERROR_COUNTERS_OVERFLOW_KECCAK indicates that the keccak counter exceeded the maximum
	EXECUTOR_ERROR_COUNTERS_OVERFLOW_KECCAK = 2
	// EXECUTOR_ERROR_COUNTERS_OVERFLOW_BINARY indicates that the binary counter exceeded the maximum
	EXECUTOR_ERROR_COUNTERS_OVERFLOW_BINARY = 3
	// EXECUTOR_ERROR_COUNTERS_OVERFLOW_MEM indicates that the memory align counter exceeded the maximum
	EXECUTOR_ERROR_COUNTERS_OVERFLOW_MEM = 4
	// EXECUTOR_ERROR_COUNTERS_OVERFLOW_ARITH indicates that the arith counter exceeded the maximum
	EXECUTOR_ERROR_COUNTERS_OVERFLOW_ARITH = 5
	// EXECUTOR_ERROR_COUNTERS_OVERFLOW_PADDING indicates that the padding counter exceeded the maximum
	EXECUTOR_ERROR_COUNTERS_OVERFLOW_PADDING = 6
	// EXECUTOR_ERROR_COUNTERS_OVERFLOW_POSEIDON indicates that the poseidon counter exceeded the maximum
	EXECUTOR_ERROR_COUNTERS_OVERFLOW_POSEIDON = 7
	// EXECUTOR_ERROR_UNSUPPORTED_FORK_ID indicates that the fork id is not supported
	EXECUTOR_ERROR_UNSUPPORTED_FORK_ID = 8
	// EXECUTOR_ERROR_BALANCE_MISMATCH indicates that there is a balance mismatch error in the ROM
	EXECUTOR_ERROR_BALANCE_MISMATCH = 9
	// EXECUTOR_ERROR_FEA2SCALAR indicates that there is a fea2scalar error in the execution
	EXECUTOR_ERROR_FEA2SCALAR = 10
	// EXECUTOR_ERROR_TOS32 indicates that there is a TOS32 error in the execution
	EXECUTOR_ERROR_TOS32 = 11
)

// RomErr returns an instance of error related to the ExecutorError
func RomErr(errorCode pb.RomError) error {
	e := int32(errorCode)
	switch e {
	case ROM_ERROR_UNSPECIFIED:
		return fmt.Errorf("unspecified ROM error")
	case ROM_ERROR_NO_ERROR:
		return nil
	case ROM_ERROR_OUT_OF_GAS:
		return runtime.ErrOutOfGas
	case ROM_ERROR_STACK_OVERFLOW:
		return runtime.ErrStackOverflow
	case ROM_ERROR_STACK_UNDERFLOW:
		return runtime.ErrStackUnderflow
	case ROM_ERROR_MAX_CODE_SIZE_EXCEEDED:
		return runtime.ErrMaxCodeSizeExceeded
	case ROM_ERROR_CONTRACT_ADDRESS_COLLISION:
		return runtime.ErrContractAddressCollision
	case ROM_ERROR_EXECUTION_REVERTED:
		return runtime.ErrExecutionReverted
	case ROM_ERROR_OUT_OF_COUNTERS_STEP:
		return runtime.ErrOutOfCountersKeccak
	case ROM_ERROR_OUT_OF_COUNTERS_KECCAK:
		return runtime.ErrOutOfCountersKeccak
	case ROM_ERROR_OUT_OF_COUNTERS_BINARY:
		return runtime.ErrOutOfCountersBinary
	case ROM_ERROR_OUT_OF_COUNTERS_MEM:
		return runtime.ErrOutOfCountersMemory
	case ROM_ERROR_OUT_OF_COUNTERS_ARITH:
		return runtime.ErrOutOfCountersArith
	case ROM_ERROR_OUT_OF_COUNTERS_PADDING:
		return runtime.ErrOutOfCountersPadding
	case ROM_ERROR_OUT_OF_COUNTERS_POSEIDON:
		return runtime.ErrOutOfCountersPoseidon
	case ROM_ERROR_INVALID_JUMP:
		return runtime.ErrInvalidJump
	case ROM_ERROR_INVALID_OPCODE:
		return runtime.ErrInvalidOpCode
	case ROM_ERROR_INVALID_STATIC:
		return runtime.ErrInvalidStatic
	case ROM_ERROR_INVALID_BYTECODE_STARTS_EF:
		return runtime.ErrInvalidByteCodeStartsEF
	case ROM_ERROR_INTRINSIC_INVALID_SIGNATURE:
		return runtime.ErrIntrinsicInvalidSignature
	case ROM_ERROR_INTRINSIC_INVALID_CHAIN_ID:
		return runtime.ErrIntrinsicInvalidChainID
	case ROM_ERROR_INTRINSIC_INVALID_NONCE:
		return runtime.ErrIntrinsicInvalidNonce
	case ROM_ERROR_INTRINSIC_INVALID_GAS_LIMIT:
		return runtime.ErrIntrinsicInvalidGasLimit
	case ROM_ERROR_INTRINSIC_INVALID_BALANCE:
		return runtime.ErrIntrinsicInvalidBalance
	case ROM_ERROR_INTRINSIC_INVALID_BATCH_GAS_LIMIT:
		return runtime.ErrIntrinsicInvalidGasLimit
	case ROM_ERROR_INTRINSIC_INVALID_SENDER_CODE:
		return runtime.ErrIntrinsicInvalidSenderCode
	case ROM_ERROR_INTRINSIC_TX_GAS_OVERFLOW:
		return runtime.ErrIntrinsicInvalidTxGasOverflow
	case ROM_ERROR_BATCH_DATA_TOO_BIG:
		return runtime.ErrBatchDataTooBig
	case ROM_ERROR_UNSUPPORTED_FORK_ID:
		return runtime.ErrUnsupportedForkId
	}
	return fmt.Errorf("unknown error")
}

// RomErrorCode returns the error code for a given error
func RomErrorCode(err error) pb.RomError {
	switch err {
	case nil:
		return pb.RomError(ROM_ERROR_NO_ERROR)
	case runtime.ErrOutOfGas:
		return pb.RomError(ROM_ERROR_OUT_OF_GAS)
	case runtime.ErrStackOverflow:
		return pb.RomError(ROM_ERROR_STACK_OVERFLOW)
	case runtime.ErrStackUnderflow:
		return pb.RomError(ROM_ERROR_STACK_UNDERFLOW)
	case runtime.ErrMaxCodeSizeExceeded:
		return pb.RomError(ROM_ERROR_MAX_CODE_SIZE_EXCEEDED)
	case runtime.ErrContractAddressCollision:
		return pb.RomError(ROM_ERROR_CONTRACT_ADDRESS_COLLISION)
	case runtime.ErrExecutionReverted:
		return pb.RomError(ROM_ERROR_EXECUTION_REVERTED)
	case runtime.ErrOutOfCountersKeccak:
		return pb.RomError(ROM_ERROR_OUT_OF_COUNTERS_KECCAK)
	case runtime.ErrOutOfCountersBinary:
		return pb.RomError(ROM_ERROR_OUT_OF_COUNTERS_BINARY)
	case runtime.ErrOutOfCountersMemory:
		return pb.RomError(ROM_ERROR_OUT_OF_COUNTERS_MEM)
	case runtime.ErrOutOfCountersArith:
		return pb.RomError(ROM_ERROR_OUT_OF_COUNTERS_ARITH)
	case runtime.ErrOutOfCountersPadding:
		return pb.RomError(ROM_ERROR_OUT_OF_COUNTERS_PADDING)
	case runtime.ErrOutOfCountersPoseidon:
		return pb.RomError(ROM_ERROR_OUT_OF_COUNTERS_POSEIDON)
	case runtime.ErrInvalidJump:
		return pb.RomError(ROM_ERROR_INVALID_JUMP)
	case runtime.ErrInvalidOpCode:
		return pb.RomError(ROM_ERROR_INVALID_OPCODE)
	case runtime.ErrInvalidStatic:
		return pb.RomError(ROM_ERROR_INVALID_STATIC)
	case runtime.ErrInvalidByteCodeStartsEF:
		return pb.RomError(ROM_ERROR_INVALID_BYTECODE_STARTS_EF)
	case runtime.ErrIntrinsicInvalidSignature:
		return pb.RomError(ROM_ERROR_INTRINSIC_INVALID_SIGNATURE)
	case runtime.ErrIntrinsicInvalidChainID:
		return pb.RomError(ROM_ERROR_INTRINSIC_INVALID_CHAIN_ID)
	case runtime.ErrIntrinsicInvalidNonce:
		return pb.RomError(ROM_ERROR_INTRINSIC_INVALID_NONCE)
	case runtime.ErrIntrinsicInvalidGasLimit:
		return pb.RomError(ROM_ERROR_INTRINSIC_INVALID_GAS_LIMIT)
	case runtime.ErrIntrinsicInvalidBalance:
		return pb.RomError(ROM_ERROR_INTRINSIC_INVALID_BALANCE)
	case runtime.ErrIntrinsicInvalidGasLimit:
		return pb.RomError(ROM_ERROR_INTRINSIC_INVALID_BATCH_GAS_LIMIT)
	case runtime.ErrIntrinsicInvalidSenderCode:
		return pb.RomError(ROM_ERROR_INTRINSIC_INVALID_SENDER_CODE)
	case runtime.ErrIntrinsicInvalidTxGasOverflow:
		return pb.RomError(ROM_ERROR_INTRINSIC_TX_GAS_OVERFLOW)
	case runtime.ErrBatchDataTooBig:
		return pb.RomError(ROM_ERROR_BATCH_DATA_TOO_BIG)
	case runtime.ErrUnsupportedForkId:
		return pb.RomError(ROM_ERROR_UNSUPPORTED_FORK_ID)
	}
	return math.MaxInt32
}

// IsROMOutOfCountersError indicates if the error is an ROM OOC
func IsROMOutOfCountersError(error pb.RomError) bool {
	return int32(error) >= ROM_ERROR_OUT_OF_COUNTERS_STEP && int32(error) <= ROM_ERROR_OUT_OF_COUNTERS_POSEIDON
}

// IsROMOutOfGasError indicates if the error is an ROM OOG
func IsROMOutOfGasError(error pb.RomError) bool {
	return int32(error) == ROM_ERROR_OUT_OF_GAS
}

// IsExecutorOutOfCountersError indicates if the error is an ROM OOC
func IsExecutorOutOfCountersError(error pb.ExecutorError) bool {
	return int32(error) >= EXECUTOR_ERROR_COUNTERS_OVERFLOW_KECCAK && int32(error) <= ROM_ERROR_OUT_OF_COUNTERS_POSEIDON
}

// IsExecutorUnespecifiedError indicates an unespecified error in the executor
func IsExecutorUnespecifiedError(error pb.ExecutorError) bool {
	return int32(error) == EXECUTOR_ERROR_UNSPECIFIED
}

// IsIntrinsicError indicates if the error is due to a intrinsic check
func IsIntrinsicError(error pb.RomError) bool {
	return int32(error) >= ROM_ERROR_INTRINSIC_INVALID_SIGNATURE && int32(error) <= ROM_ERROR_INTRINSIC_TX_GAS_OVERFLOW
}

// IsInvalidNonceError indicates if the error is due to a invalid nonce
func IsInvalidNonceError(error pb.RomError) bool {
	return int32(error) == ROM_ERROR_INTRINSIC_INVALID_NONCE
}

// IsInvalidBalanceError indicates if the error is due to a invalid balance
func IsInvalidBalanceError(error pb.RomError) bool {
	return int32(error) == ROM_ERROR_INTRINSIC_INVALID_BALANCE
}

// ExecutorErr returns an instance of error related to the ExecutorError
func ExecutorErr(errorCode pb.ExecutorError) error {
	e := int32(errorCode)
	switch e {
	case EXECUTOR_ERROR_UNSPECIFIED:
		return fmt.Errorf("unspecified executor error")
	case EXECUTOR_ERROR_NO_ERROR:
		return nil
	case EXECUTOR_ERROR_COUNTERS_OVERFLOW_KECCAK:
		return runtime.ErrOutOfCountersKeccak
	case EXECUTOR_ERROR_COUNTERS_OVERFLOW_BINARY:
		return runtime.ErrOutOfCountersBinary
	case EXECUTOR_ERROR_COUNTERS_OVERFLOW_MEM:
		return runtime.ErrOutOfCountersMemory
	case EXECUTOR_ERROR_COUNTERS_OVERFLOW_ARITH:
		return runtime.ErrOutOfCountersArith
	case EXECUTOR_ERROR_COUNTERS_OVERFLOW_PADDING:
		return runtime.ErrOutOfCountersPadding
	case EXECUTOR_ERROR_COUNTERS_OVERFLOW_POSEIDON:
		return runtime.ErrOutOfCountersPoseidon
	case EXECUTOR_ERROR_UNSUPPORTED_FORK_ID:
		return runtime.ErrUnsupportedForkId
	case EXECUTOR_ERROR_BALANCE_MISMATCH:
		return runtime.ErrBalanceMismatch
	case EXECUTOR_ERROR_FEA2SCALAR:
		return runtime.ErrFea2Scalar
	case EXECUTOR_ERROR_TOS32:
		return runtime.ErrTos32
	}
	return fmt.Errorf("unknown error")
}

// ExecutorErrorCode returns the error code for a given error
func ExecutorErrorCode(err error) pb.ExecutorError {
	switch err {
	case nil:
		return pb.ExecutorError(EXECUTOR_ERROR_NO_ERROR)
	case runtime.ErrOutOfCountersKeccak:
		return pb.ExecutorError(EXECUTOR_ERROR_COUNTERS_OVERFLOW_KECCAK)
	case runtime.ErrOutOfCountersBinary:
		return pb.ExecutorError(EXECUTOR_ERROR_COUNTERS_OVERFLOW_BINARY)
	case runtime.ErrOutOfCountersMemory:
		return pb.ExecutorError(EXECUTOR_ERROR_COUNTERS_OVERFLOW_MEM)
	case runtime.ErrOutOfCountersArith:
		return pb.ExecutorError(EXECUTOR_ERROR_COUNTERS_OVERFLOW_ARITH)
	case runtime.ErrOutOfCountersPadding:
		return pb.ExecutorError(EXECUTOR_ERROR_COUNTERS_OVERFLOW_PADDING)
	case runtime.ErrOutOfCountersPoseidon:
		return pb.ExecutorError(EXECUTOR_ERROR_COUNTERS_OVERFLOW_POSEIDON)
	case runtime.ErrUnsupportedForkId:
		return pb.ExecutorError(EXECUTOR_ERROR_UNSUPPORTED_FORK_ID)
	case runtime.ErrBalanceMismatch:
		return pb.ExecutorError(EXECUTOR_ERROR_BALANCE_MISMATCH)
	case runtime.ErrFea2Scalar:
		return pb.ExecutorError(EXECUTOR_ERROR_FEA2SCALAR)
	case runtime.ErrTos32:
		return pb.ExecutorError(EXECUTOR_ERROR_TOS32)
	}
	return math.MaxInt32
}
