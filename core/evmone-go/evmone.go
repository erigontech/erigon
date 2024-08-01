package evmonego

/*
#cgo LDFLAGS: -L${SRCDIR}/lib -levmone -Wl,-rpath,${SRCDIR}/lib

#cgo CXXFLAGS: -std=c++20
#cgo CXXFLAGS: -I${SRCDIR}/../../build/_deps/evmone-src/include -Wall -Wextra -Wno-unused-parameter
#cgo CXXFLAGS: -I${SRCDIR}/../../build/_deps/evmone-src/lib -Wall -Wextra -Wno-unused-parameter
#cgo CXXFLAGS: -I${SRCDIR}/../../build/_deps/evmone-src/evmc/include -Wall -Wextra -Wno-unused-parameter
#cgo CXXFLAGS: -I${SRCDIR}/build/_deps/intx-src/include -Wall -Wextra -Wno-unused-parameter


#cgo CFLAGS: -I${SRCDIR}/../../build/_deps/evmone-src/include -Wall -Wextra -Wno-unused-parameter
#cgo CFLAGS: -I${SRCDIR}/../../build/_deps/evmone-src/evmc/include -Wall -Wextra -Wno-unused-parameter
// #cgo CFLAGS: -I${SRCDIR}/../../build/_deps/intx-src/include -Wall -Wextra -Wno-unused-parameter

#include "evmonego.h"


static inline enum evmc_set_option_result set_option(struct evmc_vm* h,
                                                     char* name, char* value)
                                                     {
    enum evmc_set_option_result ret = evmc_set_option(h, name, value);
    free(name);
    free(value);
    return ret;
}
*/
import "C"
import (
	"fmt"
	"sync"
	"unsafe"

	"github.com/erigontech/erigon-lib/common"
)

// Static asserts.
const (
	// The size of evmc_bytes32 equals the size of common.Hash.
	_ = uint(len(common.Hash{}) - C.sizeof_evmc_bytes32)
	_ = uint(C.sizeof_evmc_bytes32 - len(common.Hash{}))

	// The size of evmc_address equals the size of common.Address.
	_ = uint(len(common.Address{}) - C.sizeof_evmc_address)
	_ = uint(C.sizeof_evmc_address - len(common.Address{}))
)

type Error int32

func (err Error) IsInternalError() bool {
	return err < 0
}

func (err Error) Error() string {
	return C.GoString(C.evmc_status_code_to_string(C.enum_evmc_status_code(err)))
}

func errorToStatusCode(err error) Error {
	switch err.Error() {
	case "success": //"EVMC_SUCCESS:
		return C.EVMC_SUCCESS // "success";
	case "failure": // EVMC_FAILURE:
		return C.EVMC_FAILURE // "failure";
	case "revert": //EVMC_REVERT:
		return C.EVMC_REVERT // "revert";
	case "out of gas": // EVMC_OUT_OF_GAS:
		return C.EVMC_OUT_OF_GAS
	case "invalid instruction": // EVMC_INVALID_INSTRUCTION:
		return C.EVMC_INVALID_INSTRUCTION
	case "undefined instruction": // EVMC_UNDEFINED_INSTRUCTION:
		return C.EVMC_UNDEFINED_INSTRUCTION
	case "stack overflow": // EVMC_STACK_OVERFLOW:
		return C.EVMC_STACK_OVERFLOW
	case "stack underflow": // EVMC_STACK_UNDERFLOW:
		return C.EVMC_STACK_UNDERFLOW
	case "bad jump destination": // EVMC_BAD_JUMP_DESTINATION:
		return C.EVMC_BAD_JUMP_DESTINATION
	case "invalid memory access": // EVMC_INVALID_MEMORY_ACCESS:
		return C.EVMC_INVALID_MEMORY_ACCESS
	case "call depth exceeded": // EVMC_CALL_DEPTH_EXCEEDED:
		return C.EVMC_CALL_DEPTH_EXCEEDED
	case "static mode violation": // EVMC_STATIC_MODE_VIOLATION:
		return C.EVMC_STATIC_MODE_VIOLATION
	case "precompile failure": // EVMC_PRECOMPILE_FAILURE:
		return C.EVMC_PRECOMPILE_FAILURE
	case "contract validation failure": // EVMC_CONTRACT_VALIDATION_FAILURE:
		return C.EVMC_CONTRACT_VALIDATION_FAILURE
	case "argument out of range": // EVMC_ARGUMENT_OUT_OF_RANGE:
		return C.EVMC_ARGUMENT_OUT_OF_RANGE
	case "wasm unreachable instruction": // EVMC_WASM_UNREACHABLE_INSTRUCTION:
		return C.EVMC_WASM_UNREACHABLE_INSTRUCTION
	case "wasm trap": // EVMC_WASM_TRAP:
		return C.EVMC_WASM_TRAP
	case "insufficient balance": // EVMC_INSUFFICIENT_BALANCE:
		return C.EVMC_INSUFFICIENT_BALANCE
	case "internal error": // EVMC_INTERNAL_ERROR:
		return C.EVMC_INTERNAL_ERROR
	case "rejected": // EVMC_REJECTED:
		return C.EVMC_REJECTED
	case "out of memory": // EVMC_OUT_OF_MEMORY:
		return C.EVMC_OUT_OF_MEMORY
	}
	return 0
}

// func mapStatusCodeToVmErr(statusCode int32) error {
// 	switch statusCode {
// 	case C.EVMC_SUCCESS:
// 		return nil // "success";
// 	// case C.EVMC_FAILURE:
// 	case C.EVMC_REVERT:
// 		return vm.ErrExecutionReverted
// 	case C.EVMC_OUT_OF_GAS:
// 		return vm.ErrOutOfGas
// 	// case C.EVMC_INVALID_INSTRUCTION:
// 	// case C.EVMC_UNDEFINED_INSTRUCTION:
// 	case C.EVMC_STACK_OVERFLOW:
// 		return errors.New("stack overflow")
// 	case C.EVMC_STACK_UNDERFLOW:
// 		return errors.New("stack underflow")
// 	case C.EVMC_BAD_JUMP_DESTINATION:
// 		return vm.ErrInvalidJump
// 	// case C.EVMC_INVALID_MEMORY_ACCESS:
// 	case C.EVMC_CALL_DEPTH_EXCEEDED:
// 		return vm.ErrDepth
// 	// case C.EVMC_STATIC_MODE_VIOLATION:
// 	// case C.EVMC_PRECOMPILE_FAILURE:
// 	// case C.EVMC_CONTRACT_VALIDATION_FAILURE:
// 	// case C.EVMC_ARGUMENT_OUT_OF_RANGE:
// 	// case C.EVMC_WASM_UNREACHABLE_INSTRUCTION:
// 	// case C.EVMC_WASM_TRAP:
// 	// case C.EVMC_INSUFFICIENT_BALANCE:
// 	// case C.EVMC_INTERNAL_ERROR:
// 	// case C.EVMC_REJECTED:
// 	// case C.EVMC_OUT_OF_MEMORY:
// 	default:
// 		panic(fmt.Sprintf("unhandled status code: %v", statusCode))
// 	}
// }

const (
	Failure = Error(C.EVMC_FAILURE)
	Revert  = Error(C.EVMC_REVERT)
)

type Revision int32

const (
	Frontier             Revision = C.EVMC_FRONTIER
	Homestead            Revision = C.EVMC_HOMESTEAD
	TangerineWhistle     Revision = C.EVMC_TANGERINE_WHISTLE
	SpuriousDragon       Revision = C.EVMC_SPURIOUS_DRAGON
	Byzantium            Revision = C.EVMC_BYZANTIUM
	Constantinople       Revision = C.EVMC_CONSTANTINOPLE
	Petersburg           Revision = C.EVMC_PETERSBURG
	Istanbul             Revision = C.EVMC_ISTANBUL
	Berlin               Revision = C.EVMC_BERLIN
	London               Revision = C.EVMC_LONDON
	Paris                Revision = C.EVMC_PARIS
	Shanghai             Revision = C.EVMC_SHANGHAI
	Cancun               Revision = C.EVMC_CANCUN
	Prague               Revision = C.EVMC_PRAGUE
	Osaka                Revision = C.EVMC_OSAKA
	MaxRevision          Revision = C.EVMC_MAX_REVISION
	LatestStableRevision Revision = C.EVMC_LATEST_STABLE_REVISION
)

func (h *HostImpl) DestroyVM() {
	C.destroy_vm(h.handle)
}

func (h *HostImpl) Name() string {
	// TODO: consider using C.evmc_vm_name(h.handle)
	return C.GoString(h.handle.name)
}

func (h *HostImpl) Version() string {
	// TODO: consider using C.evmc_vm_version(h.handle)
	return C.GoString(h.handle.version)
}

type Capability uint32

const (
	CapabilityEVM1  Capability = C.EVMC_CAPABILITY_EVM1
	CapabilityEWASM Capability = C.EVMC_CAPABILITY_EWASM
	// CapabilityPrecompiles Capability = C.EVMC_CAPABILITY_PRECOMPILES // is not supported
)

func (h *HostImpl) HasCapability(capability Capability) bool {
	return bool(C.evmc_vm_has_capability(h.handle, uint32(capability)))
}

func (h *HostImpl) SetOption(name string, value string) (err error) {

	r := C.set_option(h.handle, C.CString(name), C.CString(value))
	switch r {
	case C.EVMC_SET_OPTION_INVALID_NAME:
		err = fmt.Errorf("evmc: option '%s' not accepted", name)
	case C.EVMC_SET_OPTION_INVALID_VALUE:
		err = fmt.Errorf("evmc: option '%s' has invalid value", name)
	case C.EVMC_SET_OPTION_SUCCESS:
	}
	return err
}

type Result struct {
	StatusCode int32
	Output     []byte
	GasLeft    int64
	GasRefund  int64
}

func (h *HostImpl) Execute(
	kind CallKind, static bool, depth int, gas int64,
	recipient common.Address, sender common.Address, input []byte, value common.Hash,
	code []byte) (res Result, err error) {

	flags := C.uint32_t(0)
	if static {
		flags |= C.EVMC_STATIC
	}

	ctxId := addHostContext(h)
	// FIXME: Clarify passing by pointer vs passing by value.
	evmcRecipient := evmcAddress(recipient)
	evmcSender := evmcAddress(sender)
	evmcValue := evmcBytes32(value)
	result := C.execute_wrapper(h.handle, C.uintptr_t(ctxId), uint32(h.rev),
		C.enum_evmc_call_kind(kind), flags, C.int32_t(depth), C.int64_t(gas),
		&evmcRecipient, &evmcSender, bytesPtr(input), C.size_t(len(input)), &evmcValue,
		bytesPtr(code), C.size_t(len(code)))
	removeHostContext(ctxId)

	res.Output = C.GoBytes(unsafe.Pointer(result.output_data), C.int(result.output_size))
	res.GasLeft = int64(result.gas_left)
	res.GasRefund = int64(result.gas_refund)
	res.StatusCode = int32(result.status_code)
	if result.status_code != C.EVMC_SUCCESS {
		err = Error(result.status_code) // TODO: map status code to error msg
	}

	if result.release != nil {
		C.evmc_release_result(&result)
	}

	return res, err
}

var (
	hostContextCounter uintptr
	hostContextMap     = map[uintptr]HostContext{}
	hostContextMapMu   sync.Mutex
)

func addHostContext(ctx HostContext) uintptr {
	hostContextMapMu.Lock()
	id := hostContextCounter
	hostContextCounter++
	hostContextMap[id] = ctx
	hostContextMapMu.Unlock()
	return id
}

func removeHostContext(id uintptr) {
	hostContextMapMu.Lock()
	delete(hostContextMap, id)
	hostContextMapMu.Unlock()
}

func getHostContext(idx uintptr) HostContext {
	hostContextMapMu.Lock()
	ctx := hostContextMap[idx]
	hostContextMapMu.Unlock()
	return ctx
}

func evmcBytes32(in common.Hash) C.evmc_bytes32 {
	out := C.evmc_bytes32{}
	for i := 0; i < len(in); i++ {
		out.bytes[i] = C.uint8_t(in[i])
	}
	return out
}

func evmcAddress(address common.Address) C.evmc_address {
	r := C.evmc_address{}
	for i := 0; i < len(address); i++ {
		r.bytes[i] = C.uint8_t(address[i])
	}
	return r
}

func bytesPtr(bytes []byte) *C.uint8_t {
	if len(bytes) == 0 {
		return nil
	}
	return (*C.uint8_t)(unsafe.Pointer(&bytes[0]))
}
