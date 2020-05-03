// EVMC: Ethereum Client-VM Connector API.
// Copyright 2018-2019 The EVMC Authors.
// Licensed under the Apache License, Version 2.0.

package evmc

/*
#cgo CFLAGS:  -I${SRCDIR}/.. -Wall -Wextra
#cgo !windows LDFLAGS: -ldl

#include <evmc/evmc.h>
#include <evmc/helpers.h>
#include <evmc/loader.h>

#include <stdlib.h>
#include <string.h>

static inline enum evmc_set_option_result set_option(struct evmc_vm* vm, char* name, char* value)
{
	enum evmc_set_option_result ret = evmc_set_option(vm, name, value);
	free(name);
	free(value);
	return ret;
}

extern const struct evmc_host_interface evmc_go_host;

static struct evmc_result execute_wrapper(struct evmc_vm* vm,
	uintptr_t context_index, enum evmc_revision rev,
	enum evmc_call_kind kind, uint32_t flags, int32_t depth, int64_t gas,
	const evmc_address* destination, const evmc_address* sender,
	const uint8_t* input_data, size_t input_size, const evmc_uint256be* value,
	const uint8_t* code, size_t code_size, const evmc_bytes32* create2_salt)
{
	struct evmc_message msg = {
		kind,
		flags,
		depth,
		gas,
		*destination,
		*sender,
		input_data,
		input_size,
		*value,
		*create2_salt,
	};

	struct evmc_host_context* context = (struct evmc_host_context*)context_index;
	return evmc_execute(vm, &evmc_go_host, context, rev, &msg, code, code_size);
}
*/
import "C"

import (
	"fmt"
	"sync"
	"unsafe"

	"github.com/ledgerwatch/turbo-geth/common"
)

// Static asserts.
const (
	_ = uint(common.HashLength - C.sizeof_evmc_bytes32) // The size of evmc_bytes32 equals the size of Hash.
	_ = uint(C.sizeof_evmc_bytes32 - common.HashLength)
	_ = uint(common.AddressLength - C.sizeof_evmc_address) // The size of evmc_address equals the size of Address.
	_ = uint(C.sizeof_evmc_address - common.AddressLength)
)

type Error int32

func (err Error) IsInternalError() bool {
	return err < 0
}

func (err Error) Error() string {
	code := C.enum_evmc_status_code(err)

	switch code {
	case C.EVMC_FAILURE:
		return "evmc: failure"
	case C.EVMC_REVERT:
		return "evmc: revert"
	case C.EVMC_OUT_OF_GAS:
		return "evmc: out of gas"
	case C.EVMC_INVALID_INSTRUCTION:
		return "evmc: invalid instruction"
	case C.EVMC_UNDEFINED_INSTRUCTION:
		return "evmc: undefined instruction"
	case C.EVMC_STACK_OVERFLOW:
		return "evmc: stack overflow"
	case C.EVMC_STACK_UNDERFLOW:
		return "evmc: stack underflow"
	case C.EVMC_BAD_JUMP_DESTINATION:
		return "evmc: bad jump destination"
	case C.EVMC_INVALID_MEMORY_ACCESS:
		return "evmc: invalid memory access"
	case C.EVMC_CALL_DEPTH_EXCEEDED:
		return "evmc: call depth exceeded"
	case C.EVMC_STATIC_MODE_VIOLATION:
		return "evmc: static mode violation"
	case C.EVMC_PRECOMPILE_FAILURE:
		return "evmc: precompile failure"
	case C.EVMC_CONTRACT_VALIDATION_FAILURE:
		return "evmc: contract validation failure"
	case C.EVMC_ARGUMENT_OUT_OF_RANGE:
		return "evmc: argument out of range"
	case C.EVMC_WASM_UNREACHABLE_INSTRUCTION:
		return "evmc: the WebAssembly unreachable instruction has been hit during execution"
	case C.EVMC_WASM_TRAP:
		return "evmc: a WebAssembly trap has been hit during execution"
	case C.EVMC_REJECTED:
		return "evmc: rejected"
	}

	if code < 0 {
		return fmt.Sprintf("evmc: internal error (%d)", int32(code))
	}

	return fmt.Sprintf("evmc: unknown non-fatal status code %d", int32(code))
}

const (
	Failure = Error(C.EVMC_FAILURE)
	Revert  = Error(C.EVMC_REVERT)
)

type Revision int32

const (
	Frontier         Revision = C.EVMC_FRONTIER
	Homestead        Revision = C.EVMC_HOMESTEAD
	TangerineWhistle Revision = C.EVMC_TANGERINE_WHISTLE
	SpuriousDragon   Revision = C.EVMC_SPURIOUS_DRAGON
	Byzantium        Revision = C.EVMC_BYZANTIUM
	Constantinople   Revision = C.EVMC_CONSTANTINOPLE
	Petersburg       Revision = C.EVMC_PETERSBURG
	Istanbul         Revision = C.EVMC_ISTANBUL
)

type VM struct {
	handle *C.struct_evmc_vm
}

func Load(filename string) (vm *VM, err error) {
	cfilename := C.CString(filename)
	var loaderErr C.enum_evmc_loader_error_code
	handle := C.evmc_load_and_create(cfilename, &loaderErr)
	C.free(unsafe.Pointer(cfilename))

	if loaderErr == C.EVMC_LOADER_SUCCESS {
		vm = &VM{handle}
	} else {
		errMsg := C.evmc_last_error_msg()
		if errMsg != nil {
			err = fmt.Errorf("EVMC loading error: %s", C.GoString(errMsg))
		} else {
			err = fmt.Errorf("EVMC loading error %d", int(loaderErr))
		}
	}

	return vm, err
}

func LoadAndConfigure(config string) (vm *VM, err error) {
	cconfig := C.CString(config)
	var loaderErr C.enum_evmc_loader_error_code
	handle := C.evmc_load_and_configure(cconfig, &loaderErr)
	C.free(unsafe.Pointer(cconfig))

	if loaderErr == C.EVMC_LOADER_SUCCESS {
		vm = &VM{handle}
	} else {
		errMsg := C.evmc_last_error_msg()
		if errMsg != nil {
			err = fmt.Errorf("EVMC loading error: %s", C.GoString(errMsg))
		} else {
			err = fmt.Errorf("EVMC loading error %d", int(loaderErr))
		}
	}

	return vm, err
}

func (vm *VM) Destroy() {
	C.evmc_destroy(vm.handle)
}

func (vm *VM) Name() string {
	// TODO: consider using C.evmc_vm_name(vm.handle)
	return C.GoString(vm.handle.name)
}

func (vm *VM) Version() string {
	// TODO: consider using C.evmc_vm_version(vm.handle)
	return C.GoString(vm.handle.version)
}

type Capability uint32

const (
	CapabilityEVM1  Capability = C.EVMC_CAPABILITY_EVM1
	CapabilityEWASM Capability = C.EVMC_CAPABILITY_EWASM
)

func (vm *VM) HasCapability(capability Capability) bool {
	return bool(C.evmc_vm_has_capability(vm.handle, uint32(capability)))
}

func (vm *VM) SetOption(name string, value string) (err error) {

	r := C.set_option(vm.handle, C.CString(name), C.CString(value))
	switch r {
	case C.EVMC_SET_OPTION_INVALID_NAME:
		err = fmt.Errorf("evmc: option '%s' not accepted", name)
	case C.EVMC_SET_OPTION_INVALID_VALUE:
		err = fmt.Errorf("evmc: option '%s' has invalid value", name)
	case C.EVMC_SET_OPTION_SUCCESS:
	}
	return err
}

func (vm *VM) Execute(ctx HostContext, rev Revision,
	kind CallKind, static bool, depth int, gas int64,
	destination common.Address, sender common.Address, input []byte, value common.Hash,
	code []byte, create2Salt common.Hash) (output []byte, gasLeft int64, err error) {

	flags := C.uint32_t(0)
	if static {
		flags |= C.EVMC_STATIC
	}

	ctxId := addHostContext(ctx)
	// FIXME: Clarify passing by pointer vs passing by value.
	evmcDestination := evmcAddress(destination)
	evmcSender := evmcAddress(sender)
	evmcValue := evmcBytes32(value)
	evmcCreate2Salt := evmcBytes32(create2Salt)
	result := C.execute_wrapper(vm.handle, C.uintptr_t(ctxId), uint32(rev),
		C.enum_evmc_call_kind(kind), flags, C.int32_t(depth), C.int64_t(gas),
		&evmcDestination, &evmcSender, bytesPtr(input), C.size_t(len(input)), &evmcValue,
		bytesPtr(code), C.size_t(len(code)), &evmcCreate2Salt)
	removeHostContext(ctxId)

	output = C.GoBytes(unsafe.Pointer(result.output_data), C.int(result.output_size))
	gasLeft = int64(result.gas_left)
	if result.status_code != C.EVMC_SUCCESS {
		err = Error(result.status_code)
	}

	if result.release != nil {
		C.evmc_release_result(&result)
	}

	return output, gasLeft, err
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
