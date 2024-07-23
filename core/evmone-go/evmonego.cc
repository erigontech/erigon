#include "evmonego.h"

#include <evmone/baseline.hpp>

struct evmc_result execute_wrapper(
    struct evmc_vm* vm, uintptr_t context_index, enum evmc_revision rev,
    enum evmc_call_kind kind, uint32_t flags, int32_t depth, int64_t gas,
    const evmc_address* recipient, const evmc_address* sender,
    const uint8_t* input_data, size_t input_size, const evmc_uint256be* value,
    const uint8_t* code, size_t code_size) {
    struct evmc_message msg = {
        kind,
        flags,
        depth,
        gas,
        *recipient,
        *sender,
        input_data,
        input_size,
        *value,
        {{0}},      // create2_salt: not required
                    // for execution
        {{0}},      // code_address: not required for execution
        code,       // code
        code_size,  // code_size
    };

    struct evmc_host_context* context =
        (struct evmc_host_context*)context_index;
    return vm->execute(vm, &evmc_go_host, context, rev, &msg, code, code_size);
}

struct evmc_vm* new_evmc_vm() { return evmc_create_evmone(); }
void destroy_vm(struct evmc_vm* vm) { vm->destroy(vm); }
