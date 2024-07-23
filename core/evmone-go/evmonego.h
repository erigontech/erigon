#ifndef EVMONEGO_H
#define EVMONEGO_H

#include <evmc/evmc.h>
#include <evmc/helpers.h>
#include <evmc/loader.h>
#include <evmone/evmone.h>
#include <stdlib.h>
#include <string.h>

extern const struct evmc_host_interface evmc_go_host;



#ifdef __cplusplus

#else

#endif

#ifdef __cplusplus
extern "C" {
#endif

// TODO do we need extern here?
extern struct evmc_result execute_wrapper(
    struct evmc_vm* vm, uintptr_t context_index, enum evmc_revision rev,
    enum evmc_call_kind kind, uint32_t flags, int32_t depth, int64_t gas,
    const evmc_address* recipient, const evmc_address* sender,
    const uint8_t* input_data, size_t input_size, const evmc_uint256be* value,
    const uint8_t* code, size_t code_size);

extern struct evmc_vm* new_evmc_vm();
void destroy_vm(struct evmc_vm* vm);
#ifdef __cplusplus
}
#endif

#endif  // EVMONEGO_H