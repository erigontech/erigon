/*
   Copyright 2023 The Silkworm Authors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#ifndef SILKWORM_API_FUNC_H_
#define SILKWORM_API_FUNC_H_

#include "silkworm_api.h"

typedef int (*silkworm_init_func)(SilkwormHandle* handle, const struct SilkwormSettings* settings);

int call_silkworm_init_func(void* func_ptr, SilkwormHandle* handle, const struct SilkwormSettings* settings) {
    return ((silkworm_init_func)func_ptr)(handle, settings);
}

typedef int (*silkworm_add_snapshot_func)(SilkwormHandle handle, struct SilkwormChainSnapshot* snapshot);

int call_silkworm_add_snapshot_func(void* func_ptr, SilkwormHandle handle, struct SilkwormChainSnapshot* snapshot) {
    return ((silkworm_add_snapshot_func)func_ptr)(handle, snapshot);
}

typedef int (*silkworm_start_rpcdaemon_func)(SilkwormHandle handle, MDBX_env* env);

int call_silkworm_start_rpcdaemon_func(void* func_ptr, SilkwormHandle handle, MDBX_env* env) {
    return ((silkworm_start_rpcdaemon_func)func_ptr)(handle, env);
}

typedef int (*silkworm_stop_rpcdaemon_func)(SilkwormHandle handle);

int call_silkworm_stop_rpcdaemon_func(void* func_ptr, SilkwormHandle handle) {
    return ((silkworm_stop_rpcdaemon_func)func_ptr)(handle);
}

typedef int (*silkworm_sentry_start_func)(SilkwormHandle handle, const struct SilkwormSentrySettings* settings);

int call_silkworm_sentry_start_func(void* func_ptr, SilkwormHandle handle, const struct SilkwormSentrySettings* settings) {
	return ((silkworm_sentry_start_func)func_ptr)(handle, settings);
}

typedef int (*silkworm_sentry_stop_func)(SilkwormHandle handle);

int call_silkworm_sentry_stop_func(void* func_ptr, SilkwormHandle handle) {
	return ((silkworm_sentry_stop_func)func_ptr)(handle);
}

typedef int (*silkworm_execute_blocks_func)(SilkwormHandle handle, MDBX_txn* txn, uint64_t chain_id, uint64_t start_block,
    uint64_t max_block, uint64_t batch_size, bool write_change_sets, bool write_receipts, bool write_call_traces,
	uint64_t* last_executed_block, int* mdbx_error_code);

int call_silkworm_execute_blocks_func(void* func_ptr, SilkwormHandle handle, MDBX_txn* txn, uint64_t chain_id, uint64_t start_block,
	uint64_t max_block, uint64_t batch_size, bool write_change_sets, bool write_receipts, bool write_call_traces,
	uint64_t* last_executed_block, int* mdbx_error_code) {
    return ((silkworm_execute_blocks_func)func_ptr)(handle, txn, chain_id, start_block, max_block, batch_size, write_change_sets,
		write_receipts, write_call_traces, last_executed_block, mdbx_error_code);
}

typedef int (*silkworm_fini_func)(SilkwormHandle handle);

int call_silkworm_fini_func(void* func_ptr, SilkwormHandle handle) {
    return ((silkworm_fini_func)func_ptr)(handle);
}

#endif  // SILKWORM_API_FUNC_H_
