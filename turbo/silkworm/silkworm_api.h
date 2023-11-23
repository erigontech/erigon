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

#ifndef SILKWORM_API_H_
#define SILKWORM_API_H_

// C API exported by Silkworm to be used in Erigon.

#include <stdbool.h>  // NOLINT(*-deprecated-headers)
#include <stddef.h>  // NOLINT(*-deprecated-headers)
#include <stdint.h>  // NOLINT(*-deprecated-headers)

#if defined _MSC_VER
#define SILKWORM_EXPORT __declspec(dllexport)
#else
#define SILKWORM_EXPORT __attribute__((visibility("default")))
#endif

#if __cplusplus
#define SILKWORM_NOEXCEPT noexcept
#else
#define SILKWORM_NOEXCEPT
#endif

#if __cplusplus
extern "C" {
#endif

// Silkworm library error codes (SILKWORM_OK indicates no error, i.e. success)

#define SILKWORM_OK 0
#define SILKWORM_INTERNAL_ERROR 1
#define SILKWORM_UNKNOWN_ERROR 2
#define SILKWORM_INVALID_HANDLE 3
#define SILKWORM_INVALID_PATH 4
#define SILKWORM_INVALID_SNAPSHOT 5
#define SILKWORM_INVALID_MDBX_TXN 6
#define SILKWORM_INVALID_BLOCK_RANGE 7
#define SILKWORM_BLOCK_NOT_FOUND 8
#define SILKWORM_UNKNOWN_CHAIN_ID 9
#define SILKWORM_MDBX_ERROR 10
#define SILKWORM_INVALID_BLOCK 11
#define SILKWORM_DECODING_ERROR 12
#define SILKWORM_TOO_MANY_INSTANCES 13
#define SILKWORM_INVALID_SETTINGS 14
#define SILKWORM_TERMINATION_SIGNAL 15
#define SILKWORM_SERVICE_ALREADY_STARTED 16

typedef struct MDBX_env MDBX_env;
typedef struct MDBX_txn MDBX_txn;

struct SilkwormInstance;
typedef struct SilkwormInstance* SilkwormHandle;

struct SilkwormMemoryMappedFile {
    const char* file_path;
    uint8_t* memory_address;
    uint64_t memory_length;
};

struct SilkwormHeadersSnapshot {
    struct SilkwormMemoryMappedFile segment;
    struct SilkwormMemoryMappedFile header_hash_index;
};

struct SilkwormBodiesSnapshot {
    struct SilkwormMemoryMappedFile segment;
    struct SilkwormMemoryMappedFile block_num_index;
};

struct SilkwormTransactionsSnapshot {
    struct SilkwormMemoryMappedFile segment;
    struct SilkwormMemoryMappedFile tx_hash_index;
    struct SilkwormMemoryMappedFile tx_hash_2_block_index;
};

struct SilkwormChainSnapshot {
    struct SilkwormHeadersSnapshot headers;
    struct SilkwormBodiesSnapshot bodies;
    struct SilkwormTransactionsSnapshot transactions;
};

#define SILKWORM_PATH_SIZE 260

struct SilkwormSettings {
    //! Data directory path in UTF-8.
    char data_dir_path[SILKWORM_PATH_SIZE];
};

/**
 * \brief Initialize the Silkworm C API library.
 * \param[in,out] handle Silkworm instance handle returned on successful initialization.
 * \param[in] settings General Silkworm settings.
 * \return SILKWORM_OK (=0) on success, a non-zero error value on failure.
 */
SILKWORM_EXPORT int silkworm_init(
    SilkwormHandle* handle,
    const struct SilkwormSettings* settings) SILKWORM_NOEXCEPT;

/**
 * \brief Build a set of indexes for the given snapshots.
 * \param[in] handle A valid Silkworm instance handle, got with silkworm_init.
 * \param[in] snapshots An array of snapshots to index.
 * \param[in] indexPaths An array of paths to write indexes to.
 * Note that the name of the index is a part of the path and it is used to determine the index type.
 * \param[in] len The number of snapshots and paths.
 * \return SILKWORM_OK (=0) on success, a non-zero error value on failure on some or all indexes.
 */
SILKWORM_EXPORT int silkworm_build_recsplit_indexes(SilkwormHandle handle, struct SilkwormMemoryMappedFile* snapshots[], int len) SILKWORM_NOEXCEPT;

/**
 * \brief Notify Silkworm about a new snapshot to use.
 * \param[in] handle A valid Silkworm instance handle, got with silkworm_init.
 * \param[in] snapshot A snapshot to use.
 * \return SILKWORM_OK (=0) on success, a non-zero error value on failure.
 */
SILKWORM_EXPORT int silkworm_add_snapshot(SilkwormHandle handle, struct SilkwormChainSnapshot* snapshot) SILKWORM_NOEXCEPT;

/**
 * \brief Start Silkworm RPC daemon.
 * \param[in] handle A valid Silkworm instance handle, got with silkworm_init.Must not be zero.
 * \param[in] env An valid MDBX environment. Must not be zero.
 * \return SILKWORM_OK (=0) on success, a non-zero error value on failure.
 */
SILKWORM_EXPORT int silkworm_start_rpcdaemon(SilkwormHandle handle, MDBX_env* env) SILKWORM_NOEXCEPT;

/**
 * \brief Stop Silkworm RPC daemon and wait for its termination.
 * \param[in] handle A valid Silkworm instance handle, got with silkworm_init. Must not be zero.
 * \param[in] snapshot A snapshot to use.
 * \return SILKWORM_OK (=0) on success, a non-zero error value on failure.
 */
SILKWORM_EXPORT int silkworm_stop_rpcdaemon(SilkwormHandle handle) SILKWORM_NOEXCEPT;

#define SILKWORM_SENTRY_SETTINGS_CLIENT_ID_SIZE 128
#define SILKWORM_SENTRY_SETTINGS_NAT_SIZE 50
#define SILKWORM_SENTRY_SETTINGS_NODE_KEY_SIZE 32
#define SILKWORM_SENTRY_SETTINGS_PEERS_MAX 128
#define SILKWORM_SENTRY_SETTINGS_PEER_URL_SIZE 200

struct SilkwormSentrySettings {
    char client_id[SILKWORM_SENTRY_SETTINGS_CLIENT_ID_SIZE];
    uint16_t api_port;
    uint16_t port;
    char nat[SILKWORM_SENTRY_SETTINGS_NAT_SIZE];
    uint64_t network_id;
    uint8_t node_key[SILKWORM_SENTRY_SETTINGS_NODE_KEY_SIZE];
    char static_peers[SILKWORM_SENTRY_SETTINGS_PEERS_MAX][SILKWORM_SENTRY_SETTINGS_PEER_URL_SIZE];
    char bootnodes[SILKWORM_SENTRY_SETTINGS_PEERS_MAX][SILKWORM_SENTRY_SETTINGS_PEER_URL_SIZE];
    bool no_discover;
    size_t max_peers;
};

SILKWORM_EXPORT int silkworm_sentry_start(SilkwormHandle handle, const struct SilkwormSentrySettings* settings) SILKWORM_NOEXCEPT;
SILKWORM_EXPORT int silkworm_sentry_stop(SilkwormHandle handle) SILKWORM_NOEXCEPT;

/**
 * \brief Execute a batch of blocks and write resulting changes into the database.
 * \param[in] handle A valid Silkworm instance handle, got with silkworm_init.
 * \param[in] txn A valid read-write MDBX transaction. Must not be zero.
 * This function does not commit nor abort the transaction.
 * \param[in] chain_id EIP-155 chain ID. SILKWORM_UNKNOWN_CHAIN_ID is returned in case of an unknown or unsupported chain.
 * \param[in] start_block The block height to start the execution from.
 * \param[in] max_block Do not execute after this block.
 * max_block may be executed, or the execution may stop earlier if the batch is full.
 * \param[in] batch_size The size of DB changes to accumulate before returning from this method.
 * Pass 0 if you want to execute just 1 block.
 * \param[in] write_change_sets Whether to write state changes into the DB.
 * \param[in] write_receipts Whether to write CBOR-encoded receipts into the DB.
 * \param[in] write_call_traces Whether to write call traces into the DB.
 * \param[out] last_executed_block The height of the last successfully executed block.
 * Not written to if no blocks were executed, otherwise *last_executed_block ≤ max_block.
 * \param[out] mdbx_error_code If an MDBX error occurs (this function returns kSilkwormMdbxError)
 * and mdbx_error_code isn't NULL, it's populated with the relevant MDBX error code.
 * \return SILKWORM_OK (=0) on success, a non-zero error value on failure.
 * SILKWORM_BLOCK_NOT_FOUND is probably OK: it simply means that the execution reached the end of the chain
 * (blocks up to and incl. last_executed_block were still executed).
 */
SILKWORM_EXPORT int silkworm_execute_blocks(
    SilkwormHandle handle, MDBX_txn* txn, uint64_t chain_id, uint64_t start_block, uint64_t max_block,
    uint64_t batch_size, bool write_change_sets, bool write_receipts, bool write_call_traces,
    uint64_t* last_executed_block, int* mdbx_error_code) SILKWORM_NOEXCEPT;

/**
 * \brief Finalize the Silkworm C API library.
 * \param[in] handle A valid Silkworm instance handle got with silkworm_init.
 * \return SILKWORM_OK (=0) on success, a non-zero error value on failure.
 */
SILKWORM_EXPORT int silkworm_fini(SilkwormHandle handle) SILKWORM_NOEXCEPT;

#if __cplusplus
}
#endif

#endif  // SILKWORM_API_H_
