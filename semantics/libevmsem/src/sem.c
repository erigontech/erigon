#include "sem.h"
#include <stdlib.h>
#include <string.h>
#include <z3.h>

Z3_context ctx;
// Integer sort to act as account addresses, balances, nonces, indices in arrays, and storage keys and values
Z3_sort int_sort;
// Array sort that is used for contract storage, mapping keys (int) to values (int)
Z3_sort contract_storage_sort;
// Array sort that is used for contract code, mapping indices (program counter, int) to opcodes (int)
Z3_sort contract_code_sort;
// Account type constructor
Z3_constructor account_constructor;
// Account type
Z3_sort account_sort;
// Ethereum state type (array sort of accounts)
Z3_sort state_sort;

// Maximum number of terms in the sequence - constant for now to avoid dynamic memory allocation
#define MAX_TERMS 1024*1024

enum TermKind {
    None,
    StateHash,
    GasCounter,
    InputByte,
};

// Kind of a specific term
typedef enum TermKind term_kind_type;

// Kinds of terms in the sequence
term_kind_type term_kinds[MAX_TERMS];

// Type of state root
typedef struct {
    int depth;           // Depth of the key prefix
    char key_prefix[32]; // Key prefix
    char hash[32];       // Hash of the merkle subtree
} state_hash_type;

// Pointers to hash structures for StateHash terms
state_hash_type* state_hashes[MAX_TERMS];

// Gas counters
__uint64_t gas_counters[MAX_TERMS];

// Actual bytes for the InputByte terms
char input_bytes[MAX_TERMS];

// Create z3 context and necessary sorts and datatypes
void init() {
    Z3_config cfg;
    cfg = Z3_mk_config();
    ctx = Z3_mk_context(cfg);
    Z3_del_config(cfg);
    int_sort = Z3_mk_int_sort(ctx);
    contract_storage_sort = Z3_mk_array_sort(ctx, int_sort, int_sort);
    contract_code_sort = Z3_mk_array_sort(ctx, int_sort, int_sort);
    Z3_symbol account_fields[5] = {
        Z3_mk_string_symbol(ctx, "balance"),
        Z3_mk_string_symbol(ctx, "nonce"),
        Z3_mk_string_symbol(ctx, "codelen"),
        Z3_mk_string_symbol(ctx, "code"),
        Z3_mk_string_symbol(ctx, "storage"),
    };
    Z3_sort account_field_sorts[5] = {
        int_sort,              // balance
        int_sort,              // nonce
        int_sort,              // codelen
        contract_code_sort,    // code
        contract_storage_sort, // storage
    };
    unsigned int account_sort_refs[5] = { 0, 0, 0, 0, 0 }; // There are no recursive datatypes, therefore all zeroes
    account_constructor = Z3_mk_constructor(ctx,
        Z3_mk_string_symbol(ctx, "account_const"), // name of the constructor
        Z3_mk_string_symbol(ctx, "is_account"),    // name of the recognizer function
        5,                                         // number of fields
        account_fields,                            // field symbols
        account_field_sorts,                       // field sorts
        account_sort_refs                          // sort references
    );
    Z3_constructor account_constructors[1];
    account_constructors[0] = account_constructor;
    account_sort = Z3_mk_datatype(ctx, Z3_mk_string_symbol(ctx, "account"), 1, account_constructors);
    // state is the sort of accounts
    state_sort = Z3_mk_array_sort(ctx, int_sort, account_sort);
}

// Initialises the sequence with given state root and transaction data
// Returns 0 if the initialisation is successful, otherwise error code
int initialise(void* state_root, void *from_address, void *to_address, __uint128_t value, int tx_data_len, void* tx_data, __uint64_t gas_price, __uint64_t gas) {
    if (2 + tx_data_len > MAX_TERMS) {
        // First term is to state root
        // Second term is for gas counter
        return ERR_TX_DATA_TOO_LONG;
    }

    // Initialise state root
    term_kinds[0] = StateHash;
    state_hash_type* root = (state_hash_type*)(malloc(sizeof(state_hash_type)));
    root -> depth = 0; // depth 0 means root hash
    // key prefix can contain anything, it does not matter
    memcpy(root -> hash, state_root, 32); // copy state root so that input can be freed
    state_hashes[0] = root;

    // Initialise gas counter
    term_kinds[1] = GasCounter;
    gas_counters[1] = gas;

    // Initialise transaction input bytes
    const int tx_data_offset = 2;
    char* tx_bytes = (char*)tx_data;
    for(int i = tx_data_offset; i < tx_data_len+tx_data_offset; i++) {
        term_kinds[i] = InputByte;
        input_bytes[i] = tx_bytes[i-tx_data_offset];
    }

    // Fill the rest of sequence with empties
    for(int i = tx_data_len+tx_data_offset; i < MAX_TERMS; i++) {
        term_kinds[i] = None;
    }
    return 0;
}

// Free any memory allocated during initialisation and semantic execution
void cleanup() {
    for(int i = 0; i < MAX_TERMS; i++) {
        switch (term_kinds[i]) {
            case None:
                break;
            case StateHash:
                free(state_hashes[i]);
                break;
            case GasCounter:
                break;
            case InputByte:
                break;
        }
    }
}

void destroy() {
    Z3_del_context(ctx);
}