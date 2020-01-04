#include "sem.h"
#include <stdlib.h>
#include <string.h>

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
uint64_t gas_counters[MAX_TERMS];

// Actual bytes for the InputByte terms
char input_bytes[MAX_TERMS];

// Initialises the sequence with given state root and transaction data
// Returns 0 if the initialisation is successful, otherwise error code
int initialise(void* state_root, int tx_data_len, void* tx_data, uint64_t gas) {
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
