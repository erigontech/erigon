#ifndef _SEM_
# define _SEM_

# ifdef __cplusplus
extern "C" {
# endif

#include <stddef.h>

#define ERR_TX_DATA_TOO_LONG 1

// Initialise sequence with given state root and transaction data,
// all other terms being empty
// Returns 0 if the initialisation is successful, otherwise error code (ERR_*)
int initialise(void* state_root, int tx_data_len, void* tx_data, uint64_t gas);

// Free any memory allocated during initialisation and semantic execution
void clean();

# ifdef __cplusplus
}
# endif

#endif // _SEM_