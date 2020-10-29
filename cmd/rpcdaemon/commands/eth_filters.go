package commands

// NewPendingTransactionFilter implements eth_newPendingTransactionFilter. Creates a pending transaction filter in the node. To check if the state has changed, call eth_getFilterChanges.
// Parameters:
//   None
// Returns:
//   QUANTITY - A filter id

// NewBlockFilter implements eth_newBlockFilter. Creates a block filter in the node, to notify when a new block arrives. To check if the state has changed, call eth_getFilterChanges.
// Parameters:
//   None
// Returns:
//   QUANTITY - A filter id

// NewFilter implements eth_newFilter. Creates an arbitrary filter object, based on filter options, to notify when the state changes (logs). To check if the state has changed, call eth_getFilterChanges.
// Parameters:
//   Object - The filter options
//   fromBlock: QUANTITY|TAG - (optional, default 'latest') Integer block number, or 'latest' for the last mined block or 'pending', 'earliest' for not yet mined transactions
//   toBlock: QUANTITY|TAG - (optional, default 'latest') Integer block number, or 'latest' for the last mined block or 'pending', 'earliest' for not yet mined transactions
//   address: DATA
//   Array of DATA, 20 Bytes - (optional) Contract address or a list of addresses from which logs should originate
//   topics: Array of DATA, - (optional) Array of 32 Bytes DATA topics. Topics are order-dependent. Each topic can also be an array of DATA with 'or' options
// Returns:
//   QUANTITY - A filter id

// UninstallFilter implements eth_uninstallFilter. Uninstalls a previously-created filter given the filter's id. Always uninstall filters when no longer needed.
// Note: Filters timeout when they are not requested with eth_getFilterChanges for a period of time.
// Parameters:
//   QUANTITY - The filter id
// Returns:
//   Boolean - true if the filter was successfully uninstalled, false otherwise

// GetFilterChanges implements eth_getFilterChanges. Polling method for a previously-created filter, which returns an array of logs which occurred since last poll.
// Parameters:
//   QUANTITY - The filter id
// Returns:
//   Array - Array of log objects, or an empty array if nothing has changed since last poll
