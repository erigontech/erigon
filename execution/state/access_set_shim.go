package state

import "github.com/erigontech/erigon/execution/types/accounts"

// AccessSet is an API-compatibility shim for the typed-vio refactor. In the
// pre-vio world, IBS tracked accessed addresses in a dedicated map. In typed
// vio, access tracking folds into the ReadSet (one entry per address-level
// touch). Consumers that still ask for the legacy AccessSet receive an
// empty map; readers should migrate to ReadSet inspection.
type AccessSet map[accounts.Address]struct{}

// AccessedAddresses returns nil — access tracking moved into ReadSet under
// the typed-vio shape. Kept for back-compat with txtask.AccessedAddresses
// and block_assembler.clearTxIO call sites.
func (sdb *IntraBlockState) AccessedAddresses() AccessSet { return nil }
