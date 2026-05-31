package state

// SetMaxCollationTxNum is a vio-restructure shim: no-op since the BAL-driven
// collation throttle is a separate landing not required for vio.
func (a *Aggregator) SetMaxCollationTxNum(n uint64) {}
