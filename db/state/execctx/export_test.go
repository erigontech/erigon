package execctx

import "github.com/erigontech/erigon/db/kv"

// CodeHashForAddr exposes the unexported codeHashForAddr for tests in the
// external test package (which cannot import db/state to build a SharedDomains
// internally without an import cycle).
func (sd *SharedDomains) CodeHashForAddr(tx kv.TemporalTx, addr []byte, txNum uint64) []byte {
	return sd.codeHashForAddr(tx, addr, txNum)
}

// HasStateCache reports whether a state cache is attached (it is a no-op to
// attach when USE_STATE_CACHE=false).
func (sd *SharedDomains) HasStateCache() bool {
	return sd.stateCache != nil
}
