package balcache

import "github.com/erigontech/erigon/common"

// CachedBlockAccessList is a vio-restructure shim: returns (nil, false) so
// callers fall through to the non-cached path. The real BAL cache lives in
// the separate BAL-driven commitment landing; not required for vio.
func CachedBlockAccessList(hash common.Hash) ([]byte, bool) {
	return nil, false
}
