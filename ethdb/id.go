package ethdb

import "sync/atomic"

var dbID = new(uint64)

func id() uint64 {
	return atomic.AddUint64(dbID, 1)
}