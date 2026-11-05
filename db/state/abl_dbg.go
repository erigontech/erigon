package state

import "github.com/erigontech/erigon/common/dbg"

var (
	ablMemRLock = dbg.EnvBool("ABL_MEMRLOCK", false)
)
