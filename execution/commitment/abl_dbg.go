package commitment

import "github.com/erigontech/erigon/common/dbg"

var (
	ablFillLock = dbg.EnvBool("ABL_FILLLOCK", false)
	ablTailMax = dbg.EnvBool("ABL_TAILMAX", false)
)
