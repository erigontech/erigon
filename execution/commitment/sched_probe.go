package commitment

import "github.com/erigontech/erigon/common/dbg"

var (
	warmupYield = dbg.EnvBool("WARMUP_YIELD", false)
	foldYield   = dbg.EnvBool("FOLD_YIELD", false)
	fillNoLock  = dbg.EnvBool("FILL_NOLOCK", false)
)
