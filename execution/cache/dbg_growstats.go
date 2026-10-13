package cache

import (
	"sync/atomic"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common/log/v3"
)

// dbg: log every funded grow step with cumulative migration counters.
type growStats struct {
	steps, moved, tableBytes atomic.Int64
}

func (c *GenericCache[T]) logGrow(oldCap, newCap uint32, sign int64) {
	steps := c.gs.steps.Add(sign)
	m := c.gs.moved.Add(sign * int64(oldCap))
	tb := c.gs.tableBytes.Add(sign * slotArrayBytes(newCap, c.elemBytes))
	if sign > 0 {
		log.Info("[cache-dbg] grow", "impl", "freelru", "cap_mb", int64(c.capacityB/datasize.MB),
			"steps", steps, "moved", m, "moved_bytes", m*c.elemBytes, "table_bytes", tb, "len", c.data.Load().Len(), "slots", c.data.Load().Cap(), "max_slots", c.maxCap)
	}
}
