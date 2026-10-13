package cache

import (
	"sync/atomic"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
)

// dbg: cap the set-associative slot ceiling to what freelru allocates for the
// same budget, and log every grow step with cumulative migration counters.
var dbgFreelruCap = dbg.EnvBool("SETASSOC_FREELRU_CAP", false)

type growStats struct {
	steps, moved, tableBytes atomic.Int64
}

func (c *GenericCache[T]) logGrow(moved, tableBytes int64) {
	steps := c.gs.steps.Add(1)
	m := c.gs.moved.Add(moved)
	tb := c.gs.tableBytes.Add(tableBytes)
	log.Info("[cache-dbg] grow", "impl", "setassoc", "cap_mb", int64(c.capacityB/datasize.MB), "freelru_cap", dbgFreelruCap,
		"steps", steps, "moved", m, "moved_bytes", m*8, "table_bytes", tb, "len", c.Len(), "slots", c.allocatedSlots(), "max_slots", c.maxCap)
}

// freelruEntry mirrors main's entry, so the cap follows main's element size.
type freelruEntry[T any] struct {
	key   []byte
	val   T
	size  int
	txNum uint64
	epoch uint32
}

// freelruCeiling is main's NewGenericCacheWithAvg slot ceiling for this budget.
func freelruCeiling[T any](capacityBytes datasize.ByteSize, payloadBytes uint32) uint32 {
	elemBytes := elemBytesFor[freelruEntry[T]]()
	perSlot := uint64(payloadBytes) + uint64(slotChargeBytes(elemBytes))
	approx := uint32(min(uint64(capacityBytes)/perSlot, maxCacheSlots))
	shards := max(initialShardCount(approx, shardCeil()), 1)
	cost := func(c uint32) int64 {
		total := c * shards
		perShardCap := (total + shards - 1) / shards
		table := int64(0)
		if perShardCap > 0 {
			table = int64(nextPow2(uint64(perShardCap)+uint64(perShardCap)/4)) * elemBytes
		}
		return int64(total)*int64(payloadBytes) + int64(shards)*(table+shardChargeBytes(elemBytes))
	}
	perShardCap := fitCeiling(approx/shards, maxCacheSlots/shards, capacityBytes, cost)
	return max(perShardCap*shards, genericCacheStartCapacity)
}

func nextPow2(n uint64) uint64 {
	p := uint64(1)
	for p < n {
		p <<= 1
	}
	return p
}
