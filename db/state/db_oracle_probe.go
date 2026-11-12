package state

import (
	"strings"
	"sync"
	"time"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

// Bound probe DB_ORACLE=commitment|state|all: a domain read skips the MDBX seek
// for a key the in-heap set of DB keys does not hold. The set is built by one
// table scan on first use and extended by every DomainBufferedWriter.Flush.
var dbOracleScope = dbg.EnvString("DB_ORACLE", "")

type dbKeyOracle struct {
	once  sync.Once
	mu    sync.RWMutex
	keys  map[string]struct{}
	ready bool
}

var dbOracles [kv.DomainLen]dbKeyOracle

func dbOracleOn(d kv.Domain) bool {
	switch dbOracleScope {
	case "all":
		return d == kv.CommitmentDomain || d == kv.AccountsDomain || d == kv.StorageDomain || d == kv.CodeDomain
	case "commitment":
		return d == kv.CommitmentDomain
	case "state":
		return d == kv.AccountsDomain || d == kv.StorageDomain || d == kv.CodeDomain
	}
	return false
}

// dbOracleSkip reports that key cannot be in the domain's DB table.
func (dt *DomainRoTx) dbOracleSkip(key []byte, roTx kv.Tx) bool {
	if dbOracleScope == "" || !dbOracleOn(dt.d.Name) {
		return false
	}
	o := &dbOracles[dt.d.Name]
	o.once.Do(func() { o.build(dt.d, roTx) })
	o.mu.RLock()
	defer o.mu.RUnlock()
	if !o.ready {
		return false
	}
	_, ok := o.keys[string(key)]
	return !ok
}

func (o *dbKeyOracle) build(d *Domain, roTx kv.Tx) {
	t0 := time.Now()
	keys := map[string]struct{}{}
	var c kv.Cursor
	var next func() ([]byte, []byte, error)
	var err error
	if d.LargeValues {
		c, err = roTx.Cursor(d.ValuesTable)
		if err == nil {
			next = c.Next
		}
	} else {
		var dc kv.CursorDupSort
		dc, err = roTx.CursorDupSort(d.ValuesTable)
		c = dc
		if err == nil {
			next = dc.NextNoDup
		}
	}
	if err != nil {
		log.Warn("[db-oracle] cursor", "domain", d.Name, "err", err)
		return
	}
	defer c.Close()
	for k, _, err := c.First(); k != nil; k, _, err = next() {
		if err != nil {
			log.Warn("[db-oracle] scan", "domain", d.Name, "err", err)
			return
		}
		if d.LargeValues {
			k = k[:len(k)-8]
		}
		keys[string(k)] = struct{}{}
	}
	o.mu.Lock()
	for k := range o.keys { // flushed before the scan's view was taken
		keys[k] = struct{}{}
	}
	o.keys, o.ready = keys, true
	o.mu.Unlock()
	log.Info("[db-oracle] built", "domain", d.Name, "keys", len(keys), "took", time.Since(t0))
}

// dbOracleAdd records a key flushed to the DB table; fullKey carries the step suffix for large values.
func dbOracleAdd(d kv.Domain, fullKey []byte, largeVals bool) {
	if dbOracleScope == "" || !dbOracleOn(d) {
		return
	}
	k := fullKey
	if largeVals {
		k = k[:len(k)-8]
	}
	o := &dbOracles[d]
	o.mu.Lock()
	if o.keys == nil {
		o.keys = map[string]struct{}{}
	}
	o.keys[strings.Clone(string(k))] = struct{}{}
	o.mu.Unlock()
}
