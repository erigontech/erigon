package state

import (
	"fmt"
	"sync"

	"github.com/elastic/go-freelru"
	"github.com/erigontech/erigon-lib/log/v3"
	btree2 "github.com/tidwall/btree"
)

// files seek cache

type FilesSeekCache[V any] struct {
	*freelru.LRU[uint64, V]

	hit, total, limit int
	enabled, trace    bool
}

func NewFilesSeekCache[V any](limit uint32, enabled bool, trace bool,
	hash freelru.HashKeyCallback[uint64]) *FilesSeekCache[V] {
	c, err := freelru.New[uint64, V](limit, hash)
	if err != nil {
		panic(err)
	}
	return &FilesSeekCache[V]{LRU: c, enabled: enabled, trace: trace}
}

// caching files registry

type CachingFilesRegistry[V any] struct {
	*FilesRegistry
	enabled bool
	caches  *sync.Pool
}

func NewCachingFilesRegistry[V any](dir string, limit uint32, enabled bool, trace bool,
	hash freelru.HashKeyCallback[uint64]) *CachingFilesRegistry[V] {
	f := &CachingFilesRegistry[V]{
		FilesRegistry: &FilesRegistry{
			dirtyFiles: btree2.NewBTreeGOptions(filesItemLess, btree2.Options{Degree: 128, NoLocks: false}),
			dir:        dir,
		},
		enabled: enabled,
		caches:  &sync.Pool{New: func() any { return NewFilesSeekCache[V](limit, enabled, trace, hash) }},
	}
	//f.files = f.newVisibleFiles()
	return f
}

func (c *FilesSeekCache[V]) LogStats(fileBaseName string) {
	if c == nil || !c.trace {
		return
	}
	m := c.Metrics()
	log.Warn("[dbg] FilesSeekCache", "a", fileBaseName, "ratio",
		fmt.Sprintf("%.2f", float64(c.hit)/float64(c.total)), "hit", c.hit, "collisions", m.Collisions,
		"evictions", m.Evictions, "inserts", m.Inserts, "removals", m.Removals, "limit", c.limit)
}
