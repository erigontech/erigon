package filters

import (
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/lru"
	"github.com/erigontech/erigon-lib/types"
)

// Config represents the configuration of the filter system.
type Config struct {
	LogCacheSize int           // maximum number of cached blocks (default: 32)
	Timeout      time.Duration // how long filters stay active (default: 5min)
}

func (cfg Config) withDefaults() Config {
	if cfg.Timeout == 0 {
		cfg.Timeout = 5 * time.Minute
	}
	if cfg.LogCacheSize == 0 {
		cfg.LogCacheSize = 32
	}
	return cfg
}

// FilterSystem holds resources shared by all filters.
type FilterSystem struct {
	backend   Backend
	logsCache *lru.Cache[common.Hash, *logCacheElem]
	cfg       *Config
}
type Backend struct{}

// NewFilterSystem creates a filter system.
func NewFilterSystem(backend Backend, config Config) *FilterSystem {
	config = config.withDefaults()
	return &FilterSystem{backend: backend, logsCache: lru.NewCache[common.Hash, *logCacheElem](config.LogCacheSize), cfg: &config}
}

type logCacheElem struct {
	logs []*types.Log
	body atomic.Value
}
