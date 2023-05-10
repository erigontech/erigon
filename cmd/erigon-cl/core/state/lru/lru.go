package lru

import (
	"fmt"

	"github.com/VictoriaMetrics/metrics"
	lru "github.com/hashicorp/golang-lru/v2"
)

// Cache is a wrapper around hashicorp lru but with metric for Get
type Cache[K comparable, V any] struct {
	*lru.Cache[K, V]

	metricName string
}

func New[K comparable, V any](metricName string, size int) (*Cache[K, V], error) {
	v, err := lru.NewWithEvict[K, V](size, nil)
	if err != nil {
		return nil, err
	}
	return &Cache[K, V]{Cache: v, metricName: metricName}, nil
}

func (c *Cache[K, V]) Get(k K) (V, bool) {
	v, ok := c.Cache.Get(k)
	if ok {
		metrics.GetOrCreateCounter(fmt.Sprintf(`golang_lru_cache_hit{%s="%s"}`, "cache", c.metricName)).Inc()
	} else {
		metrics.GetOrCreateCounter(fmt.Sprintf(`golang_lru_cache_miss{%s="%s"}`, "cache", c.metricName)).Inc()
	}
	return v, ok
}
