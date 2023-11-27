package cache

import (
	"time"
)

// IndiciesCache is an lru cache for validator indicies.
// I had to write a custom one because the default lru.Cache was not good enough and is prone
// to annoying bugs, if paired with Sync.Pool, it has also slow and Put times. this cache works on a few principles:
// 1. the cache is small, it contains only the last 3-6 epochs so a simple array is used for storage.
// 2. it works as a memory allocator, it allocates memory for the indicies and returns it to the caller.
// 3. it is not thread safe.
// 4. it is not a general purpose cache, it is only used for validator indicies.
// 5. it is gc friendly, it does not use sync.Pool, use only for historical reconstruction.
type IndiciesCache[K comparable] struct { // Value is established to be a []uint64 but key can be whatever
	u []struct {
		k K
		v []uint64
		t time.Time
	}
	c int
}

// NewIndiciesCache returns a new IndiciesCache with the given capacity.
func NewIndiciesCache[K comparable](capacity int) *IndiciesCache[K] {
	return &IndiciesCache[K]{c: capacity}
}

// linearSearch is a simple linear search for the key in the cache.
func (c *IndiciesCache[K]) linearSearch(key K) (int, bool) {
	for i := range c.u {
		if c.u[i].k == key {
			return i, true
		}
	}
	return 0, false
}

// Get returns the value for the key and a bool indicating if the key was found.
func (c *IndiciesCache[K]) Get(key K) ([]uint64, bool) {
	i, ok := c.linearSearch(key)
	if !ok {
		return nil, false
	}
	return c.u[i].v, true
}

// Make allocates space for n validator indicies.
func (c *IndiciesCache[K]) Make(key K, n int) []uint64 {

	// if we are at our limit, reuse the oldest entry
	if len(c.u) >= c.c {
		oldest := c.findOldest()
		if n > cap(c.u[oldest].v) {
			c.u[oldest].v = make([]uint64, n, n*2)
		}
		c.u[oldest].k = key
		c.u[oldest].v = c.u[oldest].v[:n]
		c.u[oldest].t = time.Now()
		return c.u[oldest].v
	}
	c.u = append(c.u, struct {
		k K
		v []uint64
		t time.Time
	}{k: key, v: make([]uint64, n, n*2), t: time.Now()})

	return c.u[len(c.u)-1].v
}

// findOldest returns the index of the oldest entry in the cache.
func (c *IndiciesCache[K]) findOldest() int {
	var oldest int
	for i := range c.u {
		if c.u[i].t.Before(c.u[oldest].t) {
			oldest = i
		}
	}
	return oldest
}
