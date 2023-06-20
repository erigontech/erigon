package filecache

import (
	"encoding/binary"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

type Cache[K ~string, V any] struct {
	maxEntries int

	rootPath string
	rootFs   fs.FS

	entries map[string]time.Time
	mu      sync.RWMutex
}

func NewCache[K ~string, V any](entries int, root string) (*Cache[K, V], error) {
	root = filepath.Clean(root)
	c := &Cache[K, V]{
		maxEntries: entries,
		rootPath:   root,
		entries:    map[string]time.Time{},
	}
	err := c.setup()
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Cache[K, V]) Close() error {
	err := os.RemoveAll(c.rootPath)
	if err != nil {
		return err
	}
	return nil
}

func (c *Cache[K, V]) Get(key K) (val *V, ok bool, err error) {
	val = new(V)
	// see if exists
	fpath, err := joinValidate(c.rootPath, string(key))
	if err != nil {
		return val, false, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	_, err = os.Stat(fpath)
	if err != nil {
		return val, false, nil
	}
	fp, err := os.Open(fpath)
	if err != nil {
		return val, false, nil
	}
	err = binary.Read(fp, binary.LittleEndian, val)
	if err != nil {
		return val, false, err
	}
	c.entries[fpath] = time.Now()
	ok = true
	return
}

func (c *Cache[K, V]) Add(key K, value *V) (evicted bool, err error) {
	fpath, err := joinValidate(c.rootPath, string(key))
	if err != nil {
		return false, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	fp, err := os.OpenFile(fpath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o650)
	if err != nil {
		return false, err
	}
	fp.Truncate(0)
	if err := binary.Write(fp, binary.LittleEndian, value); err != nil {
		return false, err
	}
	c.entries[fpath] = time.Now()
	evicted, err = c.clean()
	return
}

func (c *Cache[K, V]) Delete(key K) (err error) {
	fpath, err := joinValidate(c.rootPath, string(key))
	if err != nil {
		return err
	}
	return c.deleteAt(fpath)
}

func (c *Cache[K, V]) deleteAt(p string) (err error) {
	err = os.RemoveAll(p)
	if err != nil {
		return err
	}
	return
}

func (c *Cache[K, V]) setup() error {
	err := os.MkdirAll(c.rootPath, 0o750)
	if err != nil {
		return err
	}
	return nil
}
func (c *Cache[K, V]) clean() (bool, error) {
	toDelete := len(c.entries) - c.maxEntries
	if toDelete <= 0 {
		return false, nil
	}
	keys := make([]string, 0, len(c.entries))
	for k := range c.entries {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return c.entries[keys[i]].Before(c.entries[keys[j]])
	})
	for i := 0; i < toDelete; i++ {
		delete(c.entries, keys[i])
		err := c.deleteAt(keys[i])
		if err != nil {
			return true, err
		}
	}
	return true, nil
}
