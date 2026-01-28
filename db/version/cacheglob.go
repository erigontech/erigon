package version

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/elastic/go-freelru"
)

var dirCache freelru.Cache[string, []string]

func init() {
	hash := func(s string) uint32 {
		return uint32(xxhash.Sum64String(s))
	}

	cache, err := freelru.NewSharded[string, []string](1024, hash)
	if err != nil {
		panic(fmt.Sprintf("failed to create dir cache: %v", err))
	}
	cache.SetLifetime(time.Second)
	dirCache = cache
}

func cachedReadDir(dir string) ([]string, error) {
	if files, ok := dirCache.Get(dir); ok {
		return files, nil
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	files := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			files = append(files, filepath.Join(dir, e.Name()))
		}
	}

	dirCache.Add(dir, files)
	return files, nil
}

func cachedGlob(pattern string) ([]string, error) {
	dir, filePattern := filepath.Split(pattern)
	if dir == "" {
		dir = "."
	} else {
		dir = filepath.Clean(dir)
	}

	files, err := cachedReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var matches []string
	for _, f := range files {
		matched, err := filepath.Match(filePattern, filepath.Base(f))
		if err != nil {
			return nil, err
		}
		if matched {
			matches = append(matches, f)
		}
	}
	return matches, nil
}
