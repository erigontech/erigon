package common

import (
	"golang.org/x/exp/constraints"
	"golang.org/x/exp/slices"
)

func SortedKeys[K constraints.Ordered, V any](m map[K]V) []K {
	keys := make([]K, len(m))
	i := 0
	for k := range m {
		keys[i] = k
		i++
	}
	slices.Sort(keys)
	return keys
}
