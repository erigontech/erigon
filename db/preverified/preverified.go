// Several types that should be usable without importing mdbx and sais, enabling cross compilation
// of tests.
package preverified

import (
	"encoding/json"
	"slices"
	"strings"

	"github.com/anacrolix/missinggo/v2/panicif"
)

// A snapshot name and hash (hex encoded) pair.
type Item struct {
	Name string
	Hash string
}

// Provides some utilities on a list of Item. It should be kept sorted.
type SortedItems []Item

func (p SortedItems) searchName(name string) (int, bool) {
	p.assertSorted()
	return slices.BinarySearchFunc(p, name, func(l Item, target string) int {
		return strings.Compare(l.Name, target)
	})
}

// Preverified.Typed was breaking sort invariance.
func (me SortedItems) assertSorted() {
	panicif.False(slices.IsSortedFunc(me, preverifiedItemCompare))
}

func preverifiedItemCompare(a, b Item) int {
	return strings.Compare(a.Name, b.Name)
}

func (me SortedItems) Get(name string) (item Item, found bool) {
	i, found := me.searchName(name)
	if found {
		item = me[i]
	}
	return
}

func (me SortedItems) Contains(name string, ignoreVersion ...bool) bool {
	if len(ignoreVersion) > 0 && ignoreVersion[0] {
		_, name, _ := strings.Cut(name, "-")
		for _, item := range me {
			_, noVersion, _ := strings.Cut(item.Name, "-")
			if noVersion == name {
				return true
			}
		}
		return false
	}
	_, found := me.searchName(name)
	return found
}

func (p SortedItems) MarshalJSON() ([]byte, error) {
	out := map[string]string{}

	for _, i := range p {
		out[i.Name] = i.Hash
	}

	return json.Marshal(out)
}

func (p *SortedItems) UnmarshalJSON(data []byte) error {
	var outMap map[string]string

	if err := json.Unmarshal(data, &outMap); err != nil {
		return err
	}

	*p = ItemsFromMap(outMap)
	return nil
}

func (p SortedItems) Sort() {
	slices.SortFunc(p, preverifiedItemCompare)
}

func ItemsFromMap(in map[string]string) (ret SortedItems) {
	out := make([]Item, 0, len(in))
	for k, v := range in {
		out = append(out, Item{k, v})
	}
	ret = out
	ret.Sort()
	return out
}
