package app

import (
	"fmt"
	"math"
	"os"
	"strings"

	"github.com/erigontech/erigon-lib/common"
)

type Trie struct {
	level int // track prefix upto this level
	root  *Node
}

type Node struct {
	child [16]*Node
	count int // number of children
	level int
}

func NewTrie(leve int) *Trie {
	return &Trie{
		level: leve,
		root:  &Node{},
	}
}

func (t *Trie) Insert(key []byte) {
	t.root.Insert(key, t.level, 0)
}

func (n *Node) Insert(key []byte, level int, nthNibble int) {
	n.count++
	if len(key) == 0 || level == 0 {
		return
	}

	nibble := getNthNibble(key, nthNibble)
	if n.child[nibble] == nil {
		n.child[nibble] = &Node{
			level: level - 1,
		}
	}

	n.child[nibble].Insert(key, level-1, nthNibble+1)
}

func (t *Trie) PrintGraphviz() string {
	var sb strings.Builder
	sb.WriteString("digraph Trie {\n")
	sb.WriteString("  rankdir=TB;\n")
	sb.WriteString("  node [shape=circle, style=filled];\n")

	nodeID := 0
	nodeMap := make(map[*Node]int)
	t.root.generateDotHelper(&sb, &nodeID, nodeMap, "root")

	sb.WriteString("}\n")
	return sb.String()
}

func (n *Node) generateDotHelper(sb *strings.Builder, nodeID *int, nodeMap map[*Node]int, label string) {
	if n == nil {
		return
	}

	// Assign ID to this node if not already assigned
	currentID := *nodeID
	nodeMap[n] = currentID
	*nodeID++

	// Color based on count (darker = higher count)
	fillColor := "lightblue"
	if n.count > 10 {
		fillColor = "orange"
	} else if n.count > 5 {
		fillColor = "yellow"
	}

	// Create node
	sb.WriteString(fmt.Sprintf("  %d [label=\"%s\\ncount:%d\\nlevel:%d\", fillcolor=%s];\n",
		currentID, label, n.count, n.level, fillColor))

	// Create edges to children
	for i := 0; i < 16; i++ {
		if n.child[i] != nil {
			childLabel := fmt.Sprintf("%x", i)
			childID := *nodeID
			n.child[i].generateDotHelper(sb, nodeID, nodeMap, childLabel)
			sb.WriteString(fmt.Sprintf("  %d -> %d [label=\"%x\"];\n", currentID, childID, i))
		}
	}
}

// Option 4: Save Graphviz to file and optionally render
func (t *Trie) SaveGraphviz(filename string) error {
	dot := t.PrintGraphviz()
	return os.WriteFile(filename, []byte(dot), 0644)
}

func getNthNibble(key []byte, n int) int {
	return int((key[n/2] >> uint((1-n%2)*4)) & 0x0f)
}

type PrefixStats struct {
	prefix     map[string]int
	prefixLen  int
	totalCount int
}

func NewPrefixStats(prefixLength int) *PrefixStats {
	if prefixLength%2 != 0 {
		panic("prefixLength must be even")
	}
	return &PrefixStats{
		prefix:    make(map[string]int),
		prefixLen: prefixLength,
	}
}

func (ps *PrefixStats) Add(data []byte) {
	key := common.Bytes2Hex(data[:ps.prefixLen/2])
	ps.prefix[key]++
	ps.totalCount++
}

func (ps *PrefixStats) Print() {
	type prefixCount struct {
		prefix string
		count  int
	}

	// Convert map to slice
	pairs := make([]prefixCount, 0, len(ps.prefix))
	for k, v := range ps.prefix {
		pairs = append(pairs, prefixCount{k, v})
	}

	// Sort by count (descending)
	// sort.Slice(pairs, func(i, j int) bool {
	// 	return pairs[i].count > pairs[j].count
	// })

	// Print sorted results
	for _, pair := range pairs {
		ratio := 100 * float64(pair.count) / float64(ps.totalCount)
		// if ratio == 0.0 {
		// 	continue

		// }
		fmt.Printf("%s: %.8f\n", pair.prefix, ratio)
	}
	fmt.Printf("\nsize: %d\n", len(ps.prefix))
	fmt.Printf("expected ration: %.8f\n", 100.0/float64(math.Pow(16, float64(ps.prefixLen))))
}
