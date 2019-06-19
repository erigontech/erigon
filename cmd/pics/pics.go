package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"sort"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/trie"
	"github.com/ledgerwatch/turbo-geth/visual"
)

var account = flag.String("pic", "", "specifies picture to regenerate")

// Generate set of keys for the visualisation
func generate_prefix_groups() []string {
	var keys []string
	for b := byte(0); b < 32; b++ {
		key := crypto.Keccak256([]byte{b})[:2]
		quad := make([]byte, len(key)*4)
		for i := 0; i < len(key); i++ {
			quad[i*4] = key[i] & 0x3
			quad[i*4+1] = (key[i] >> 2) & 0x3
			quad[i*4+2] = (key[i] >> 4) & 0x3
			quad[i*4+3] = (key[i] >> 6) & 0x3
		}
		keys = append(keys, string(quad))
	}
	return keys
}

func prefix_groups_1() {
	fmt.Printf("Prefix groups 1\n")
	filename := "prefix_groups_1.dot"
	f, err := os.Create(filename)
	if err != nil {
		panic(err)
	}

	keys := generate_prefix_groups()
	visual.StartGraph(f)
	for i, key := range keys {
		visual.QuadVertical(f, []byte(key), len(key), fmt.Sprintf("q_%x", key))
		visual.Circle(f, fmt.Sprintf("e_%d", i), fmt.Sprintf("%d", i))
		fmt.Fprintf(f,
			`q_%x -> e_%d;
`, key, i)
	}
	visual.EndGraph(f)
	if err := f.Close(); err != nil {
		panic(err)
	}
	cmd := exec.Command("dot", "-Tpng:gd", "-O", filename)
	if output, err := cmd.CombinedOutput(); err != nil {
		fmt.Printf("error: %v, output: %s\n", err, output)
	}
}

func prefix_groups_2() {
	fmt.Printf("Prefix groups 2\n")
	filename := "prefix_groups_2.dot"
	f, err := os.Create(filename)
	if err != nil {
		panic(err)
	}

	keys := generate_prefix_groups()
	sort.Strings(keys)
	visual.StartGraph(f)
	for i, key := range keys {
		visual.QuadVertical(f, []byte(key), len(key), fmt.Sprintf("q_%x", key))
		visual.Circle(f, fmt.Sprintf("e_%d", i), fmt.Sprintf("%d", i))
		fmt.Fprintf(f,
			`q_%x -> e_%d;
`, key, i)
	}
	visual.EndGraph(f)
	if err := f.Close(); err != nil {
		panic(err)
	}
	cmd := exec.Command("dot", "-Tpng:gd", "-O", filename)
	if output, err := cmd.CombinedOutput(); err != nil {
		fmt.Printf("error: %v, output: %s\n", err, output)
	}
}

func commonPrefix(s1, s2 string) int {
	l := 0
	for l < len(s1) && l < len(s2) && s1[l] == s2[l] {
		l++
	}
	return l
}

func prefix_groups_3() {
	fmt.Printf("Prefix groups 3\n")
	filename := "prefix_groups_3.dot"
	f, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	keys := generate_prefix_groups()
	sort.Strings(keys)
	// Set of all possible common prefixes
	prefixSet := make(map[string]struct{})
	for i, key := range keys {
		if i > 0 {
			leftCommon := commonPrefix(key, keys[i-1])
			if leftCommon > 0 {
				prefixSet[key[:leftCommon]] = struct{}{}
			}
		}
		if i < len(keys)-1 {
			rightCommon := commonPrefix(key, keys[i+1])
			if rightCommon > 0 {
				prefixSet[key[:rightCommon]] = struct{}{}
			}
		}
	}
	visual.StartGraph(f)
	var prefixStack []string
	for i, key := range keys {
		// Close all the groups that do not contain the current key
		var leftCommon int
		if len(prefixStack) > 0 {
			leftCommon = commonPrefix(key, prefixStack[len(prefixStack)-1])
		}
		for len(prefixStack) > 0 && len(prefixStack[len(prefixStack)-1]) > leftCommon {
			prefixStack = prefixStack[:len(prefixStack)-1]
		}
		leftCommon = 0
		if len(prefixStack) > 0 {
			leftCommon = commonPrefix(key, prefixStack[len(prefixStack)-1])
		}
		// Open new groups that contain the current key
		for p := leftCommon + 1; p < len(key); p++ {
			if _, ok := prefixSet[key[:p]]; ok {
				group := key[:p]
				visual.QuadVertical(f, []byte(group), 0, fmt.Sprintf("q_%x", group))
				if len(prefixStack) > 0 {
					fmt.Fprintf(f,
						`
		q_%x->q_%x;
		`, prefixStack[len(prefixStack)-1], group)
				}
				prefixStack = append(prefixStack, group)
			}
		}
		if len(prefixStack) > 0 {
			fmt.Fprintf(f,
				`
q_%x->q_%x;
`, prefixStack[len(prefixStack)-1], key)
		}
		// Display the key
		visual.QuadVertical(f, []byte(key), len(key), fmt.Sprintf("q_%x", key))
		visual.Circle(f, fmt.Sprintf("e_%d", i), fmt.Sprintf("%d", i))
		fmt.Fprintf(f,
			`q_%x -> e_%d;
`, key, i)
	}
	// Close remaining groups
	for len(prefixStack) > 0 {
		prefixStack = prefixStack[:len(prefixStack)-1]
	}
	fmt.Fprintf(f, "{rank = same;")
	for _, key := range keys {
		fmt.Fprintf(f, "q_%x;", key)
	}
	fmt.Fprintf(f, `};
`)
	visual.EndGraph(f)
	if err := f.Close(); err != nil {
		panic(err)
	}
	cmd := exec.Command("dot", "-Tpng:gd", "-O", filename)
	if output, err := cmd.CombinedOutput(); err != nil {
		fmt.Printf("error: %v, output: %s\n", err, output)
	}
}

func prefix_groups_4() {
	fmt.Printf("Prefix groups 4\n")
	filename := "prefix_groups_4.dot"
	f, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	keys := generate_prefix_groups()
	sort.Strings(keys)
	tr := trie.New(common.Hash{}, false)
	var hightlights [][]byte
	for i, key := range keys {
		hexKey := make([]byte, len(key)/2)
		for j := 0; j < len(hexKey); j++ {
			hexKey[j] = key[2*j+1] | (key[2*j] << 4)
		}
		vs := fmt.Sprintf("%d", i)
		tr.Update(hexKey, []byte(vs), 0)
		hightlights = append(hightlights, hexKey)
	}
	visual.StartGraph(f)
	trie.Visual(tr, hightlights, f, visual.QuadIndexColors, visual.QuadFontColors)
	visual.EndGraph(f)
	if err := f.Close(); err != nil {
		panic(err)
	}
	cmd := exec.Command("dot", "-Tpng:gd", "-O", filename)
	if output, err := cmd.CombinedOutput(); err != nil {
		fmt.Printf("error: %v, output: %s\n", err, output)
	}
}

func prefix_groups_5() {
	fmt.Printf("Prefix groups 5\n")
	filename := "prefix_groups_5.dot"
	f, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	keys := generate_prefix_groups()
	sort.Strings(keys)
	tr := trie.New(common.Hash{}, false)
	var hightlights [][]byte
	for i, key := range keys {
		hexKey := make([]byte, len(key)/2)
		for j := 0; j < len(hexKey); j++ {
			hexKey[j] = key[2*j+1] | (key[2*j] << 4)
		}
		vs := fmt.Sprintf("%d", i)
		tr.Update(hexKey, []byte(vs), 0)
		hightlights = append(hightlights, hexKey)
	}
	tr.Fold(hightlights[:8])
	visual.StartGraph(f)
	trie.Visual(tr, hightlights, f, visual.QuadIndexColors, visual.QuadFontColors)
	visual.EndGraph(f)
	if err := f.Close(); err != nil {
		panic(err)
	}
	cmd := exec.Command("dot", "-Tpng:gd", "-O", filename)
	if output, err := cmd.CombinedOutput(); err != nil {
		fmt.Printf("error: %v, output: %s\n", err, output)
	}
}

func main() {
	flag.Parse()
	switch *account {
	case "prefix_groups_1":
		prefix_groups_1()
	case "prefix_groups_2":
		prefix_groups_2()
	case "prefix_groups_3":
		prefix_groups_3()
	case "prefix_groups_4":
		prefix_groups_4()
	case "prefix_groups_5":
		prefix_groups_5()
	}
}
