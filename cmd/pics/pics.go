package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"sort"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/turbo/trie"
	"github.com/ledgerwatch/erigon/visual"
)

var pic = flag.String("pic", "", "specifies picture to regenerate")

// Generate set of keys for the visualisation
func generatePrefixGroups() []string {
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

func prefixGroups1() {
	fmt.Printf("Prefix groups 1\n")
	filename := "prefix_groups_1.dot"
	f, err := os.Create(filename)
	if err != nil {
		panic(err)
	}

	keys := generatePrefixGroups()
	visual.StartGraph(f, false)
	for i, key := range keys {
		visual.QuadVertical(f, []byte(key), len(key), fmt.Sprintf("q_%x", key))
		visual.Circle(f, fmt.Sprintf("e_%d", i), fmt.Sprintf("%d", i), false)
		fmt.Fprintf(f,
			`q_%x -> e_%d;
`, key, i)
	}
	visual.EndGraph(f)
	if err := f.Close(); err != nil {
		panic(err)
	}
	//nolint:gosec
	cmd := exec.Command("dot", "-Tpng:gd", "-o"+dot2png(filename), filename)
	if output, err := cmd.CombinedOutput(); err != nil {
		fmt.Printf("error: %v, output: %s\n", err, output)
	}
}

func prefixGroups2() {
	fmt.Printf("Prefix groups 2\n")
	filename := "prefix_groups_2.dot"
	f, err := os.Create(filename)
	if err != nil {
		panic(err)
	}

	keys := generatePrefixGroups()
	sort.Strings(keys)
	visual.StartGraph(f, false)
	for i, key := range keys {
		visual.QuadVertical(f, []byte(key), len(key), fmt.Sprintf("q_%x", key))
		visual.Circle(f, fmt.Sprintf("e_%d", i), fmt.Sprintf("%d", i), false)
		fmt.Fprintf(f,
			`q_%x -> e_%d;
`, key, i)
	}
	visual.EndGraph(f)
	if err := f.Close(); err != nil {
		panic(err)
	}
	//nolint:gosec
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

func prefixGroups3() {
	fmt.Printf("Prefix groups 3\n")
	filename := "prefix_groups_3.dot"
	f, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	keys := generatePrefixGroups()
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
	visual.StartGraph(f, false)
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
		visual.Circle(f, fmt.Sprintf("e_%d", i), fmt.Sprintf("%d", i), false)
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
	//nolint:gosec
	cmd := exec.Command("dot", "-Tpng:gd", "-o"+dot2png(filename), filename)
	if output, err := cmd.CombinedOutput(); err != nil {
		fmt.Printf("error: %v, output: %s\n", err, output)
	}
}

func prefixGroups4() {
	fmt.Printf("Prefix groups 4\n")
	filename := "prefix_groups_4.dot"
	f, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	keys := generatePrefixGroups()
	sort.Strings(keys)
	tr := trie.New(libcommon.Hash{})
	var hightlights = make([][]byte, 0, len(keys))
	for i, key := range keys {
		hexKey := make([]byte, len(key)/2)
		for j := 0; j < len(hexKey); j++ {
			hexKey[j] = key[2*j+1] | (key[2*j] << 4)
		}
		vs := fmt.Sprintf("%d", i)
		tr.Update(hexKey, []byte(vs))
		hightlights = append(hightlights, []byte(key))
	}
	visual.StartGraph(f, false)
	trie.Visual(tr, f, &trie.VisualOpts{
		Highlights:  hightlights,
		IndexColors: visual.QuadIndexColors,
		FontColors:  visual.QuadFontColors,
		Values:      true,
		SameLevel:   true,
	})
	visual.EndGraph(f)
	if err := f.Close(); err != nil {
		panic(err)
	}
	//nolint:gosec
	cmd := exec.Command("dot", "-Tpng:gd", "-o"+dot2png(filename), filename)
	if output, err := cmd.CombinedOutput(); err != nil {
		fmt.Printf("error: %v, output: %s\n", err, output)
	}
}

func prefixGroups5() {
	fmt.Printf("Prefix groups 5\n")
	filename := "prefix_groups_5.dot"
	f, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	keys := generatePrefixGroups()
	sort.Strings(keys)
	tr := trie.New(libcommon.Hash{})
	var hightlights = make([][]byte, 0, len(keys))
	var folds = make([][]byte, 0, len(keys))
	for i, key := range keys {
		hexKey := make([]byte, len(key)/2)
		for j := 0; j < len(hexKey); j++ {
			hexKey[j] = key[2*j+1] | (key[2*j] << 4)
		}
		vs := fmt.Sprintf("%d", i)
		tr.Update(hexKey, []byte(vs))
		hightlights = append(hightlights, []byte(key))
		folds = append(folds, hexKey)
	}
	tr.Fold(folds[:8])
	visual.StartGraph(f, false)
	trie.Visual(tr, f, &trie.VisualOpts{
		Highlights:  hightlights,
		IndexColors: visual.QuadIndexColors,
		FontColors:  visual.QuadFontColors,
		Values:      true,
		SameLevel:   true,
	})
	visual.EndGraph(f)
	if err := f.Close(); err != nil {
		panic(err)
	}
	//nolint:gosec
	cmd := exec.Command("dot", "-Tpng:gd", "-o"+dot2png(filename), filename)
	if output, err := cmd.CombinedOutput(); err != nil {
		fmt.Printf("error: %v, output: %s\n", err, output)
	}
}

func prefixGroups6() {
	fmt.Printf("Prefix groups 6\n")
	filename := "prefix_groups_6.dot"
	f, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	keys := generatePrefixGroups()
	sort.Strings(keys)
	tr := trie.New(libcommon.Hash{})
	var hightlights = make([][]byte, 0, len(keys))
	var folds = make([][]byte, 0, len(keys))
	for i, key := range keys {
		hexKey := make([]byte, len(key)/2)
		for j := 0; j < len(hexKey); j++ {
			hexKey[j] = key[2*j+1] | (key[2*j] << 4)
		}
		vs := fmt.Sprintf("%d", i)
		tr.Update(hexKey, []byte(vs))
		hightlights = append(hightlights, []byte(key))
		folds = append(folds, hexKey)
	}
	tr.Fold(folds[:8])
	tr.Fold(folds[8:16])
	visual.StartGraph(f, false)
	trie.Visual(tr, f, &trie.VisualOpts{
		Highlights:  hightlights,
		IndexColors: visual.QuadIndexColors,
		FontColors:  visual.QuadFontColors,
		Values:      true,
		SameLevel:   true,
	})
	visual.EndGraph(f)
	if err := f.Close(); err != nil {
		panic(err)
	}
	//nolint:gosec
	cmd := exec.Command("dot", "-Tpng:gd", "-o"+dot2png(filename), filename)
	if output, err := cmd.CombinedOutput(); err != nil {
		fmt.Printf("error: %v, output: %s\n", err, output)
	}
}

func prefixGroups7() {
	fmt.Printf("Prefix groups 7\n")
	filename := "prefix_groups_7.dot"
	f, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	keys := generatePrefixGroups()
	sort.Strings(keys)
	tr := trie.New(libcommon.Hash{})
	var hightlights = make([][]byte, 0, len(keys))
	var folds = make([][]byte, 0, len(keys))
	for i, key := range keys {
		hexKey := make([]byte, len(key)/2)
		for j := 0; j < len(hexKey); j++ {
			hexKey[j] = key[2*j+1] | (key[2*j] << 4)
		}
		vs := fmt.Sprintf("%d", i)
		tr.Update(hexKey, []byte(vs))
		hightlights = append(hightlights, []byte(key))
		folds = append(folds, hexKey)
	}
	tr.Fold(folds[:8])
	tr.Fold(folds[8:16])
	tr.Fold(folds[16:24])
	tr.Fold(folds[24:])
	visual.StartGraph(f, false)
	trie.Visual(tr, f, &trie.VisualOpts{
		Highlights:  hightlights,
		IndexColors: visual.QuadIndexColors,
		FontColors:  visual.QuadFontColors,
		Values:      true,
		SameLevel:   true,
	})
	visual.EndGraph(f)
	if err := f.Close(); err != nil {
		panic(err)
	}
	//nolint:gosec
	cmd := exec.Command("dot", "-Tpng:gd", "-o"+dot2png(filename), filename)
	if output, err := cmd.CombinedOutput(); err != nil {
		fmt.Printf("error: %v, output: %s\n", err, output)
	}
}

func prefixGroups8() {
	fmt.Printf("Prefix groups 8\n")
	filename := "prefix_groups_8.dot"
	f, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	keys := generatePrefixGroups()
	sort.Strings(keys)
	tr := trie.New(libcommon.Hash{})
	var hightlights = make([][]byte, 0, len(keys))
	var folds [][]byte
	for i, key := range keys {
		hexKey := make([]byte, len(key)/2)
		for j := 0; j < len(hexKey); j++ {
			hexKey[j] = key[2*j+1] | (key[2*j] << 4)
		}
		vs := fmt.Sprintf("%d", i)
		tr.Update(hexKey, []byte(vs))
		hightlights = append(hightlights, []byte(key))
		switch i {
		case 3, 8, 22, 23:
		default:
			folds = append(folds, hexKey)
		}
	}
	tr.Fold(folds)
	visual.StartGraph(f, false)
	trie.Visual(tr, f, &trie.VisualOpts{
		Highlights:  hightlights,
		IndexColors: visual.QuadIndexColors,
		FontColors:  visual.QuadFontColors,
		Values:      true,
		SameLevel:   true,
	})
	visual.EndGraph(f)
	if err := f.Close(); err != nil {
		panic(err)
	}
	//nolint:gosec
	cmd := exec.Command("dot", "-Tpng:gd", "-o"+dot2png(filename), filename)
	if output, err := cmd.CombinedOutput(); err != nil {
		fmt.Printf("error: %v, output: %s\n", err, output)
	}
}

func main() {
	flag.Parse()
	switch *pic {
	case "prefix_groups_1":
		prefixGroups1()
	case "prefix_groups_2":
		prefixGroups2()
	case "prefix_groups_3":
		prefixGroups3()
	case "prefix_groups_4":
		prefixGroups4()
	case "prefix_groups_5":
		prefixGroups5()
	case "prefix_groups_6":
		prefixGroups6()
	case "prefix_groups_7":
		prefixGroups7()
	case "prefix_groups_8":
		prefixGroups8()
	case "initial_state_1":
		if err := initialState1(); err != nil {
			fmt.Printf("%v\n", err)
		}
	}
}
