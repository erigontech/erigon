// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"

	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon/cmd/pics/visual"
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
		visual.Circle(f, fmt.Sprintf("e_%d", i), strconv.Itoa(i), false)
		fmt.Fprintf(f,
			`q_%x -> e_%d;
`, key, i)
	}
	visual.EndGraph(f)
	if err := f.Close(); err != nil {
		panic(err)
	}
	//nolint:gosec
	cmd := exec.CommandContext(context.Background(), "dot", "-Tpng:gd", "-o"+dot2png(filename), filename)
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
		visual.Circle(f, fmt.Sprintf("e_%d", i), strconv.Itoa(i), false)
		fmt.Fprintf(f,
			`q_%x -> e_%d;
`, key, i)
	}
	visual.EndGraph(f)
	if err := f.Close(); err != nil {
		panic(err)
	}
	//nolint:gosec
	cmd := exec.CommandContext(context.Background(), "dot", "-Tpng:gd", "-O", filename)
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
		visual.Circle(f, fmt.Sprintf("e_%d", i), strconv.Itoa(i), false)
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
	cmd := exec.CommandContext(context.Background(), "dot", "-Tpng:gd", "-o"+dot2png(filename), filename)
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
	case "initial_state_1":
		if err := initialState1(); err != nil {
			fmt.Printf("%v\n", err)
		}
	}
}
