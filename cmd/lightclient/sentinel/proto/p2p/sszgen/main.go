package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/ferranbt/fastssz/sszgen/version"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto/p2p/sszgen/generator"
)

func main() {
	args := os.Args[1:]

	var cmd string
	if len(args) != 0 {
		cmd = args[0]
	}
	switch cmd {
	case "version":
		fmt.Println(version.Version)
	default:
		generate()
	}
}

func generate() {
	var source string
	var objsStr string
	var output string
	var include string
	var excludeObjs string
	var suffix string

	flag.StringVar(&source, "path", "", "")
	flag.StringVar(&objsStr, "objs", "", "")
	flag.StringVar(&excludeObjs, "exclude-objs", "", "Comma-separated list of types to exclude from output")
	flag.StringVar(&output, "output", "", "")
	flag.StringVar(&include, "include", "", "")
	flag.StringVar(&suffix, "suffix", "encoding", "")

	flag.Parse()

	targets := decodeList(objsStr)
	includeList := decodeList(include)
	excludeTypeNames := make(map[string]bool)
	for _, name := range decodeList(excludeObjs) {
		excludeTypeNames[name] = true
	}

	if !strings.HasPrefix(suffix, "_") {
		suffix = fmt.Sprintf("_%s", suffix)
	}
	if !strings.HasSuffix(suffix, ".go") {
		suffix = fmt.Sprintf("%s.go", suffix)
	}

	if err := generator.Encode(source, targets, output, includeList, excludeTypeNames, suffix); err != nil {
		fmt.Printf("[ERR]: %v\n", err)
		os.Exit(1)
	}
}

func decodeList(input string) []string {
	if input == "" {
		return []string{}
	}
	return strings.Split(strings.TrimSpace(input), ",")
}
