package main

import (
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/erigontech/erigon/db/seg"
)

func main() {
	kvFile := flag.String("kv", "", "path to .kv file")
	addr := flag.String("addr", "", "address to search for (hex, with or without 0x)")
	flag.Parse()

	if *kvFile == "" || *addr == "" {
		fmt.Fprintf(os.Stderr, "Usage: kv_search -kv <path-to-code.kv> -addr <hex-address>\n")
		os.Exit(1)
	}

	addrHex := strings.TrimPrefix(strings.ToLower(*addr), "0x")
	addrBytes, err := hex.DecodeString(addrHex)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid address hex: %v\n", err)
		os.Exit(1)
	}

	dec, err := seg.NewDecompressor(*kvFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open kv file: %v\n", err)
		os.Exit(1)
	}
	defer dec.Close()

	reader := seg.NewReader(dec.MakeGetter(), seg.CompressVals)
	reader.Reset(0)

	var key, val []byte
	var keyOffset, valOffset uint64
	count := 0
	for reader.HasNext() {
		key, keyOffset = reader.Next(key[:0])
		if !reader.HasNext() {
			fmt.Fprintf(os.Stderr, "key %x has no value (truncated file?)\n", key)
			break
		}
		val, valOffset = reader.Next(val[:0])
		count++

		if len(key) == len(addrBytes) && equal(key, addrBytes) {
			fmt.Printf("FOUND at entry #%d\n", count)
			fmt.Printf("  key:        %x\n", key)
			fmt.Printf("  key offset: %d\n", keyOffset)
			fmt.Printf("  val offset: %d\n", valOffset)
			fmt.Printf("  value len:  %d\n", len(val))
			fmt.Printf("  value hex:  %x\n", val)
			os.Exit(0)
		}
	}

	fmt.Printf("NOT FOUND after scanning %d entries\n", count)
	os.Exit(1)
}

func equal(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
