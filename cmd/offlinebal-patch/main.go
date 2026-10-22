// Copyright 2026 The Erigon Authors
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

// offlinebal-patch writes a copy of an offline-BAL store with selected records
// replaced by those of a patch store (e.g. one written by a short
// --generate-offline-bals run), and rebuilds the index.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/erigontech/erigon/execution/bal/offlinebal"
)

func main() {
	src := flag.String("src", "", "offline-BAL store to copy")
	patch := flag.String("patch", "", "offline-BAL store with the replacement records")
	dst := flag.String("dst", "", "empty directory for the patched store")
	flag.Parse()
	if *src == "" || *patch == "" || *dst == "" {
		flag.Usage()
		os.Exit(2)
	}
	patched, err := offlinebal.Rewrite(*src, *patch, *dst)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("patched %d records\n", patched)
}
