package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/ledgerwatch/erigon/zk/datastream/client"
	"github.com/ledgerwatch/erigon/zk/datastream/types"
	"github.com/nsf/jsondiff"
	"reflect"
)

var (
	stream1 = ""
	stream2 = ""

	consoleOptions = jsondiff.DefaultConsoleOptions()
)

func main() {
	flag.StringVar(&stream1, "stream1", "", "the first stream to pull data from")
	flag.StringVar(&stream2, "stream2", "", "the second stream to pull data from")
	flag.Parse()

	client1 := client.NewClient(stream1)
	client2 := client.NewClient(stream2)

	err := client1.Start()
	if err != nil {
		fmt.Printf("error: %s\n", err)
		return
	}

	err = client2.Start()
	if err != nil {
		fmt.Printf("error: %s\n", err)
		return
	}

	initialBookmark := &types.Bookmark{
		Type: types.BookmarkTypeStart,
		From: 0,
	}

	blocks1, gers1, bookmarks1, entries1, err := client1.ReadEntries(initialBookmark, 100)
	if err != nil {
		fmt.Printf("error: %s\n", err)
		return
	}

	blocks2, gers2, bookmarks2, entries2, err := client2.ReadEntries(initialBookmark, 100)
	if err != nil {
		fmt.Printf("error: %v\n", err)
	}

	b1s := *blocks1
	b2s := *blocks2

	if entries1 != entries2 {
		fmt.Printf("entries count mismatch: have: %v, want: %v\n", entries2, entries1)
	}

	if len(b1s) != len(b2s) {
		fmt.Printf("block count mismatch: have %v, want: %v\n", len(*blocks2), len(*blocks1))
	}

	if len(*gers1) != len(*gers2) {
		fmt.Printf("gers count mismatch: have %s, want: %v\n", len(*gers2), len(*gers1))
	}

	if len(bookmarks1) != len(bookmarks2) {
		fmt.Printf("bookmarks count mismatch: have: %v, want: %v\n", len(bookmarks2), len(bookmarks1))
	}

	for i := 0; i < len(b1s); i++ {
		b1 := b1s[i]
		b2 := b2s[i]
		eq := reflect.DeepEqual(b1, b2)
		if !eq {
			fmt.Printf("blocks %v not equal\n", b1.L2BlockNumber)
			b1j, _ := json.Marshal(b1)
			b2j, _ := json.Marshal(b2)
			_, report := jsondiff.Compare(b1j, b2j, &consoleOptions)
			fmt.Println(report)
		}
	}
}
