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

	data1, err := readFromClient(client1, initialBookmark, 5000)
	if err != nil {
		fmt.Printf("error: %v", err)
	}

	data2, err := readFromClient(client2, initialBookmark, 5000)
	if err != nil {
		fmt.Printf("error: %v", err)
	}

	for i := 0; i < len(data1); i++ {
		d1 := data1[i]
		d2 := data2[i]
		if !reflect.DeepEqual(d1, d2) {
			d1j, _ := json.Marshal(d1)
			d2j, _ := json.Marshal(d2)
			_, report := jsondiff.Compare(d1j, d2j, &consoleOptions)
			fmt.Printf("error comparing stream at index %v", i)
			fmt.Println(report)
		}
	}

	fmt.Println("test complete...")
}

func readFromClient(client *client.StreamClient, bookmark *types.Bookmark, total int) ([]interface{}, error) {
	go func() {
		err := client.ReadAllEntriesToChannel(bookmark)
		if err != nil {
			fmt.Printf("error: %v", err)
			return
		}
	}()

	data := make([]interface{}, 0)
	count := 0

LOOP:
	for {
		select {
		case d := <-client.L2BlockChan:
			data = append(data, d)
			count++
		case d := <-client.GerUpdatesChan:
			data = append(data, d)
			count++
		}

		if count == total {
			break LOOP
		}

	}

	return data, nil
}
