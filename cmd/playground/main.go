package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/polygon/heimdall"
)

//
// TODO do not commit
//

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := log.New()
	heimdallClient := heimdall.NewHeimdallClient("https://heimdall-api-amoy.polygon.technology", logger)
	latestSpan, err := heimdallClient.FetchLatestSpan(ctx)
	if err != nil {
		panic(err)
	}

	const dir = "/Users/taratorio/erigon/polygon/heimdall/testdata/amoy/spans"
	for spanId := uint64(0); spanId < uint64(latestSpan.Id); spanId++ {
		span, err := heimdallClient.FetchSpan(ctx, spanId)
		if err != nil {
			panic(err)
		}

		bytes, err := json.MarshalIndent(span, "", "  ")
		if err != nil {
			panic(err)
		}

		filePath := fmt.Sprintf("%s/span_%d.json", dir, span.Id)
		err = os.WriteFile(filePath, bytes, 0644)
		if err != nil {
			panic(err)
		}
	}
}
