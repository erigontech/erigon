package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/polygon/heimdall"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := log.New()
	heimdallClient := heimdall.NewHeimdallClient("https://heimdall-api.polygon.technology", logger)
	spans := make([]*heimdall.Span, 0, 10_000)
	for page := uint64(1); ; page++ {
		spansBatch, err := heimdallClient.FetchSpans(ctx, page, heimdall.SpansFetchLimit)
		if err != nil {
			panic(err)
		}

		if len(spansBatch) == 0 {
			break
		}

		spans = append(spans, spansBatch...)
	}

	sort.Slice(spans, func(i, j int) bool {
		return spans[i].Id < spans[j].Id
	})

	for _, span := range spans {
		bytes, err := json.MarshalIndent(span, "", "  ")
		if err != nil {
			panic(err)
		}

		path := fmt.Sprintf("/Users/taratorio/erigon/polygon/heimdall/testdata/mainnet/spans/span_%d.json", span.Id)
		if err = os.WriteFile(path, bytes, 0644); err != nil {
			panic(err)
		}
	}

	checkpointCount, err := heimdallClient.FetchCheckpointCount(ctx)
	if err != nil {
		panic(err)
	}

	checkpoint, err := heimdallClient.FetchCheckpoint(ctx, checkpointCount)
	if err != nil {
		panic(err)
	}

	checkpointBytes, err := json.MarshalIndent(checkpoint, "", "  ")
	if err != nil {
		panic(err)
	}

	checkpointPath := fmt.Sprintf("/Users/taratorio/erigon/polygon/heimdall/testdata/mainnet/checkpoints/checkpoint_%d.json", checkpoint.Id)
	if err = os.WriteFile(checkpointPath, checkpointBytes, 0644); err != nil {
		panic(err)
	}

	milestoneCount, err := heimdallClient.FetchMilestoneCount(ctx)
	if err != nil {
		panic(err)
	}

	milestone, err := heimdallClient.FetchMilestone(ctx, milestoneCount)
	if err != nil {
		panic(err)
	}

	milestoneBytes, err := json.MarshalIndent(milestone, "", "  ")
	if err != nil {
		panic(err)
	}

	milestonePath := fmt.Sprintf("/Users/taratorio/erigon/polygon/heimdall/testdata/mainnet/milestones/milestone_%d.json", milestone.Id)
	if err = os.WriteFile(milestonePath, milestoneBytes, 0644); err != nil {
		panic(err)
	}
}
