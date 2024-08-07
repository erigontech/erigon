package main

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"

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
	const testDataDir = "/Users/taratorio/erigon/polygon/heimdall/testdata/amoy"

	// --- SPANS ---
	latestSpan, err := heimdallClient.FetchLatestSpan(ctx)
	if err != nil {
		panic(err)
	}

	spanDir := fmt.Sprintf("%s/spans", testDataDir)
	lastSpanId := min(uint64(latestSpan.Id), 1280)
	for spanId := uint64(0); spanId < lastSpanId; spanId++ {
		span, err := heimdallClient.FetchSpan(ctx, spanId)
		if err != nil {
			panic(err)
		}

		bytes, err := json.MarshalIndent(span, "", "  ")
		if err != nil {
			panic(err)
		}

		filePath := fmt.Sprintf("%s/span_%d.json", spanDir, span.Id)
		err = os.WriteFile(filePath, bytes, 0644)
		if err != nil {
			panic(err)
		}
	}

	// --- CHECKPOINTS ---
	checkpointsDir := fmt.Sprintf("%s/checkpoints", testDataDir)
	var checkpoints []*heimdall.Checkpoint
	for page := uint64(1); ; page++ {
		cps, err := heimdallClient.FetchCheckpoints(ctx, page, 10_000)
		if err != nil {
			panic(err)
		}

		if len(cps) == 0 {
			break
		}

		checkpoints = append(checkpoints, cps...)
	}

	slices.SortFunc(checkpoints, func(cp1, cp2 *heimdall.Checkpoint) int {
		n1 := cp1.StartBlock().Uint64()
		n2 := cp2.StartBlock().Uint64()
		return cmp.Compare(n1, n2)
	})

	checkpoints = checkpoints[:150]
	for i, checkpoint := range checkpoints {
		checkpoint.SetRawId(uint64(i + 1))

		bytes, err := json.MarshalIndent(checkpoint, "", "  ")
		if err != nil {
			panic(err)
		}

		filePath := fmt.Sprintf("%s/checkpoint_%d.json", checkpointsDir, checkpoint.Id)
		err = os.WriteFile(filePath, bytes, 0644)
		if err != nil {
			panic(err)
		}
	}

	// --- MILESTONES ---
	milestonesDir := fmt.Sprintf("%s/milestones", testDataDir)
	milestoneCount, err := heimdallClient.FetchMilestoneCount(ctx)
	if err != nil {
		panic(err)
	}

	for milestoneId := milestoneCount - 99; milestoneId <= milestoneCount; milestoneId++ {
		milestone, err := heimdallClient.FetchMilestone(ctx, milestoneId)
		if err != nil {
			panic(err)
		}

		bytes, err := json.MarshalIndent(milestone, "", "  ")
		if err != nil {
			panic(err)
		}

		filePath := fmt.Sprintf("%s/milestone_%d.json", milestonesDir, milestone.Id)
		err = os.WriteFile(filePath, bytes, 0644)
		if err != nil {
			panic(err)
		}
	}
}
