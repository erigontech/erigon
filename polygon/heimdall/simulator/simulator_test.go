package simulator_test

import (
	"context"
	"testing"
	"time"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/polygon/heimdall/simulator"
)

func TestSimulatorStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := log.New()
	logger.SetHandler(log.StdoutHandler)
	dataDir := t.TempDir()

	sim, err := simulator.NewHeimdall(ctx, "mumbai", dataDir, logger)
	if err != nil {
		t.Fatal(err)
	}

	res, err := sim.FetchStateSyncEvents(ctx, 0, time.Now(), 1000)
	t.Log(res)
	t.Log(err)

	//res, err = sim.FetchStateSyncEvents(ctx, 0, time.Now(), 2)
	//t.Log(res)
	//t.Log(err)
	//
	//span, err := sim.FetchLatestSpan(ctx)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//t.Log(span)
	//
	//span, err = sim.FetchLatestSpan(ctx)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//t.Log(span)
	//
	//span, err = sim.FetchSpan(ctx, 0)
	//if err != nil {
	//	t.Fatal(err)
	//}
	//
	//t.Log(span)
	//
	//span, err = sim.FetchSpan(ctx, 2)
	//if err != nil {
	//	t.Fatal(err)
	//}

	//t.Log(span)
}
