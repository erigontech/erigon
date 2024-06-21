package simulator_test

import (
	"context"
	_ "embed"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/assert"

	"github.com/ledgerwatch/erigon/polygon/heimdall"
	"github.com/ledgerwatch/erigon/polygon/heimdall/simulator"
)

//go:embed testdata/v1-000000-000500-borevents.seg
var events []byte

//go:embed testdata/v1-000500-001000-borevents.seg
var events2 []byte

//go:embed testdata/v1-000000-000500-borspans.seg
var spans []byte

func createFiles(dataDir, chain string) error {
	destPath := filepath.Join(dataDir, "torrents", chain)
	err := os.MkdirAll(destPath, 0755)
	if err != nil {
		return err
	}

	destFile := filepath.Join(destPath, "v1-000000-000500-borevents.seg")
	err = os.WriteFile(destFile, events, 0755)
	if err != nil {
		return err
	}

	destFile = filepath.Join(destPath, "v1-000500-001000-borevents.seg")
	err = os.WriteFile(destFile, events2, 0755)
	if err != nil {
		return err
	}

	destFile = filepath.Join(destPath, "v1-000000-000500-borspans.seg")
	err = os.WriteFile(destFile, spans, 0755)
	if err != nil {
		return err
	}

	return nil
}

func setup(t *testing.T, ctx context.Context, iterations []uint64) simulator.HeimdallSimulator {
	chain := "mumbai"
	logger := log.New()
	logger.SetHandler(log.StdoutHandler)
	dataDir := t.TempDir()

	err := createFiles(dataDir, chain)
	if err != nil {
		t.Fatal(err)
	}

	sim, err := simulator.NewHeimdall(ctx, chain, dataDir, logger, iterations)
	if err != nil {
		t.Fatal(err)
	}

	return sim
}

func TestSimulatorEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sim := setup(t, ctx, []uint64{1_000_000})

	res, err := sim.FetchStateSyncEvents(ctx, 0, time.Now(), 100)
	assert.NoError(t, err)
	assert.Equal(t, 100, len(res))

	resLimit, err := sim.FetchStateSyncEvents(ctx, 0, time.Now(), 2)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(resLimit))
	assert.Equal(t, res[:2], resLimit)

	resStart, err := sim.FetchStateSyncEvents(ctx, 10, time.Now(), 5)
	assert.NoError(t, err)
	assert.Equal(t, 5, len(resStart))
	assert.Equal(t, uint64(10), resStart[0].ID)
	assert.Equal(t, res[9:14], resStart)

	lastTime := res[len(res)-1].Time
	resTime, err := sim.FetchStateSyncEvents(ctx, 0, lastTime.Add(-1*time.Second), 100)
	assert.NoError(t, err)
	assert.Equal(t, 99, len(resTime))
	assert.Equal(t, res[:len(res)-1], resTime)
}

func TestSimulatorSpans(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sim := setup(t, ctx, []uint64{100_000, 205_055})

	// should have the final span from first iteration
	span, err := sim.FetchLatestSpan(ctx)
	assert.NoError(t, err)
	assert.Equal(t, heimdall.SpanIdAt(100_000), span.Id)
	assert.Equal(t, uint64(96_256), span.StartBlock)
	assert.Equal(t, uint64(102_655), span.EndBlock)

	// get last span to move to next iteration
	span2, err := sim.FetchSpan(ctx, uint64(heimdall.SpanIdAt(100_000)))
	assert.NoError(t, err)
	assert.Equal(t, span, span2)

	// check if we are in the next iteration
	span3, err := sim.FetchLatestSpan(ctx)
	assert.NoError(t, err)
	assert.Equal(t, heimdall.SpanIdAt(205_055), span3.Id)
	assert.Equal(t, uint64(198_656), span3.StartBlock)
	assert.Equal(t, uint64(205_055), span3.EndBlock)

	// higher spans should not be available
	_, err = sim.FetchSpan(ctx, uint64(heimdall.SpanIdAt(205_055)+1))
	assert.Error(t, err, "span not found")

	// move to next iteration (should be +1 block since we have no more iterations defined)
	span4, err := sim.FetchSpan(ctx, uint64(heimdall.SpanIdAt(205_055)))
	assert.NoError(t, err)
	assert.Equal(t, span4, span3)

	// check latest span (should be the same since we are
	span5, err := sim.FetchLatestSpan(ctx)
	assert.NoError(t, err)
	assert.Equal(t, heimdall.SpanIdAt(205_056), span5.Id)
	assert.Equal(t, uint64(205_056), span5.StartBlock)
	assert.Equal(t, uint64(211_455), span5.EndBlock)
}
