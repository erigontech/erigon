package bridge

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/stretchr/testify/require"
)

func TestEventFetchReponse(t *testing.T) {
	output := []byte(`{"event_records":[{"id":"1","contract":"0xf8e3d5a3b7d6006e21379a857108280cfe324eec","data":"AAAAAAAAAAAAAAAAINhDPcgZrwhzNqclQi30z77ylxAAAAAAAAAAAAAAAACNJo0nhdQBcOCP5UVCMv0YFePRwwAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABWvHXi1jEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAyqME=","tx_hash":"0x7fcc0fdae1aaca66fde7341314d802f09b28a0799e1b4c7f68218b08f8abfb30","log_index":"3","bor_chain_id":"2756","record_time":"2025-06-23T15:23:51.278830863Z"}]}`)

	var v StateSyncEventsResponseV2

	err := json.Unmarshal(output, &v)
	require.Nil(t, err)

	events, err := v.GetEventRecords()
	require.Nil(t, err)
	require.Len(t, events, 1)
}

func TestOver50EventBlockFetch(t *testing.T) {
	if _, ok := os.LookupEnv("ENABLE_TEST_OVER_50_EVENT_BLOCK_FETCH"); !ok {
		// this test is intended only for manual runs, since it takes a long time to finish,
		// and it uses an externally hosted service
		return
	}

	heimdallClient := NewHttpClient("https://heimdall-api.polygon.technology/", log.New())

	// block      := 48077376
	// block time := Sep-28-2023 08:13:58 AM
	events, err := heimdallClient.FetchStateSyncEvents(context.Background(), 2774290, time.Unix(1695888838-128, 0), 0)

	if err != nil {
		t.Fatal(err)
	}

	if len(events) != 113 {
		t.Fatal("Unexpected event count, exptected: 113, got:", len(events))
	}

	// block :=  23893568
	// block time := Jan-19-2022 04:48:04 AM

	events, err = heimdallClient.FetchStateSyncEvents(context.Background(), 1479540, time.Unix(1642567374, 0), 0)

	if err != nil {
		t.Fatal(err)
	}

	if len(events) != 9417 {
		t.Fatal("Unexpected event count, exptected: 9417, got:", len(events))
	}

}
