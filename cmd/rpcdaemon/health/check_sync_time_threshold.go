package health

import (
	"context"
	"fmt"
	"time"
)

func checkSyncTimeThreshold(syncTimeThreshold uint, ctx context.Context, api EthAPI) error {
	if api == nil {
		return fmt.Errorf("no connection to the Erigon server or `eth` namespace isn't enabled")
	}
	status, err := api.LastSyncInfo(ctx)
	if err != nil {
		return err
	}
	lastSyncTime, ok := status["lastSyncTime"].(uint64)
	if !ok {
		return fmt.Errorf("invalid last sync time")
	}
	tm := time.Unix(0, int64(lastSyncTime))
	if time.Since(tm).Seconds() > float64(syncTimeThreshold) {
		return fmt.Errorf("time from the last sync (%v) exceed (%v seconds)", tm.Unix(), syncTimeThreshold)
	}

	return nil
}
