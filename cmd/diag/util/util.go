package util

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/ledgerwatch/erigon-lib/diagnostics"
)

func MakeHttpGetCall(ctx context.Context, url string, data interface{}) error {
	var client = &http.Client{
		Timeout: time.Second * 20,
	}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var dataaaa diagnostics.SyncStatistics
	numerr := json.Unmarshal(body, &dataaaa)
	if numerr != nil {
		return numerr
	}

	err = json.Unmarshal(body, &data)
	if err != nil {
		return err
	}

	return nil
}

func CalculateTime(amountLeft, rate uint64) string {
	if rate == 0 {
		return "999hrs:99m"
	}
	timeLeftInSeconds := amountLeft / rate

	hours := timeLeftInSeconds / 3600
	minutes := (timeLeftInSeconds / 60) % 60

	return fmt.Sprintf("%dhrs:%dm", hours, minutes)
}
