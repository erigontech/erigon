package datasource

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// TorrentsInfo holds torrent download progress from the downloader API.
type TorrentsInfo struct {
	Total    int    `json:"total"`
	Complete int    `json:"complete"`
	Phase    string `json:"phase"`
}

// DownloaderStats holds computed download metrics for the widget.
type DownloaderStats struct {
	Total    int
	Complete int
	Phase    string

	// Computed metrics
	SpeedPerSec float64 // pieces per second (sliding average)
	ETASeconds  float64 // estimated seconds remaining (-1 if unknown)
}

// DownloaderPinger polls the Erigon downloader HTTP API for torrent progress.
type DownloaderPinger struct {
	BaseURL string
	Client  *http.Client
}

// DownloaderTracker tracks download progress over time and computes speed/ETA.
type DownloaderTracker struct {
	samples     [10]sample
	sampleIdx   int
	sampleCount int
}

type sample struct {
	complete int
	ts       time.Time
}

// NewDownloaderPinger creates a DownloaderPinger targeting the given base URL.
func NewDownloaderPinger(baseURL string) *DownloaderPinger {
	return &DownloaderPinger{
		BaseURL: baseURL,
		Client:  &http.Client{Timeout: 10 * time.Second},
	}
}

// NewDownloaderTracker creates a tracker for computing download speed/ETA.
func NewDownloaderTracker() *DownloaderTracker {
	return &DownloaderTracker{}
}

// GetTorrentsInfo fetches current torrent download progress.
func (d *DownloaderPinger) GetTorrentsInfo(ctx context.Context) (*TorrentsInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.BaseURL+"/downloader/torrentsInfo", nil)
	if err != nil {
		return nil, fmt.Errorf("create request error: %w", err)
	}

	resp, err := d.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http do error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status: %s", resp.Status)
	}

	var info TorrentsInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return nil, fmt.Errorf("decode json error: %w", err)
	}

	return &info, nil
}

// Update records a new data point and returns computed stats.
func (t *DownloaderTracker) Update(info *TorrentsInfo) DownloaderStats {
	now := time.Now()

	// Record sample in ring buffer
	t.samples[t.sampleIdx] = sample{complete: info.Complete, ts: now}
	t.sampleIdx = (t.sampleIdx + 1) % len(t.samples)
	if t.sampleCount < len(t.samples) {
		t.sampleCount++
	}

	stats := DownloaderStats{
		Total:      info.Total,
		Complete:   info.Complete,
		Phase:      info.Phase,
		ETASeconds: -1,
	}

	// Compute sliding average speed from oldest available sample
	if t.sampleCount >= 2 {
		// Oldest sample is at sampleIdx when buffer is full, otherwise at 0
		oldestIdx := 0
		if t.sampleCount == len(t.samples) {
			oldestIdx = t.sampleIdx // next to be overwritten = oldest
		}
		oldest := t.samples[oldestIdx]
		dt := now.Sub(oldest.ts).Seconds()
		if dt > 0 {
			delta := float64(info.Complete - oldest.complete)
			stats.SpeedPerSec = delta / dt
			if stats.SpeedPerSec > 0 {
				remaining := float64(info.Total - info.Complete)
				stats.ETASeconds = remaining / stats.SpeedPerSec
			}
		}
	}

	return stats
}
