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

// DownloaderPinger polls the Erigon downloader HTTP API for torrent progress.
type DownloaderPinger struct {
	BaseURL string
	Client  *http.Client
}

// NewDownloaderPinger creates a DownloaderPinger targeting the given base URL.
func NewDownloaderPinger(baseURL string) *DownloaderPinger {
	return &DownloaderPinger{
		BaseURL: baseURL,
		Client:  &http.Client{Timeout: 10 * time.Second},
	}
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
