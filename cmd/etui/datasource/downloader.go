package datasource

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
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

	mu         sync.Mutex
	activeURL  string
	candidates []string
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
	candidates := downloaderCandidates(baseURL)
	return &DownloaderPinger{
		BaseURL:    baseURL,
		Client:     &http.Client{Timeout: 10 * time.Second},
		activeURL:  baseURL,
		candidates: candidates,
	}
}

// NewDownloaderTracker creates a tracker for computing download speed/ETA.
func NewDownloaderTracker() *DownloaderTracker {
	return &DownloaderTracker{}
}

// GetTorrentsInfo fetches current torrent download progress.
func (d *DownloaderPinger) GetTorrentsInfo(ctx context.Context) (*TorrentsInfo, error) {
	candidates := d.tryOrder()
	var saw404 bool
	var sawConnect bool
	var lastErr error
	for _, baseURL := range candidates {
		info, err := d.getTorrentsInfoFrom(ctx, baseURL)
		if err == nil {
			d.setActiveURL(baseURL)
			return info, nil
		}
		lastErr = err
		msg := err.Error()
		if strings.Contains(msg, "404 Not Found") {
			saw404 = true
		}
		if strings.Contains(msg, "connect: connection refused") {
			sawConnect = true
		}
	}
	switch {
	case saw404:
		return nil, fmt.Errorf("downloader progress endpoint is not exposed by this node")
	case sawConnect:
		return nil, fmt.Errorf("downloader HTTP endpoint is unreachable")
	case lastErr != nil:
		return nil, fmt.Errorf("downloader progress probe failed: %v", lastErr)
	default:
		return nil, fmt.Errorf("downloader progress probe failed")
	}
}

func (d *DownloaderPinger) getTorrentsInfoFrom(ctx context.Context, baseURL string) (*TorrentsInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(baseURL, "/")+"/downloader/torrentsInfo", nil)
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

func (d *DownloaderPinger) tryOrder() []string {
	d.mu.Lock()
	defer d.mu.Unlock()

	if len(d.candidates) == 0 {
		return nil
	}

	out := make([]string, 0, len(d.candidates))
	seen := make(map[string]struct{}, len(d.candidates))
	if d.activeURL != "" {
		out = append(out, d.activeURL)
		seen[d.activeURL] = struct{}{}
	}
	for _, candidate := range d.candidates {
		if _, ok := seen[candidate]; ok {
			continue
		}
		out = append(out, candidate)
		seen[candidate] = struct{}{}
	}
	return out
}

func (d *DownloaderPinger) setActiveURL(baseURL string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.activeURL = baseURL
}

func downloaderCandidates(baseURL string) []string {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		return []string{"http://127.0.0.1:6060"}
	}

	candidates := []string{baseURL}
	u, err := url.Parse(baseURL)
	if err != nil {
		return candidates
	}

	host := strings.ToLower(u.Hostname())
	port := u.Port()
	if !isLoopbackHost(host) {
		return candidates
	}

	for _, altHost := range []string{"127.0.0.1", "localhost"} {
		for _, altPort := range []string{"6060", "6061"} {
			if host == altHost && port == altPort {
				continue
			}
			candidate := *u
			candidate.Host = altHost + ":" + altPort
			candidates = append(candidates, strings.TrimRight(candidate.String(), "/"))
		}
	}
	return candidates
}

func isLoopbackHost(host string) bool {
	switch host {
	case "localhost", "127.0.0.1", "::1":
		return true
	default:
		return false
	}
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
