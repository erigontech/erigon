package widgets

import (
	"fmt"
	"strings"

	"github.com/rivo/tview"

	"github.com/erigontech/erigon/cmd/etui/datasource"
)

// DownloaderView displays download progress with text-based progress bars.
type DownloaderView struct {
	*tview.TextView
}

// NewDownloaderView creates a new downloader widget.
func NewDownloaderView() *DownloaderView {
	tv := tview.NewTextView().
		SetDynamicColors(true).
		SetText("[yellow]Downloader:[-] waiting...")
	tv.Box.SetBorder(true).SetTitle(" Downloader ")
	return &DownloaderView{TextView: tv}
}

// UpdateDownloader renders the current download stats.
func (v *DownloaderView) UpdateDownloader(stats datasource.DownloaderStats) {
	v.SetText(formatDownloaderStats(stats))
}

// SetError displays an error/retry message.
func (v *DownloaderView) SetError(msg string) {
	v.SetDynamicColors(true)
	v.SetText(fmt.Sprintf("[yellow]Downloader:[-] retrying... (%s)", msg))
}

func formatDownloaderStats(s datasource.DownloaderStats) string {
	if s.Total <= 0 {
		return "[yellow]Downloader:[-] waiting for torrents..."
	}

	if s.Complete >= s.Total {
		label := "Snapshots"
		if s.Phase != "" {
			label = s.Phase
		}
		return fmt.Sprintf("[green]%s[-] [green]%s[-] 100%% | [green]Done[-]",
			label, makeBar(25, 1.0))
	}

	pct := float64(s.Complete) / float64(s.Total)
	label := "Snapshots"
	if s.Phase != "" {
		label = s.Phase
	}

	// Speed string
	speedStr := "-- pcs/s"
	if s.SpeedPerSec > 0 {
		speedStr = fmt.Sprintf("%.0f pcs/s", s.SpeedPerSec)
	}

	// ETA string
	etaStr := "ETA: --"
	if s.ETASeconds > 0 && s.ETASeconds < 365*24*3600 { // cap at 1 year
		etaStr = "ETA: ~" + formatDuration(s.ETASeconds)
	}

	var b strings.Builder
	fmt.Fprintf(&b, "[yellow]%s[-] %s %.0f%% | %s | %s\n",
		label, makeBar(25, pct), pct*100, speedStr, etaStr)
	fmt.Fprintf(&b, "Pieces: [cyan]%d/%d[-]", s.Complete, s.Total)
	return b.String()
}

// makeBar builds a text progress bar of the given width using █ and ░.
func makeBar(width int, fraction float64) string {
	if fraction < 0 {
		fraction = 0
	}
	if fraction > 1 {
		fraction = 1
	}
	filled := int(float64(width) * fraction)
	if filled > width {
		filled = width
	}
	var b strings.Builder
	b.Grow(width * 3) // UTF-8 block chars are multi-byte
	for i := 0; i < width; i++ {
		if i < filled {
			b.WriteRune('█')
		} else {
			b.WriteRune('░')
		}
	}
	return b.String()
}

// formatDuration formats seconds into a human-readable string like "21m" or "2h 15m".
func formatDuration(seconds float64) string {
	s := int(seconds)
	if s < 60 {
		return fmt.Sprintf("%ds", s)
	}
	if s < 3600 {
		return fmt.Sprintf("%dm", s/60)
	}
	h := s / 3600
	m := (s % 3600) / 60
	if m == 0 {
		return fmt.Sprintf("%dh", h)
	}
	return fmt.Sprintf("%dh %dm", h, m)
}
