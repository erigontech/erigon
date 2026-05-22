package datasource

import (
	"fmt"
	"sync"
	"time"
)

// AlertLevel classifies an alert's severity.
type AlertLevel int

const (
	AlertInfo AlertLevel = iota
	AlertWarn
	AlertError
)

// String returns the short label for an AlertLevel.
func (l AlertLevel) String() string {
	switch l {
	case AlertError:
		return "ERROR"
	case AlertWarn:
		return "WARN"
	default:
		return "INFO"
	}
}

// Alert represents a single alert entry.
type Alert struct {
	Level   AlertLevel
	Message string
	Time    time.Time
}

// FormattedLine returns the tview-colored display string: [LEVEL] Message HH:MM:SS
func (a Alert) FormattedLine() string {
	color := "[green]"
	switch a.Level {
	case AlertError:
		color = "[red]"
	case AlertWarn:
		color = "[yellow]"
	}
	return fmt.Sprintf("%s[%s][-] %s %s",
		color, a.Level, a.Message, a.Time.Format("15:04:05"))
}

const alertRingSize = 50

// AlertManager monitors system metrics for threshold breaches and
// maintains a ring buffer of the most recent alerts.
type AlertManager struct {
	mu      sync.Mutex
	ring    [alertRingSize]Alert
	idx     int   // next write position
	count   int   // total alerts added (capped display at alertRingSize)
	version int64 // incremented on each new alert for change detection

	// Dedup: track which conditions are currently active so we don't
	// spam the same alert on every poll cycle.
	active map[string]bool
}

// NewAlertManager creates a new AlertManager.
func NewAlertManager() *AlertManager {
	return &AlertManager{
		active: make(map[string]bool),
	}
}

// Push adds a new alert to the ring buffer.
func (am *AlertManager) Push(level AlertLevel, msg string) {
	am.mu.Lock()
	defer am.mu.Unlock()
	am.pushLocked(level, msg)
}

// pushLocked appends an alert to the ring buffer.
// Caller must hold am.mu.
func (am *AlertManager) pushLocked(level AlertLevel, msg string) {
	am.ring[am.idx] = Alert{
		Level:   level,
		Message: msg,
		Time:    time.Now(),
	}
	am.idx = (am.idx + 1) % alertRingSize
	if am.count < alertRingSize {
		am.count++
	}
	am.version++
}

// Recent returns up to n most recent alerts, newest first.
func (am *AlertManager) Recent(n int) []Alert {
	am.mu.Lock()
	defer am.mu.Unlock()

	if n > am.count {
		n = am.count
	}
	out := make([]Alert, n)
	for i := range n {
		pos := (am.idx - 1 - i + alertRingSize) % alertRingSize
		out[i] = am.ring[pos]
	}
	return out
}

// Version returns a monotonically increasing counter that changes
// whenever a new alert is pushed. Widgets can use this to avoid
// unnecessary redraws.
func (am *AlertManager) Version() int64 {
	am.mu.Lock()
	defer am.mu.Unlock()
	return am.version
}

// CheckSystemStats inspects system metrics and fires alerts when
// thresholds are breached. It also clears alerts when conditions recover.
func (am *AlertManager) CheckSystemStats(stats SystemStats) {
	// RAM > 90%
	if stats.MemTotal > 0 {
		memPct := stats.MemUsed * 100 / stats.MemTotal
		am.checkThreshold("ram_high", memPct > 90,
			AlertWarn, fmt.Sprintf("RAM usage critical: %d%%", memPct),
			AlertInfo, "RAM usage recovered to normal")
	}

	// Disk > 90%
	if stats.DiskTotal > 0 {
		diskPct := stats.DiskUsed * 100 / stats.DiskTotal
		am.checkThreshold("disk_high", diskPct > 90,
			AlertWarn, fmt.Sprintf("Disk usage critical: %d%%", diskPct),
			AlertInfo, "Disk usage recovered to normal")
	}
}

// CheckSyncMetrics inspects sync metrics for alertable conditions.
func (am *AlertManager) CheckSyncMetrics(metrics SyncMetrics) {
	// Tip lag > 30s
	am.checkThreshold("tip_lag", metrics.TipLagSeconds > 30 && metrics.TipLagSeconds >= 0,
		AlertWarn, fmt.Sprintf("Tip lag high: %ds behind chain head", metrics.TipLagSeconds),
		AlertInfo, "Tip lag recovered — node in sync")
}

// CheckDownloaderError records downloader connectivity issues.
func (am *AlertManager) CheckDownloaderError(err error) {
	if err != nil {
		am.checkThreshold("dl_unreachable", true,
			AlertError, "Downloader unreachable: "+err.Error(),
			AlertInfo, "")
	} else {
		am.checkThreshold("dl_unreachable", false,
			AlertError, "",
			AlertInfo, "Downloader connection restored")
	}
}

// ReportError pushes a one-shot error alert (no dedup).
func (am *AlertManager) ReportError(msg string) {
	am.Push(AlertError, msg)
}

// checkThreshold is the core dedup helper. When the condition transitions
// from inactive→active it fires the breach alert; when it transitions
// from active→inactive it fires the recovery alert (if non-empty).
// The entire read-check-write-push is atomic under a single lock to
// prevent concurrent goroutines from double-firing the same transition.
func (am *AlertManager) checkThreshold(
	key string, breached bool,
	breachLevel AlertLevel, breachMsg string,
	recoverLevel AlertLevel, recoverMsg string,
) {
	am.mu.Lock()
	defer am.mu.Unlock()

	wasActive := am.active[key]
	if breached && !wasActive {
		am.active[key] = true
		am.pushLocked(breachLevel, breachMsg)
	} else if !breached && wasActive {
		am.active[key] = false
		if recoverMsg != "" {
			am.pushLocked(recoverLevel, recoverMsg)
		}
	}
}
