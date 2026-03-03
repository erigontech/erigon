package datasource

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/flock"
)

// NodeState represents the current state of the Erigon node process.
type NodeState int

const (
	NodeStopped  NodeState = iota // no node running
	NodeRunning                   // node is running (managed or external — no distinction)
	NodeStarting                  // just spawned, waiting for LOCK to appear
	NodeStopping                  // SIGTERM sent, waiting for lock release
)

// String returns a human-readable label.
func (s NodeState) String() string {
	switch s {
	case NodeRunning:
		return "Running"
	case NodeStarting:
		return "Starting..."
	case NodeStopping:
		return "Stopping..."
	default:
		return "Stopped"
	}
}

// NodeStatus is a point-in-time snapshot of the node state, safe to pass across
// goroutine boundaries.
type NodeStatus struct {
	State   NodeState
	PID     int // >0 when Running, Starting, or Stopping
	Uptime  time.Duration
	ExitErr string // non-empty if the last run ended in error
}

// NodeManager monitors and optionally controls the Erigon node process.
// Spawned processes are fully detached (new session via Setsid) and survive
// TUI exit. All interaction with the node is via PID, not *exec.Cmd.
// It is safe for concurrent use.
type NodeManager struct {
	mu sync.Mutex

	datadir  string
	binPath  string // resolved absolute path to the erigon binary
	chain    string
	lockPath string // <datadir>/LOCK
	pidPath  string // <datadir>/etui.pid

	state     NodeState
	pid       int
	startTime time.Time
	exitErr   string
	version   int64 // bumped on every state change
}

// NewNodeManager creates a NodeManager.
// The binary path is resolved relative to the executable's directory
// (i.e. alongside the etui binary), not relative to CWD.
func NewNodeManager(datadir, chain string) *NodeManager {
	binPath := resolveErigonBinary()
	lockPath := filepath.Join(datadir, "LOCK")
	pidPath := filepath.Join(datadir, "etui.pid")
	return &NodeManager{
		datadir:  datadir,
		binPath:  binPath,
		chain:    chain,
		lockPath: lockPath,
		pidPath:  pidPath,
		state:    NodeStopped,
	}
}

// resolveErigonBinary finds the erigon binary. It first looks next to the
// running executable (the common case when etui lives in build/bin/), then
// falls back to build/bin/erigon relative to CWD.
func resolveErigonBinary() string {
	name := "erigon"
	if runtime.GOOS == "windows" {
		name = "erigon.exe"
	}
	// Try next to the running executable first.
	if exe, err := os.Executable(); err == nil {
		candidate := filepath.Join(filepath.Dir(exe), name)
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	// Fallback: CWD-relative (works when running `go run` from repo root).
	return filepath.Join(".", "build", "bin", name)
}

// Status returns a snapshot of the current node state.
func (nm *NodeManager) Status() NodeStatus {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	s := NodeStatus{
		State:   nm.state,
		PID:     nm.pid,
		ExitErr: nm.exitErr,
	}
	if nm.state == NodeRunning || nm.state == NodeStarting || nm.state == NodeStopping {
		if !nm.startTime.IsZero() {
			s.Uptime = time.Since(nm.startTime)
		}
	}
	return s
}

// Version returns a counter that increments on every state change.
func (nm *NodeManager) Version() int64 {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	return nm.version
}

// Detect checks whether an Erigon process holds the datadir lock.
// Call this periodically (every 2s) to keep the state up-to-date.
// The flock probe and PID lookup are performed without holding the mutex.
func (nm *NodeManager) Detect() {
	nm.mu.Lock()
	prevStartTime := nm.startTime
	nm.mu.Unlock()

	// Perform the flock probe and PID discovery without holding the lock —
	// both involve I/O and we must not stall other goroutines.
	locked := isLockHeld(nm.lockPath)
	pid := 0
	if locked {
		pid = findLockHolderPID(nm.lockPath)
		// Fallback: read etui.pid if /proc/locks didn't find PID.
		if pid == 0 {
			pid = nm.readPID()
		}
	}

	nm.mu.Lock()
	defer nm.mu.Unlock()

	switch {
	case locked && nm.state == NodeStarting:
		// Spawned process has taken the lock — confirm running.
		nm.state = NodeRunning
		if pid > 0 {
			nm.pid = pid
		}
		nm.version++

	case locked && nm.state == NodeStopped:
		// Node appeared (external or restarted).
		nm.state = NodeRunning
		nm.pid = pid
		nm.exitErr = ""
		nm.startTime = time.Now() // approximate
		nm.version++

	case locked && nm.state == NodeRunning:
		// Still running — update PID if changed.
		if pid > 0 && nm.pid != pid {
			nm.pid = pid
			nm.version++
		}

	case !locked && nm.state == NodeRunning:
		// Node stopped (externally or crashed).
		nm.state = NodeStopped
		nm.pid = 0
		// If we spawned the node (etui.pid exists), this is a crash —
		// extract the last error from the log file.
		if nm.HasPIDFile() {
			nm.exitErr = nm.lastLogError()
			nm.removePID()
		}
		nm.version++

	case !locked && nm.state == NodeStopping:
		// Our stop request succeeded.
		nm.state = NodeStopped
		nm.pid = 0
		nm.version++

	case !locked && nm.state == NodeStarting:
		// Node hasn't taken the lock yet — check timeout.
		if !prevStartTime.IsZero() && time.Since(prevStartTime) > 10*time.Second {
			nm.state = NodeStopped
			nm.pid = 0
			nm.exitErr = "node failed to start (LOCK not acquired within 10s)"
			nm.version++
		}
		// Otherwise still waiting — do nothing.
	}
}

// HasPIDFile returns true if an etui.pid file exists in the datadir,
// indicating that etui previously spawned the node.
func (nm *NodeManager) HasPIDFile() bool {
	_, err := os.Stat(nm.pidPath)
	return err == nil
}

// readPID reads the PID from etui.pid. Returns 0 if unreadable.
func (nm *NodeManager) readPID() int {
	data, err := os.ReadFile(nm.pidPath)
	if err != nil {
		return 0
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || pid <= 0 {
		return 0
	}
	return pid
}

// writePID writes the PID to etui.pid for reconnection after TUI restart.
func (nm *NodeManager) writePID(pid int) error {
	return os.WriteFile(nm.pidPath, []byte(strconv.Itoa(pid)), 0644)
}

func (nm *NodeManager) removePID() {
	os.Remove(nm.pidPath) //nolint:errcheck
}

// lastLogError reads the tail of the Erigon log file and extracts the last
// error or panic line. Returns a short description for display, or a generic
// "node exited unexpectedly" if no error line is found.
func (nm *NodeManager) lastLogError() string {
	logPath := filepath.Join(nm.datadir, "logs", "erigon.log")
	f, err := os.Open(logPath)
	if err != nil {
		return "node exited unexpectedly"
	}
	defer f.Close()

	// Read up to the last 8KB to find the last error line.
	const tailSize = 8192
	fi, err := f.Stat()
	if err != nil {
		return "node exited unexpectedly"
	}
	offset := fi.Size() - tailSize
	if offset < 0 {
		offset = 0
	}
	if _, err := f.Seek(offset, 0); err != nil {
		return "node exited unexpectedly"
	}
	buf := make([]byte, tailSize)
	n, _ := f.Read(buf)
	if n == 0 {
		return "node exited unexpectedly"
	}

	// Scan lines in reverse for panic or error indicators.
	lines := strings.Split(string(buf[:n]), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := lines[i]
		lower := strings.ToLower(line)
		if strings.Contains(lower, "panic") || strings.Contains(lower, "fatal") {
			msg := strings.TrimSpace(line)
			if len(msg) > 120 {
				msg = msg[:120] + "…"
			}
			return msg
		}
		if strings.Contains(lower, "err") && strings.Contains(lower, "lvl=") {
			msg := strings.TrimSpace(line)
			if len(msg) > 120 {
				msg = msg[:120] + "…"
			}
			return msg
		}
	}
	return "node exited unexpectedly"
}

// isLockHeld attempts a non-blocking flock on the datadir LOCK file.
// Returns true if the file is locked by another process.
func isLockHeld(lockPath string) bool {
	// If the lock file doesn't exist, nothing is running.
	if _, err := os.Stat(lockPath); errors.Is(err, os.ErrNotExist) {
		return false
	}

	fl := flock.New(lockPath)
	locked, err := fl.TryLock()
	if err != nil {
		// Lock attempt failed (EAGAIN etc.) — someone else holds it.
		return true
	}
	if locked {
		// We acquired it — nobody else holds it. Release immediately.
		fl.Unlock() //nolint:errcheck
		return false
	}
	// TryLock returned false without error — already locked.
	return true
}

// Start spawns the Erigon binary as a detached process.
// The node runs in its own session (Setsid) and survives TUI exit.
// Returns an error if a process is already running.
// This method performs blocking syscalls (stat, open, exec); callers on the
// tview event loop must dispatch it to a background goroutine.
func (nm *NodeManager) Start() error {
	nm.mu.Lock()

	switch nm.state {
	case NodeRunning, NodeStarting, NodeStopping:
		st, pid := nm.state, nm.pid
		nm.mu.Unlock()
		return fmt.Errorf("node already %s (PID %d)", st, pid)
	}

	nm.mu.Unlock()

	// Check if lock is held — node may be running without our knowledge.
	if isLockHeld(nm.lockPath) {
		return fmt.Errorf("datadir is locked — a node is already running")
	}

	// Resolve and verify the binary outside the lock — involves I/O.
	absPath, err := filepath.Abs(nm.binPath)
	if err != nil {
		return fmt.Errorf("resolving binary path: %w", err)
	}
	if _, err := os.Stat(absPath); err != nil {
		return fmt.Errorf("erigon binary not found at %s (run 'make erigon')", absPath)
	}

	// Prepare the log file outside the lock.
	logDir := filepath.Join(nm.datadir, "logs")
	os.MkdirAll(logDir, 0755) //nolint:errcheck
	logFile, err := os.OpenFile(
		filepath.Join(logDir, "erigon.log"),
		os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644,
	)
	if err != nil {
		return fmt.Errorf("opening log file: %w", err)
	}

	args := []string{
		"--datadir", nm.datadir,
	}
	if nm.chain != "" {
		args = append(args, "--chain", nm.chain)
	}

	cmd := exec.Command(absPath, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	// Detach: new session so the process survives TUI exit.
	spawnDetached(cmd)

	if err := cmd.Start(); err != nil {
		logFile.Close()
		return fmt.Errorf("starting erigon: %w", err)
	}

	// Write PID file for reconnection after TUI restart.
	pid := cmd.Process.Pid
	nm.writePID(pid) //nolint:errcheck

	// Release the log file — the child has inherited the fd.
	// Do NOT call cmd.Wait() — the process is fully detached.
	logFile.Close()

	nm.mu.Lock()
	nm.pid = pid
	nm.state = NodeStarting
	nm.startTime = time.Now()
	nm.exitErr = ""
	nm.version++
	nm.mu.Unlock()

	return nil
}

// Stop sends SIGTERM to the node process by PID and waits for the lock
// to be released. Falls back to SIGKILL after 30 seconds.
// This method blocks; callers on the tview event loop must dispatch it
// to a background goroutine.
func (nm *NodeManager) Stop() error {
	nm.mu.Lock()
	if nm.state != NodeRunning && nm.state != NodeStarting {
		nm.mu.Unlock()
		return fmt.Errorf("no node is running")
	}

	pid := nm.pid
	nm.state = NodeStopping
	nm.version++
	nm.mu.Unlock()

	if pid <= 0 {
		return fmt.Errorf("no PID known for the running node")
	}

	// Send SIGTERM (or platform equivalent).
	killByPID(pid)

	// Poll for lock release (up to 30s).
	deadline := time.After(30 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			// Force kill.
			forceKillByPID(pid)
			// Wait briefly for the forced exit.
			time.Sleep(2 * time.Second)
			if !isLockHeld(nm.lockPath) {
				nm.mu.Lock()
				nm.state = NodeStopped
				nm.pid = 0
				nm.version++
				nm.mu.Unlock()
				nm.removePID()
				return nil
			}
			return fmt.Errorf("node (PID %d) did not exit after SIGKILL", pid)
		case <-ticker.C:
			if !isLockHeld(nm.lockPath) {
				nm.mu.Lock()
				nm.state = NodeStopped
				nm.pid = 0
				nm.version++
				nm.mu.Unlock()
				nm.removePID()
				return nil
			}
		}
	}
}

// IsManaged returns true if the TUI spawned the running process (etui.pid exists).
func (nm *NodeManager) IsManaged() bool {
	return nm.HasPIDFile()
}
