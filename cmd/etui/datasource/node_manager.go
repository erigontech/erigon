package datasource

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/gofrs/flock"
)

// NodeState represents the current state of the Erigon node process.
type NodeState int

const (
	NodeStopped  NodeState = iota // no node running
	NodeRunning                   // child process managed by this TUI
	NodeExternal                  // node running externally (detected via lock file)
	NodeStarting                  // child process recently spawned, not yet confirmed
	NodeStopping                  // SIGTERM sent, waiting for exit
)

// String returns a human-readable label.
func (s NodeState) String() string {
	switch s {
	case NodeRunning:
		return "Running"
	case NodeExternal:
		return "Running (external)"
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
	PID     int // >0 when Running, Stopping, or External (discovered via /proc/locks)
	Uptime  time.Duration
	ExitErr string // non-empty if the last run ended in error
}

// NodeManager manages the lifecycle of an Erigon child process.
// It is safe for concurrent use.
type NodeManager struct {
	mu sync.Mutex

	datadir  string
	binPath  string // resolved absolute path to the erigon binary
	chain    string
	lockPath string // <datadir>/LOCK

	state     NodeState
	cmd       *exec.Cmd
	pid       int
	startTime time.Time
	exitErr   string
	version   int64 // bumped on every state change

	// done is closed when the child process exits. Re-created on each Start.
	done chan struct{}
}

// NewNodeManager creates a NodeManager.
// The binary path is resolved relative to the executable's directory
// (i.e. alongside the etui binary), not relative to CWD.
func NewNodeManager(datadir, chain string) *NodeManager {
	binPath := resolveErigonBinary()
	lockPath := filepath.Join(datadir, "LOCK")
	return &NodeManager{
		datadir:  datadir,
		binPath:  binPath,
		chain:    chain,
		lockPath: lockPath,
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

// DetectExternal checks whether an external Erigon process holds the datadir
// lock. Call this periodically to keep the state up-to-date.
// The flock probe and PID lookup are performed without holding the mutex.
func (nm *NodeManager) DetectExternal() {
	nm.mu.Lock()
	// Only probe when we don't own a child process ourselves.
	if nm.state == NodeRunning || nm.state == NodeStarting || nm.state == NodeStopping {
		nm.mu.Unlock()
		return
	}
	prevState := nm.state
	nm.mu.Unlock()

	// Perform the flock probe and PID discovery without holding the lock —
	// both involve I/O and we must not stall other goroutines.
	locked := isLockHeld(nm.lockPath)
	pid := 0
	if locked {
		pid = findLockHolderPID(nm.lockPath)
	}

	nm.mu.Lock()
	defer nm.mu.Unlock()

	// Re-check state: a Start() may have raced in while we were probing.
	if nm.state == NodeRunning || nm.state == NodeStarting || nm.state == NodeStopping {
		return
	}

	if locked {
		if nm.state != NodeExternal {
			nm.state = NodeExternal
			nm.pid = pid
			nm.exitErr = ""
			nm.version++
		} else if nm.pid != pid {
			// Already external — update PID if it changed (process restart).
			nm.pid = pid
			nm.version++
		}
	} else if prevState == NodeExternal && nm.state == NodeExternal {
		nm.state = NodeStopped
		nm.pid = 0
		nm.version++
	}
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

// Start spawns the Erigon binary as a child process.
// Returns an error if a process is already managed or an external node is detected.
// This method performs blocking syscalls (stat, open, exec); callers on the
// tview event loop must dispatch it to a background goroutine.
func (nm *NodeManager) Start(ctx context.Context) error {
	nm.mu.Lock()

	switch nm.state {
	case NodeRunning, NodeStarting, NodeStopping:
		st, pid := nm.state, nm.pid
		nm.mu.Unlock()
		return fmt.Errorf("node already %s (PID %d)", st, pid)
	case NodeExternal:
		nm.mu.Unlock()
		return fmt.Errorf("external node is running — stop it first")
	}

	nm.mu.Unlock()

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

	cmd := exec.CommandContext(ctx, absPath, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	// Set a process group so we can signal the whole group.
	setProcessGroup(cmd)

	if err := cmd.Start(); err != nil {
		logFile.Close()
		return fmt.Errorf("starting erigon: %w", err)
	}

	// Re-acquire lock to update state atomically.
	nm.mu.Lock()
	nm.cmd = cmd
	nm.pid = cmd.Process.Pid
	nm.state = NodeStarting
	nm.startTime = time.Now()
	nm.exitErr = ""
	nm.version++
	nm.done = make(chan struct{})
	// Capture done channel as a local so confirmStartup uses the correct one
	// even if Stop()+Start() is called concurrently.
	done := nm.done
	nm.mu.Unlock()

	// Background goroutine to wait for exit and update state.
	go nm.waitForExit(cmd, logFile)

	// Transition Starting→Running after a brief delay (if still alive).
	go nm.confirmStartup(done)

	return nil
}

// waitForExit waits for the child process to finish and updates state.
func (nm *NodeManager) waitForExit(cmd *exec.Cmd, logFile *os.File) {
	err := cmd.Wait()
	logFile.Close()

	nm.mu.Lock()
	defer nm.mu.Unlock()

	// Only update if this cmd is still the current one (guards against
	// a rapid Stop()+Start() that replaced nm.cmd).
	if nm.cmd != cmd {
		return
	}

	nm.pid = 0
	nm.cmd = nil
	if err != nil {
		nm.exitErr = err.Error()
	} else {
		nm.exitErr = ""
	}
	nm.state = NodeStopped
	nm.version++

	close(nm.done)
}

// confirmStartup transitions Starting→Running if the process is still alive
// after 2 seconds. done is the channel from the specific Start() invocation
// that spawned this goroutine.
func (nm *NodeManager) confirmStartup(done <-chan struct{}) {
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()

	select {
	case <-timer.C:
		nm.mu.Lock()
		if nm.state == NodeStarting {
			nm.state = NodeRunning
			nm.version++
		}
		nm.mu.Unlock()
	case <-done:
		// Process exited before confirmation.
	}
}

// Stop sends SIGTERM to the child process and waits up to 15 seconds.
// If the process doesn't exit, it sends SIGKILL.
// Returns an error if no managed process is running or if the process
// survives SIGKILL.
func (nm *NodeManager) Stop() error {
	nm.mu.Lock()
	if nm.state == NodeExternal {
		nm.mu.Unlock()
		return fmt.Errorf("cannot stop external node — stop it outside the TUI")
	}
	if nm.state != NodeRunning && nm.state != NodeStarting {
		nm.mu.Unlock()
		return fmt.Errorf("no managed node is running")
	}

	cmd := nm.cmd
	done := nm.done
	nm.state = NodeStopping
	nm.version++
	nm.mu.Unlock()

	// Send SIGTERM (or platform equivalent) to the process group.
	if cmd != nil && cmd.Process != nil {
		terminateGraceful(cmd) //nolint:errcheck
	}

	// Wait up to 15 seconds for graceful shutdown.
	select {
	case <-done:
		return nil
	case <-time.After(15 * time.Second):
	}

	// Force kill.
	if cmd != nil && cmd.Process != nil {
		terminateForce(cmd) //nolint:errcheck
	}

	// Wait for the process to actually finish after SIGKILL.
	select {
	case <-done:
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("process (PID %d) did not exit after SIGKILL", cmd.Process.Pid)
	}
}

// IsManaged returns true if the TUI owns the running process (vs external).
func (nm *NodeManager) IsManaged() bool {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	return nm.state == NodeRunning || nm.state == NodeStarting || nm.state == NodeStopping
}
