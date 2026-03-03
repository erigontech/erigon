//go:build windows

package datasource

import (
	"os"
	"os/exec"
	"syscall"
)

// findLockHolderPID is not supported on Windows (no /proc/locks).
// Returns 0 — the external PID will be unknown.
func findLockHolderPID(_ string) int {
	return 0
}

// spawnDetached configures the command to run in a new process group
// with DETACHED_PROCESS so the child survives TUI exit.
func spawnDetached(cmd *exec.Cmd) {
	const detachedProcess = 0x00000008
	cmd.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP | detachedProcess,
	}
}

// killByPID sends an interrupt signal to the process.
// On Windows this sends CTRL_BREAK_EVENT to the process group.
func killByPID(pid int) {
	p, err := os.FindProcess(pid)
	if err != nil {
		return
	}
	p.Signal(os.Interrupt) //nolint:errcheck
}

// forceKillByPID forcefully kills the process.
func forceKillByPID(pid int) {
	p, err := os.FindProcess(pid)
	if err != nil {
		return
	}
	p.Kill() //nolint:errcheck
}
