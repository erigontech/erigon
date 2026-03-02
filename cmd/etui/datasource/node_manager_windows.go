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

// setProcessGroup configures the command to run in a new process group
// using CREATE_NEW_PROCESS_GROUP so the child can be signalled independently.
func setProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP}
}

// terminateGraceful sends an interrupt signal to the process.
// On Windows this sends CTRL_BREAK_EVENT to the process group.
func terminateGraceful(cmd *exec.Cmd) {
	cmd.Process.Signal(os.Interrupt) //nolint:errcheck
}

// terminateForce forcefully kills the process.
func terminateForce(cmd *exec.Cmd) {
	cmd.Process.Kill() //nolint:errcheck
}
