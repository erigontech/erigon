//go:build !windows

package datasource

import (
	"bufio"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
)

// findLockHolderPID discovers the PID of the process holding a flock on the
// given file by parsing /proc/locks. Returns 0 if the PID cannot be determined
// (e.g. /proc/locks unavailable, or lock held via a mechanism other than flock).
func findLockHolderPID(lockPath string) int {
	fi, err := os.Stat(lockPath)
	if err != nil {
		return 0
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0
	}

	// Cast to uint64 to avoid overflow on platforms where Stat_t.Dev is int32
	// (e.g. macOS/darwin). On Linux it is already uint64.
	dev := uint64(st.Dev)

	// Decode major:minor from stat dev_t (matches kernel new_decode_dev).
	targetMajor := uint32(((dev & 0x00000000000fff00) >> 8) |
		((dev & 0xfffff00000000000) >> 32))
	targetMinor := uint32((dev & 0x00000000000000ff) |
		((dev & 0x00000ffffff00000) >> 12))
	targetIno := st.Ino

	f, err := os.Open("/proc/locks")
	if err != nil {
		return 0
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		// /proc/locks format:
		//   1: FLOCK  ADVISORY  WRITE 12345 08:01:123456 0 EOF
		fields := strings.Fields(scanner.Text())
		if len(fields) < 8 || fields[1] != "FLOCK" {
			continue
		}

		pid, err := strconv.Atoi(fields[4])
		if err != nil || pid <= 0 {
			continue
		}

		// Parse "major:minor:inode" triplet.
		parts := strings.Split(fields[5], ":")
		if len(parts) != 3 {
			continue
		}
		major, err := strconv.ParseUint(parts[0], 16, 32)
		if err != nil {
			continue
		}
		minor, err := strconv.ParseUint(parts[1], 16, 32)
		if err != nil {
			continue
		}
		ino, err := strconv.ParseUint(parts[2], 10, 64)
		if err != nil {
			continue
		}

		if uint32(major) == targetMajor && uint32(minor) == targetMinor && ino == targetIno {
			return pid
		}
	}
	return 0
}

// spawnDetached configures the command to run in a new session (Setsid),
// so the child process survives TUI exit.
func spawnDetached(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
}

// killByPID sends SIGTERM to the entire process group led by the given PID.
// Using -pid signals all processes in the group, preventing orphaned children
// when the node spawns sub-processes (e.g. miners, p2p listeners).
func killByPID(pid int) {
	syscall.Kill(-pid, syscall.SIGTERM) //nolint:errcheck
}

// forceKillByPID sends SIGKILL to the entire process group led by the given PID.
func forceKillByPID(pid int) {
	syscall.Kill(-pid, syscall.SIGKILL) //nolint:errcheck
}
