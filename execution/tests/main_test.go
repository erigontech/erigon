package executiontests

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"

	"github.com/erigontech/erigon/common/dir"
)

func TestMain(m *testing.M) {
	cleanup := setupRAMTmpdir()
	code := m.Run()
	cleanup()
	os.Exit(code)
}

// setupRAMTmpdir sets TMPDIR to a RAM-backed filesystem for faster I/O in tests.
// Returns a cleanup function. Falls back gracefully if setup fails.
func setupRAMTmpdir() func() {
	noop := func() {}

	switch runtime.GOOS {
	case "darwin":
		return setupDarwinRAMDisk()
	case "linux":
		// /dev/shm is tmpfs on most Linux distros
		if info, err := os.Stat("/dev/shm"); err == nil && info.IsDir() {
			tmpDir, err := os.MkdirTemp("/dev/shm", "erigon-test-*")
			if err != nil {
				return noop
			}
			os.Setenv("TMPDIR", tmpDir)
			fmt.Fprintf(os.Stderr, "test tmpdir: %s (tmpfs)\n", tmpDir)
			return func() {
				dir.RemoveAll(tmpDir)
			}
		}
	}
	return noop
}

func setupDarwinRAMDisk() func() {
	noop := func() {}

	// Create 2GB RAM disk: 4194304 sectors * 512 bytes = 2GB
	out, err := exec.Command("hdiutil", "attach", "-nomount", "ram://4194304").Output()
	if err != nil {
		return noop
	}
	device := strings.TrimSpace(string(out))

	// Format as HFS+
	if out, err := exec.Command("diskutil", "erasevolume", "HFS+", "erigon-test", device).CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "ramdisk format failed: %s\n", out)
		exec.Command("hdiutil", "detach", device).Run()
		return noop
	}

	// Remount at a writable path (macOS sandbox blocks /Volumes writes)
	mountPoint, err := os.MkdirTemp("", "erigon-ramdisk-*")
	if err != nil {
		exec.Command("hdiutil", "detach", device).Run()
		return noop
	}

	// erasevolume mounts at /Volumes/<name>, unmount so we can remount at our path
	exec.Command("diskutil", "unmount", device).Run()
	if out, err := exec.Command("diskutil", "mount", "-mountPoint", mountPoint, device).CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "ramdisk mount failed: %s\n", out)
		dir.RemoveAll(mountPoint)
		exec.Command("hdiutil", "detach", device).Run()
		return noop
	}

	os.Setenv("TMPDIR", mountPoint)
	fmt.Fprintf(os.Stderr, "test tmpdir: %s (ramdisk %s)\n", mountPoint, device)

	return func() {
		exec.Command("diskutil", "unmount", mountPoint).Run()
		exec.Command("hdiutil", "detach", device).Run()
		dir.RemoveAll(mountPoint)
	}
}
