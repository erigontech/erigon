//go:build !windows

package widgets

import "golang.org/x/sys/unix"

// diskFreeGB returns the available disk space in GB for the given directory.
func diskFreeGB(dir string) (float64, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(dir, &stat); err != nil {
		return 0, err
	}
	return float64(stat.Bavail) * float64(stat.Bsize) / (1 << 30), nil
}
