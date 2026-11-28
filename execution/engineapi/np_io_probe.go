package engineapi

import (
	"bytes"
	"os"
	"strconv"
	"syscall"
	"time"
)

// npIO is a process-wide IO/CPU counter snapshot: major faults, block reads, read_bytes, user+sys CPU.
type npIO struct{ majflt, inblock, readBytes, cpuUs int64 }

func npIONow() (s npIO) {
	var ru syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &ru) == nil {
		s.majflt, s.inblock = int64(ru.Majflt), int64(ru.Inblock)
		s.cpuUs = (int64(ru.Utime.Sec)+int64(ru.Stime.Sec))*1e6 + int64(ru.Utime.Usec) + int64(ru.Stime.Usec)
	}
	if b, err := os.ReadFile("/proc/self/io"); err == nil {
		for _, l := range bytes.Split(b, []byte("\n")) {
			if v, ok := bytes.CutPrefix(l, []byte("read_bytes: ")); ok {
				s.readBytes, _ = strconv.ParseInt(string(v), 10, 64)
			}
		}
	}
	return s
}

// npIOArgs logs counters at entry, after the readiness wait, and at the end; zero snapshots are skipped.
func npIOArgs(entry, wait, end npIO, waitT time.Time) []any {
	if waitT.IsZero() {
		wait = entry
	}
	return []any{"flt0", entry.majflt, "fltW", wait.majflt, "flt1", end.majflt, "rb0", entry.readBytes, "rbW", wait.readBytes, "rb1", end.readBytes,
		"inb0", entry.inblock, "inb1", end.inblock, "cpu0", entry.cpuUs, "cpuW", wait.cpuUs, "cpu1", end.cpuUs}
}
