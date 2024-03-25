package terminate

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"syscall"
	"time"
)

func Gracefully(format string, args ...any) {
	//goland:noinspection GoBoolExpressions
	if runtime.GOOS == "windows" {
		Fatalf(format, args)
		return
	}

	w := Writer()
	timer := time.NewTimer(15 * time.Second)
	for range timer.C {
		if err := syscall.Kill(syscall.Getpid(), syscall.SIGINT); err != nil {
			_, _ = fmt.Fprintf(w, "could not send term signal - err=%v", err)
		}
	}
}

// Fatalf formats a message to standard error and exits the program.
// The message is also printed to standard output if standard error
// is redirected to a different file.
func Fatalf(format string, args ...any) {
	w := Writer()
	_, _ = fmt.Fprintf(w, "Fatal: "+format+"\n", args...)
	os.Exit(1)
}

func Writer() io.Writer {
	//goland:noinspection GoBoolExpressions
	if runtime.GOOS == "windows" {
		// The SameFile check below doesn't work on Windows.
		// stdout is unlikely to get redirected though, so just print there.
		return os.Stdout
	}

	outf, _ := os.Stdout.Stat()
	errf, _ := os.Stderr.Stat()
	if outf != nil && errf != nil && os.SameFile(outf, errf) {
		return os.Stderr
	}

	return io.MultiWriter(os.Stdout, os.Stderr)
}
