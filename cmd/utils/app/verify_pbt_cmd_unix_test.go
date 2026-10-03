//go:build !windows

package app

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"testing"
)

func TestVerifyPBTScratchIOHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_VERIFY_PBT_IO_HELPER") != "1" {
		return
	}
	limit := &syscall.Rlimit{Cur: 2 << 20, Max: 2 << 20}
	if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, limit); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	err := verifyPBTFiles(t.Context(), os.Getenv("VERIFY_PBT_DATADIR"), os.Getenv("VERIFY_PBT_SNAPSHOT"), os.Getenv("VERIFY_PBT_PREIMAGES"), 0, os.Getenv("VERIFY_PBT_TMPDIR"))
	if err == nil || errors.Is(err, errVerifyPBTInvalid) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Fprintln(os.Stderr, err)
	os.Exit(0)
}
