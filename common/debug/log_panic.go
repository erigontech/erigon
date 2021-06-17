package debug

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/ledgerwatch/erigon/log"
)

var sigc chan os.Signal
var crashReportDir string

func GetSigC(sig *chan os.Signal) {
	sigc = *sig
}

func prettyTime() string {
	time := fmt.Sprintf("%v", time.Now())
	return strings.Replace(time[:19], " ", "-", 1)
}

func CheckForCrashes(datadir string) {
	crashReportDir = filepath.Join(datadir, "crashreports")
	if _, err := os.Stat(crashReportDir); os.IsNotExist(err) {
		os.Mkdir(crashReportDir, 0755)
	} else if err != nil {
		log.Error("log_panic.go: CheckForCrashes", "error", err)
		return
	}
	f, err := os.Open(crashReportDir)
	if err != nil {
		log.Error("log_panic.go: CheckForCrashes", "error", err)
		return
	}
	fileInfo, err := f.ReadDir(-1)
	if err != nil {
		log.Error("log_panic.go: CheckForCrashes", "error", err)
		return
	}
	for _, v := range fileInfo {
		if !v.IsDir() {
			msg := fmt.Sprintf("Crashes From Previous Boots Detected. Find the stack trace in %v",
				crashReportDir)
			log.Warn(msg)
			f.Close()
			return
		}
	}
}

func LogPanic(err error, stopErigon bool, panicResult interface{}) error {
	if panicResult != nil {
		stack := string(debug.Stack())
		switch typed := panicResult.(type) {
		case error:
			err = fmt.Errorf("%w, trace: %s", typed, stack)
		default:
			err = fmt.Errorf("%+v, trace: %s", typed, stack)
		}
		WriteStackTraceOnPanic(stack)
		if stopErigon && sigc != nil {
			sigc <- syscall.SIGINT
		} else {
			return err
		}
	}
	return err
}

func WriteStackTraceOnPanic(stack string) {
	fileName := filepath.Join(crashReportDir, prettyTime()+".txt")
	f, errFs := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if errFs != nil {
		log.Error("log_panic.go:WriteStackTraceOnPanic", "error", errFs)
		f.Close()
		return
	}
	f.WriteString(stack)
	f.Sync()
	f.Close()
}
