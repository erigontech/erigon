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
	if _, err := os.Stat(crashReportDir); err != nil && os.IsNotExist(err) {
		_ = os.MkdirAll(crashReportDir, 0755)
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
			_ = f.Close()
			return
		}
	}
}

var panicReplacer = strings.NewReplacer("\n", " ", "\t", "", "\r", "")

// LogPanic - does log panic to logger and to <datadir>/crashreports then stops the process
func LogPanic() {
	panicResult := recover()
	if panicResult == nil {
		return
	}

	stack := string(debug.Stack())
	log.Error("panic", "err", panicResult, "stack", panicReplacer.Replace(stack))
	WriteStackTraceOnPanic(stack)
	if sigc != nil {
		sigc <- syscall.SIGINT
	}
}

// ReportPanicAndRecover - does save panic to datadir/crashreports, bud doesn't log to logger and doesn't stop the process
// it returns recovered panic as error in format friendly for our logger
// common pattern of use - assign to named output param:
//  func A() (err error) {
//	    defer func() { err = debug.ReportPanicAndRecover() }() // avoid crash because Erigon's core does many things
//  }
func ReportPanicAndRecover() (err error) {
	panicResult := recover()
	if panicResult == nil {
		return nil
	}

	stack := string(debug.Stack())
	switch typed := panicResult.(type) {
	case error:
		err = fmt.Errorf("%w, trace: %s", typed, panicReplacer.Replace(stack))
	default:
		err = fmt.Errorf("%+v, trace: %s", typed, panicReplacer.Replace(stack))
	}
	WriteStackTraceOnPanic(stack)
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
