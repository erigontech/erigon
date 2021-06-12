package debug

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	"github.com/ledgerwatch/erigon/log"
)

func prettyTime() string {
	time := fmt.Sprintf("%v", time.Now())
	return strings.Replace(time[:19], " ", "-", 1)
}

func ArchiveReportedCrashes() {
	ex, _ := os.Executable()
	binPath := filepath.Dir(ex)
	crashReportDir := binPath[:len(binPath)-10] + "/crashreports/"
	f, err := os.Open(crashReportDir)
	if err != nil {
		log.Error(err.Error())
		return
	}
	fileInfo, err := f.ReadDir(-1)
	for _, v := range fileInfo {
		if !v.IsDir() {
			oldFilePath := fmt.Sprintf("%v%v", crashReportDir, v.Name())
			newFilePath := fmt.Sprintf("%vold/%v", crashReportDir, v.Name())
			os.Rename(oldFilePath, newFilePath)
			return
		}
	}

}

func CheckForCrashes() {
	ex, _ := os.Executable()
	binPath := filepath.Dir(ex)
	crashReportDir := binPath[:len(binPath)-10] + "/crashreports/"
	f, err := os.Open(crashReportDir)
	if err != nil {
		log.Error(err.Error())
		return
	}
	fileInfo, err := f.ReadDir(-1)
	for _, v := range fileInfo {
		if !v.IsDir() {
			msg := fmt.Sprintf("Crash From Previous Boot Detected. Find the stack trace in %v",
				crashReportDir)
			log.Warn(msg)
			return
		}
	}
}

func RecoverStackTrace(err error, panicResult interface{}) error {
	if panicResult != nil {
		panicReplacer := strings.NewReplacer("\n", " ", "\t", "", "\r", "")
		stack := panicReplacer.Replace(string(debug.Stack()))
		switch typed := panicResult.(type) {
		case error:
			err = fmt.Errorf("%w, trace: %s", typed, stack)
		default:
			err = fmt.Errorf("%+v, trace: %s", typed, stack)
		}
		WriteStackTraceOnPanic(stack)
		return err
	}
	return err
}

func WriteStackTraceOnPanic(stack string) {
	ex, _ := os.Executable()
	binPath := filepath.Dir(ex)
	crashReportDir := binPath[:len(binPath)-10] + "/crashreports/"
	fileName := fmt.Sprintf("%v%v.txt", crashReportDir, prettyTime())
	f, errFs := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if errFs != nil {
		log.Error(errFs.Error())
	}
	f.WriteString(stack)
	f.Sync()
	f.Close()
}
