package debug

import (
	"os"
	"sync"
	"syscall"

	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/log/v3"
)

var (
	sigm sync.Mutex
	sigc chan os.Signal
)

func GetSigC(sig *chan os.Signal) {
	sigm.Lock()
	sigc = *sig
	sigm.Unlock()
}

// LogPanic - does log panic to logger and to <datadir>/crashreports then stops the process
func LogPanic() {
	panicResult := recover()
	if panicResult == nil {
		return
	}

	log.Error("catch panic", "err", panicResult, "stack", dbg.Stack())

	sigm.Lock()
	defer sigm.Unlock()
	if sigc != nil {
		sigc <- syscall.SIGINT
	}
}
