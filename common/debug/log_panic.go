package debug

import (
	"os"
	"sync/atomic"
	"syscall"

	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/log/v3"
)

var sigc atomic.Value

func GetSigC(sig *chan os.Signal) {
	sigc.Store(*sig)
}

// LogPanic - does log panic to logger and to <datadir>/crashreports then stops the process
func LogPanic() {
	panicResult := recover()
	if panicResult == nil {
		return
	}

	log.Error("catch panic", "err", panicResult, "stack", dbg.Stack())
	if sl := sigc.Load(); sl != nil {
		sl.(chan os.Signal) <- syscall.SIGINT
	}
}
