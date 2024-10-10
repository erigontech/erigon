//go:build !windows

package debug

import (
	"io"
	"os"
	"os/signal"
	"runtime/pprof"

	_debug "github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sys/unix"
)

func ListenSignals(stack io.Closer, logger log.Logger) {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, unix.SIGINT, unix.SIGTERM)
	_debug.GetSigC(&sigc)
	defer signal.Stop(sigc)

	usr1 := make(chan os.Signal, 1)
	signal.Notify(usr1, unix.SIGUSR1)
	for {
		select {
		case <-sigc:
			logger.Info("Got interrupt, shutting down...")
			if stack != nil {
				go stack.Close()
			}
			for i := 10; i > 0; i-- {
				<-sigc
				if i > 1 {
					logger.Warn("Already shutting down, interrupt more to panic.", "times", i-1)
				}
			}
			Exit() // ensure trace and CPU profile data is flushed.
			LoudPanic("boom")
		case <-usr1:
			pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
		}
	}
}
