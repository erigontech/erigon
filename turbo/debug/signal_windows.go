//go:build windows

package debug

import (
	"io"
	"os"
	"os/signal"

	_debug "github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/log/v3"
)

func ListenSignals(stack io.Closer, logger log.Logger) {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt)
	_debug.GetSigC(&sigc)
	defer signal.Stop(sigc)

	<-sigc
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
}
