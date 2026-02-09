// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package debug

import (
	"os"
	"os/signal"
	"runtime/pprof"

	g "github.com/anacrolix/generics"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
)

func listenSignalsInner(handle func(), logger log.Logger, shutdown []os.Signal, prof g.Option[os.Signal]) {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, shutdown...)
	dbg.GetSigC(&sigc)
	defer signal.Stop(sigc)

	usr1 := make(chan os.Signal, 1)
	if prof.Ok {
		signal.Notify(usr1, prof.Value)
	}
	for {
		select {
		case <-sigc:
			logger.Info("Got interrupt, shutting down...")
			if handle != nil {
				go handle()
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
