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
	"bytes"
	"fmt"
	"os"
	"runtime/debug"
	"sync/atomic"
	"syscall"

	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/log/v3"
	"golang.org/x/sync/errgroup"
)

var sigc atomic.Value

func GetSigC(sig *chan os.Signal) {
	sigc.Store(*sig)
}

// LogPanic - does log panic to logger then stops the process
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

// Recovers errors, logs the stack trace and sets an error value.
func RecoverPanicIntoError(logger log.Logger, outErr *error) {
	if *outErr != nil {
		// Don't swallow panics if an error is already set. This is an unrecoverable situation.
		return
	}
	r := recover()
	if r == nil {
		return
	}
	err, ok := r.(error)
	if !ok {
		panic(r)
	}
	*outErr = err
	logger.Crit("recovered panic", "err", err, "stack", string(debug.Stack()))
}

// Runs errgroup.Group.Wait and recovers from errgroup.PanicError.
func WaitErrGroupRecoverPanicError(eg *errgroup.Group, logger log.Logger) (err error) {
	defer func() {
		if err != nil {
			return
		}
		r := recover()
		if r == nil {
			return
		}
		// Don't check for nested error as it's expected that we are the first to handle the error.
		pe, ok := r.(errgroup.PanicError)
		if !ok {
			panic(r)
		}
		// We really don't want to miss the panic stack traces.
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "panicked with error: %v\n%s", pe.Recovered, pe.Stack)
		fmt.Fprintf(&buf, "recovered by %s", debug.Stack())
		os.Stderr.Write(buf.Bytes())
		// Do we need context for where the Wait occurred? Because we do the Wait here, we know the
		// stack trace between the Wait and the callsite is predictable and uninteresting.
		logger.Crit(
			"recovered from errgroup.PanicError",
			"err", pe.Recovered,
			"panic-stack", string(pe.Stack),
			"recover-stack", string(debug.Stack()))
		err = pe.Recovered
	}()
	return eg.Wait()
}
