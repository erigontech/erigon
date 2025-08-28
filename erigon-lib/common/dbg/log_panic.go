// Copyright 2021 The Erigon Authors
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

package dbg

import (
	"os"
	"runtime/debug"
	"sync/atomic"
	"syscall"

	stack2 "github.com/go-stack/stack"

	"github.com/erigontech/erigon-lib/log/v3"
)

// Stack returns stack-trace in logger-friendly compact formatting
func Stack() string {
	return stack2.Trace().TrimBelow(stack2.Caller(1)).String()
}
func StackSkip(skip int) string {
	return stack2.Trace().TrimBelow(stack2.Caller(skip)).String()
}

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

	log.Error("catch panic", "err", panicResult, "stack", Stack())
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
