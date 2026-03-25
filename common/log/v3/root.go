package log

import (
	"os"

	"github.com/mattn/go-colorable"
	"github.com/mattn/go-isatty"
)

// Predefined handlers.
//
// StdoutHandler and StderrHandler use AsyncStreamHandler so that goroutines
// writing log records are never blocked by a mutex.  Under high concurrency
// (e.g. thousands of Caplin validator goroutines) the synchronous SyncHandler
// inside the plain StreamHandler caused complete process freezes because every
// goroutine serialised on the same mutex for every log write.
var (
	root          *logger
	StdoutHandler = AsyncStreamHandler(os.Stdout, TerminalFormatNoColor())
	StderrHandler = AsyncStreamHandler(os.Stderr, TerminalFormatNoColor())
)

func init() {
	if isatty.IsTerminal(os.Stdout.Fd()) {
		StdoutHandler = AsyncStreamHandler(colorable.NewColorableStdout(), TerminalFormat())
	}

	if isatty.IsTerminal(os.Stderr.Fd()) {
		StderrHandler = AsyncStreamHandler(colorable.NewColorableStderr(), TerminalFormat())
	}

	root = &logger{[]any{}, new(swapHandler)}
	root.SetHandler(LvlFilterHandler(LvlWarn, StdoutHandler))
}

// New returns a new logger with the given context.
// New is a convenient alias for Root().New
func New(ctx ...any) Logger {
	return root.New(ctx...)
}

// Root returns the root logger
func Root() Logger {
	return root
}

// The following functions bypass the exported logger methods (logger.Debug,
// etc.) to keep the call depth the same for all paths to logger.write so
// runtime.Caller(2) always refers to the call site in client code.

// Trace is a convenient alias for Root().Trace
func Trace(msg string, ctx ...any) {
	root.write(msg, LvlTrace, ctx)
}

// Debug is a convenient alias for Root().Debug
func Debug(msg string, ctx ...any) {
	root.write(msg, LvlDebug, ctx)
}

// Info is a convenient alias for Root().Info
func Info(msg string, ctx ...any) {
	root.write(msg, LvlInfo, ctx)
}

// Warn is a convenient alias for Root().Warn
func Warn(msg string, ctx ...any) {
	root.write(msg, LvlWarn, ctx)
}

// Error is a convenient alias for Root().Error
func Error(msg string, ctx ...any) {
	root.write(msg, LvlError, ctx)
}

// Crit is a convenient alias for Root().Crit
func Crit(msg string, ctx ...any) {
	root.write(msg, LvlCrit, ctx)
}

// Log method to route configurable log level
func Log(level Lvl, msg string, ctx ...any) {
	root.write(msg, level, ctx)
}

// SetRootHandler recreates root logger and set h as multihandler along with existed root handler
func SetRootHandler(h Handler) {
	oldHandler := root.GetHandler()
	root = &logger{[]any{}, new(swapHandler)}
	root.SetHandler(MultiHandler(oldHandler, h))
}
