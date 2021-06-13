package common

// Go runs a goroutine and when used with
// RecoverStackTrace, it writes the stack trace to a file
// on panic, cleans up gracefully and alerts the user
//of the files location on next boot
func Go(goroutine func(args ...interface{}), args ...interface{}) {
	go func() {
		defer func() { LogPanic(recover()) }()
		goroutine(args...)
	}()
}
