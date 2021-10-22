package dbg

import (
	"fmt"

	stack2 "github.com/go-stack/stack"
)

func StackInLogsFormat() string {
	return stack2.Trace().TrimBelow(stack2.Caller(1)).String()
}

// Recover - does save panic to datadir/crashreports, bud doesn't log to logger and doesn't stop the process
// it returns recovered panic as error in format friendly for our logger
// common pattern of use - assign to named output param:
//  func A() (err error) {
//	    defer func() { err = debug.Recover(err) }() // avoid crash because Erigon's core does many things
//  }
func Recover(err error) error {
	panicResult := recover()
	if panicResult == nil {
		return err
	}

	switch typed := panicResult.(type) {
	case error:
		err = fmt.Errorf("%w, trace: %s", typed, StackInLogsFormat())
	default:
		err = fmt.Errorf("%+v, trace: %s", typed, StackInLogsFormat())
	}
	return err
}
