package dbg

import (
	stack2 "github.com/go-stack/stack"
)

// Stack returns stack-trace in logger-friendly compact formatting
func Stack() string {
	return stack2.Trace().TrimBelow(stack2.Caller(1)).String()
}
