package debug

import (
	"runtime"
)

func Callers(show int) []string {
	var callers []string

	fpcs := make([]uintptr, show)
	n := runtime.Callers(2, fpcs)
	if n == 0 {
		return nil
	}

	for _, p := range fpcs {
		caller := runtime.FuncForPC(p - 1)
		if caller == nil {
			continue
		}
		callers = append(callers, caller.Name())
	}

	return callers
}

