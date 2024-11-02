package util

import (
	"runtime"
	"strings"
	"unicode"
)

// IsPrintable checks if s is ascii and printable, aka doesn't include tab, backspace, etc.
func IsPrintable(s string) bool {
	for _, r := range s {
		if r > unicode.MaxASCII || !unicode.IsPrint(r) {
			return false
		}
	}
	return true
}

func CallerPackageName(skip ...int) string {
	skipDepth := 0

	if len(skip) > 0 {
		skipDepth = skip[0]
	}

	pc, _, _, _ := runtime.Caller(1 + skipDepth)
	parts := strings.Split(runtime.FuncForPC(pc).Name(), ".")
	pl := len(parts)
	packageName := ""

	if parts[pl-2][0] == '(' || parts[pl-2] == "init" {
		packageName = strings.Join(parts[0:pl-2], ".")
	} else {
		packageName = strings.Join(parts[0:pl-1], ".")
	}

	return packageName
}
