// Copyright 2026 The Erigon Authors
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

	pc, _, _, ok := runtime.Caller(1 + skipDepth)
	if !ok {
		return ""
	}
	fn := runtime.FuncForPC(pc)
	if fn == nil {
		return ""
	}
	parts := strings.Split(fn.Name(), ".")
	pl := len(parts)
	if pl < 2 {
		return fn.Name()
	}
	packageName := ""

	if parts[pl-2][0] == '(' || parts[pl-2] == "init" {
		packageName = strings.Join(parts[0:pl-2], ".")
	} else {
		packageName = strings.Join(parts[0:pl-1], ".")
	}

	return packageName
}
