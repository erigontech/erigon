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
	"runtime"
)

// Callers returns given number of callers with packages
func Callers(show int) []string {
	fpcs := make([]uintptr, show)
	n := runtime.Callers(2, fpcs)
	if n == 0 {
		return nil
	}

	callers := make([]string, 0, len(fpcs))
	for _, p := range fpcs {
		caller := runtime.FuncForPC(p - 1)
		if caller == nil {
			continue
		}
		callers = append(callers, caller.Name())
	}

	return callers
}
