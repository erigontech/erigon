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

package harness

import "time"

// Observation captures a single read attempt: the value returned, the
// error returned, and the wall-clock duration of the call. Scenario tests
// assert on bands of latency and on err identity (errors.Is checks
// against typed sentinels).
type Observation struct {
	Value   string
	Latency time.Duration
	Err     error
}

// Observe runs fn and captures its (value, error) result paired with the
// wall-clock duration of the call. Convenience wrapper to keep test
// bodies focused on setup + assertions.
func Observe(fn func() (string, error)) Observation {
	start := time.Now()
	v, err := fn()
	return Observation{
		Value:   v,
		Latency: time.Since(start),
		Err:     err,
	}
}
