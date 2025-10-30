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
	stack2 "github.com/go-stack/stack"
)

// Stack returns stack-trace in logger-friendly compact formatting
func Stack() string {
	return stack2.Trace().TrimBelow(stack2.Caller(1)).String()
}
func StackSkip(skip int) string {
	return stack2.Trace().TrimBelow(stack2.Caller(skip)).String()
}
