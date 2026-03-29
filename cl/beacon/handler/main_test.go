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

package handler

import (
	"net"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	// Most tests in this package start httptest.Server instances. Skip the
	// whole package when the environment does not permit binding to a local
	// port (e.g. a restricted CI sandbox).
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		os.Exit(0)
	}
	ln.Close()
	os.Exit(m.Run())
}
