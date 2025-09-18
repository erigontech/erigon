// Copyright 2025 The Erigon Authors
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

<<<<<<<< HEAD:erigon-lib/common/dbg/pprof_cgo.go
//go:build debug
// +build debug

package dbg

import (
	_ "github.com/benesch/cgosymbolizer"
)
========
//go:build go1.25

package synctest

import "testing/synctest"

var Test testFunc = synctest.Test
>>>>>>>> release/3.1:erigon-lib/synctest/synctest_go_1_25_and_beyond.go
