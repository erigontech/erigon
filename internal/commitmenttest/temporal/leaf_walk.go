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

package temporal

import (
	"context"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/state"
)

func LeafWalkWorkers(t testing.TB) func() int {
	t.Helper()
	previousHandler := log.Root().GetHandler()
	previousInterval := state.PBTProgressInterval
	state.PBTProgressInterval = time.Nanosecond
	var mu sync.Mutex
	var opener uint64
	workers := make(map[uint64]struct{})
	log.Root().SetHandler(leafWalkHandler(func(record *log.Record) error {
		if record.Msg != "PBT leaf stream progress" || !leafWalkRecord(record) {
			return nil
		}
		id := goroutineID()
		mu.Lock()
		defer mu.Unlock()
		switch opener {
		case 0:
			opener = id
		case id:
			opener = 0
		default:
			workers[id] = struct{}{}
		}
		return nil
	}))
	t.Cleanup(func() {
		log.Root().SetHandler(previousHandler)
		state.PBTProgressInterval = previousInterval
	})
	return func() int {
		mu.Lock()
		defer mu.Unlock()
		count := len(workers)
		clear(workers)
		return count
	}
}

type leafWalkHandler func(*log.Record) error

func (h leafWalkHandler) Log(record *log.Record) error { return h(record) }

func (h leafWalkHandler) Enabled(context.Context, log.Lvl) bool { return true }

func leafWalkRecord(record *log.Record) bool {
	var phase, parallel bool
	for i := 0; i+1 < len(record.Ctx); i += 2 {
		switch record.Ctx[i] {
		case "phase":
			phase = record.Ctx[i+1] == "source leaf walk"
		case "workers":
			parallel = true
		}
	}
	return phase && parallel
}

func goroutineID() uint64 {
	var buf [64]byte
	fields := strings.Fields(string(buf[:runtime.Stack(buf[:], false)]))
	id, _ := strconv.ParseUint(fields[1], 10, 64)
	return id
}
