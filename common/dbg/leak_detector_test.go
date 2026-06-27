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

package dbg

import (
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Each scenario runs in a re-exec'd child since a finalizer panic is fatal.
func TestGCLeakCheck(t *testing.T) {
	switch os.Getenv("DBG_LEAK_CHILD") {
	case "leak":
		runLeakChild(false)
		return
	case "disarm":
		runLeakChild(true)
		return
	}

	t.Run("panics when armed object is GC'd without close", func(t *testing.T) {
		out, err := runLeakCheckChild(t, "leak", true)
		if err == nil {
			t.Fatalf("expected child to crash on leak, but it exited cleanly. output:\n%s", out)
		}
		if !strings.Contains(out, "garbage-collected without close") {
			t.Fatalf("expected leak panic message, got:\n%s", out)
		}
	})

	t.Run("no panic when disarmed before GC", func(t *testing.T) {
		out, err := runLeakCheckChild(t, "disarm", true)
		if err != nil {
			t.Fatalf("expected clean exit after disarm, got %v. output:\n%s", err, out)
		}
	})

	t.Run("no-op when ASSERT and SLOW_TX are not both set", func(t *testing.T) {
		out, err := runLeakCheckChild(t, "leak", false)
		if err != nil {
			t.Fatalf("expected no-op when disabled, got %v. output:\n%s", err, out)
		}
	})
}

func runLeakCheckChild(t *testing.T, mode string, enabled bool) (string, error) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=TestGCLeakCheck")
	env := append(os.Environ(), "DBG_LEAK_CHILD="+mode)
	if enabled {
		env = append(env, "ERIGON_ASSERT=true", "SLOW_TX=1m")
	} else {
		env = append(env, "ERIGON_ASSERT=false", "SLOW_TX=")
	}
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	return string(out), err
}

//go:noinline
func armLeakedObject(disarm bool) {
	obj := new([8]byte)
	ArmGCLeakCheck("test-resource", obj)
	if disarm {
		DisarmGCLeakCheck(obj)
	}
}

func runLeakChild(disarm bool) {
	armLeakedObject(disarm)
	for i := 0; i < 50; i++ {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}
}
