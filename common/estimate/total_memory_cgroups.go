//go:build linux

/*
https://github.com/raulk/go-watchdog
https://github.com/elee1766/go-watchdog

The MIT License (MIT)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package estimate

import (
	"errors"
	"fmt"
	"os"

	"github.com/containerd/cgroups/v3"
	"github.com/containerd/cgroups/v3/cgroup1"
	"github.com/containerd/cgroups/v3/cgroup2"
)

// cgroupsMemoryLimit will try to discover
// the memory limit from the cgroup of the process (derived from /proc/self/cgroup),
// or from the root cgroup path if the PID == 1 (which indicates that the process
// is running in a container).
//
// Memory usage is calculated by querying the cgroup stats.
//
// This function will return an error immediately if the OS does not support cgroups,
// or if another error occurs during initialization.
func cgroupsMemoryLimit() (uint64, error) {
	switch cgroups.Mode() {
	case cgroups.Unified:
		return cgroupsV2MemoryLimit()
	case cgroups.Legacy:
		return cgroupsV1MemoryLimit()
	case cgroups.Unavailable:
		fallthrough
	default:
		return 0, errors.New("cgroups not supported in this environment")
	}
}

func cgroupsV1MemoryLimit() (uint64, error) {
	// use self path unless our PID is 1, in which case we're running inside
	// a container and our limits are in the root path.
	path := cgroup1.NestedPath("")
	if pid := os.Getpid(); pid == 1 {
		path = cgroup1.RootPath
	}

	cgroup, err := cgroup1.Load(path, cgroup1.WithHiearchy(func() ([]cgroup1.Subsystem, error) {
		system, err := cgroup1.Default()
		if err != nil {
			return nil, err
		}
		var out []cgroup1.Subsystem
		for _, v := range system {
			switch v.Name() {
			case cgroup1.Memory:
				out = append(out, v)
			}
		}
		return out, nil
	}))
	if err != nil {
		return 0, fmt.Errorf("failed to load cgroup1 for process: %w", err)
	}

	if stat, err := cgroup.Stat(); err != nil {
		return 0, fmt.Errorf("failed to load memory cgroup1 stats: %w", err)
	} else if stat.Memory == nil || stat.Memory.Usage == nil {
		return 0, errors.New("cgroup1 memory stats are nil; aborting")
	} else {
		return stat.Memory.Usage.Limit, nil
	}
}

func cgroupsV2MemoryLimit() (uint64, error) {
	// use self path unless our PID is 1, in which case we're running inside
	// a container and our limits are in the root path.
	pid := os.Getpid()
	path, err := cgroup2.PidGroupPath(pid)
	if err != nil {
		return 0, fmt.Errorf("failed to load cgroup2 path for process pid %d: %w", pid, err)
	}

	cgroup, err := cgroup2.Load(path)
	if err != nil {
		return 0, fmt.Errorf("failed to load cgroup2 for process: %w", err)
	}

	if stat, err := cgroup.Stat(); err != nil {
		return 0, fmt.Errorf("failed to load cgroup2 memory stats: %w", err)
	} else if stat.Memory == nil {
		return 0, errors.New("cgroup2 memory stats are nil; aborting")
	} else {
		return stat.Memory.UsageLimit, nil
	}
}
