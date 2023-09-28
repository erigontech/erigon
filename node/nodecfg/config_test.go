// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package nodecfg_test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common/datadir"
	node2 "github.com/ledgerwatch/erigon/node"
	"github.com/ledgerwatch/erigon/node/nodecfg"
	"github.com/ledgerwatch/log/v3"
)

// Tests that datadirs can be successfully created, be them manually configured
// ones or automatically generated temporary ones.
func TestDataDirCreation(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please")
	}
	// Create a temporary data dir and check that it can be used by a node
	dir := t.TempDir()
	node, err := node2.New(context.Background(), &nodecfg.Config{Dirs: datadir.New(dir)}, log.New())
	if err != nil {
		t.Fatalf("failed to create stack with existing datadir: %v", err)
	}
	if err := node.Close(); err != nil {
		t.Fatalf("failed to close node: %v", err)
	}
	// Generate a long non-existing datadir path and check that it gets created by a node
	dir = filepath.Join(dir, "a", "b", "c", "d", "e", "f")
	node, err = node2.New(context.Background(), &nodecfg.Config{Dirs: datadir.New(dir)}, log.New())
	if err != nil {
		t.Fatalf("failed to create stack with creatable datadir: %v", err)
	}
	if err := node.Close(); err != nil {
		t.Fatalf("failed to close node: %v", err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("freshly created datadir not accessible: %v", err)
	}
	// Verify that an impossible datadir fails creation
	file, err := os.CreateTemp("", "")
	if err != nil {
		t.Fatalf("failed to create temporary file: %v", err)
	}
	defer os.Remove(file.Name())
}

// Tests that IPC paths are correctly resolved to valid endpoints of different
// platforms.
func TestIPCPathResolution(t *testing.T) {
	var tests = []struct {
		DataDir  string
		IPCPath  string
		Windows  bool
		Endpoint string
	}{
		{"", "", false, ""},
		{"data", "", false, ""},
		{"", "geth.ipc", false, filepath.Join(os.TempDir(), "geth.ipc")},
		{"data", "geth.ipc", false, "data/geth.ipc"},
		{"data", "./geth.ipc", false, "./geth.ipc"},
		{"data", "/geth.ipc", false, "/geth.ipc"},
		{"", "", true, ``},
		{"data", "", true, ``},
		{"", "geth.ipc", true, `\\.\pipe\geth.ipc`},
		{"data", "geth.ipc", true, `\\.\pipe\geth.ipc`},
		{"data", `\\.\pipe\geth.ipc`, true, `\\.\pipe\geth.ipc`},
	}
	for i, test := range tests {
		// Only run when platform/test match
		if (runtime.GOOS == "windows") == test.Windows {
			if endpoint := (&nodecfg.Config{Dirs: datadir.New(test.DataDir), IPCPath: test.IPCPath}).IPCEndpoint(); endpoint != test.Endpoint {
				t.Errorf("test %d: IPC endpoint mismatch: have %s, want %s", i, endpoint, test.Endpoint)
			}
		}
	}
}
