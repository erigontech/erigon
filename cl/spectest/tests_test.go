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

package spectest

import (
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/spectest/consensus_tests"
	"github.com/erigontech/erigon/cl/spectest/spectest"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/common/dbg"
)

var mainnetDir = filepath.Join("..", "..", "test-fixtures-cache", "cl_mainnet", "tests")

type devnet9FixtureFS struct{ fs.FS }

func (f devnet9FixtureFS) Open(name string) (fs.File, error) {
	switch path.Dir(name) {
	case "mainnet/gloas/operations/parent_execution_payload/pyspec_tests/process_parent_execution_payload__settle_previous_epoch",
		"mainnet/gloas/operations/parent_execution_payload/pyspec_tests/process_parent_execution_payload__older_than_previous_epoch":
		return os.DirFS("testdata/devnet9").Open(name)
	default:
		return f.FS.Open(name)
	}
}

func Test(t *testing.T) {
	// Skip when run as part of the broader test-all sweep; the dedicated
	// test-integration-caplin.yml workflow exercises this suite end-to-end
	// against the cl_mainnet fixtures. Avoids downloading ~677MB of
	// fixtures for every test-all run.
	if dbg.EnvBool("ERIGON_SKIP_CL_SPECTEST", false) {
		t.Skip("ERIGON_SKIP_CL_SPECTEST=true; consensus spec tests are covered by test-integration-caplin")
	}
	if clparams.GetBeaconConfig() == nil {
		caplinConfig := clparams.CaplinConfig{}
		clparams.InitGlobalStaticConfig(&clparams.MainnetBeaconConfig, &caplinConfig)
	}
	spectest.RunCases(t, consensus_tests.TestFormats, transition.ValidatingMachine, devnet9FixtureFS{os.DirFS(mainnetDir)})
}
