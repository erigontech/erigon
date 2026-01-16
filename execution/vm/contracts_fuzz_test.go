// Copyright 2023 The go-ethereum Authors
// (original work)
// Copyright 2025 The Erigon Authors
// (modifications)
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

package vm

import (
<<<<<<<< HEAD:execution/vm/contracts_fuzz_test.go
	"maps"
	"slices"
	"testing"
)

func FuzzPrecompiledContracts(f *testing.F) {
	// Create list of addresses
	addrs := slices.Collect(maps.Keys(allPrecompiles))
	f.Fuzz(func(t *testing.T, addr uint8, input []byte) {
		a := addrs[int(addr)%len(addrs)]
		p := allPrecompiles[a]
		gas := p.RequiredGas(input)
		if gas > 10_000_000 {
			return
		}
		inWant := string(input)
		RunPrecompiledContract(p, input, gas, nil)
		if inHave := string(input); inWant != inHave {
			t.Errorf("Precompiled %v modified input data", a)
		}
	})
========
	"fmt"
)

var (
	// Following vars are injected through the build flags (see Makefile)
	GitCommit string
	GitBranch string
	GitTag    string
)

// see https://calver.org
const (
	Major                    = 3             // Major version component of the current release
	Minor                    = 3             // Minor version component of the current release
	Micro                    = 0             // Patch version component of the current release
	Modifier                 = "dev"         // Modifier component of the current release
	DefaultSnapshotGitBranch = "release/3.1" // Branch of erigontech/erigon-snapshot to use in OtterSync
	SnapshotMainGitBranch    = "main"        // Branch of erigontech/erigon-snapshot to use in OtterSync for arb-sepolia snapshots
	VersionKeyCreated        = "ErigonVersionCreated"
	VersionKeyFinished       = "ErigonVersionFinished"
	ClientName               = "erigon"
	ClientCode               = "EG"
)

// VersionNoMeta holds the textual version string excluding the metadata.
var VersionNoMeta = func() string {
	return fmt.Sprintf("%d.%d.%d", Major, Minor, Micro)
}()

// VersionWithMeta holds the textual version string including the metadata.
var VersionWithMeta = func() string {
	v := VersionNoMeta
	if Modifier != "" {
		v += "-" + Modifier
	}
	return v
}()

func VersionWithCommit(gitCommit string) string {
	vsn := VersionWithMeta
	if len(gitCommit) >= 8 {
		vsn += "-" + gitCommit[:8]
	}
	return vsn
>>>>>>>> arbitrum:db/version/app.go
}
