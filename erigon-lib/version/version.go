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

package version

// see https://calver.org
const (
	Major                    = 3      // Major version component of the current release
	Minor                    = 1      // Minor version component of the current release
	Micro                    = 0      // Patch version component of the current release
	Modifier                 = "dev"  // Modifier component of the current release
	DefaultSnapshotGitBranch = "main" // Branch of erigontech/erigon-snapshot to use in OtterSync
)
