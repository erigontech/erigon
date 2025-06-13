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

package systemcontracts

import "github.com/erigontech/erigon-lib/common"

var (
	// SystemContractCodeLookup is used to address a flaw in the upgrade logic of the system contracts. Since they are updated directly, without first being self-destructed
	// and then re-created, the usual incarnation logic does not get activated, and all historical records of the code of these contracts are retrieved as the most
	// recent version. This problem will not exist in erigon3, but until then, a workaround will be used to access code of such contracts through this structure
	// Lookup is performed first by chain name, then by contract address. The value in the map is the list of CodeRecords, with increasing block numbers,
	// to be used in binary search to determine correct historical code
	SystemContractCodeLookup = map[string]map[common.Address][]common.CodeRecord{}
)
