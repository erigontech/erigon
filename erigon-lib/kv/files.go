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

package kv

const (
	// Domains: File<identifier>Domain constant used
	// as part of the filenames in the /snapshots/{domain/history/idx} dirs.
	//
	// Must have corresponding constants in tables.go file in the format:
	//
	// Tbl<identifier>Keys
	// Tbl<identifier>Vals
	// Tbl<identifier>HistoryKeys
	// Tbl<identifier>HistoryVals
	FileAccountDomain    = "accounts"
	FileStorageDomain    = "storage"
	FileCodeDomain       = "code"
	FileCommitmentDomain = "commitment"

	// Inverted indexes: File<identifier>Idx constant used
	// as part of the filenames in the /snapshots/idx dir.
	//
	// Must have corresponding constants in tables.go file in the format:
	//
	// Tbl<identifier>Keys
	// Tbl<identifier>Idx
	//
	// They correspond to the "hot" DB tables for these indexes.
	FileLogAddressIdx = "logaddrs"
	FileLogTopicsIdx  = "logtopics"
	FileTracesFromIdx = "tracesfrom"
	FileTracesToIdx   = "tracesto"
)
