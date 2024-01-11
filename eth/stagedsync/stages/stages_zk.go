// Copyright 2023 The Erigon Authors
// This file is part of the Erigon library.
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

package stages

var (
	// ZK stages
	L1Syncer                    SyncStage = "L1Syncer"
	L1VerificationsBatchNo      SyncStage = "L1VerificationsBatchNo"
	Batches                     SyncStage = "Batches"
	HighestHashableL2BlockNo    SyncStage = "HighestHashableL2BlockNo"
	HighestSeenBatchNumber      SyncStage = "HighestSeenBatchNumber"
	VerificationsStateRootCheck SyncStage = "VerificationStateRootCheck"
	ForkId                      SyncStage = "ForkId"
)
