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

package config3

import "math"

// Step size up to Erigon 3.3
const LegacyStepSize = 1_562_500

// Steps in frozen files up to Erigon 3.3
const LegacyStepsInFrozenFile = 64

// DefaultStepSize is the default number of transactions (txNums) in one "step".
// Prefer reading the actual step size from erigondb.toml via state.ResolveErigonDBSettings or tx.Debug().StepSize().
const DefaultStepSize = LegacyStepSize / 4 // 390_625

// DefaultStepsInFrozenFile - files of this size are completely frozen/immutable.
// files of smaller size are also immutable, but can be removed after merge to bigger files.
// Prefer reading the actual value from erigondb.toml via state.ResolveErigonDBSettings.
const DefaultStepsInFrozenFile = LegacyStepsInFrozenFile * 4 // 256

// UnboundedDomainMerge is a sentinel "steps in frozen file" value used to signal domain merges
// are unbounded, i.e., can be merged infinitely. This was the default behavior for Erigon <= 3.4 and
// this value can be used to restore it.
const UnboundedDomainMerge uint64 = math.MaxUint64

// DefaultReferencesInCommitmentBranches is the default when erigondb.toml omits it.
const DefaultReferencesInCommitmentBranches = false

// DefaultPruneDistance is the block and state-history window used by full mode,
// and the state-history window used by blocks mode. Its block count covers
// Ethereum mainnet's MIN_EPOCHS_FOR_BLOCK_REQUESTS window at the conservative
// maximum of one execution block per slot.
const DefaultPruneDistance = 1_100_000

// MinimalPruneDistance is the retention window used by the minimal prune
// mode. Smaller than DefaultPruneDistance — minimal nodes deliberately opt
// out of the default retention window in exchange for less disk usage.
const MinimalPruneDistance = 100_000
