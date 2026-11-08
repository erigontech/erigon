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

// DefaultPruneDistance is the retention window used by full and blocks prune
// modes. The value (2^18 blocks ≈ 36.4 days) matches EIP-8252's
// REORG_RETENTION_WINDOW, the inactivity-leak-bounded non-finality window
// across which an EL must be able to reconstruct state to process a reorg
// without external sync.
const DefaultPruneDistance = 262_144

// MinimalPruneDistance is the retention window used by the minimal prune
// mode. Smaller than DefaultPruneDistance — minimal nodes deliberately opt
// out of EIP-8252 compliance in exchange for less disk usage.
const MinimalPruneDistance = 100_000
