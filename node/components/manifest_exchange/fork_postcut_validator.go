// Copyright 2026 The Erigon Authors
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

package manifest_exchange

import (
	"github.com/erigontech/erigon/db/downloader"
)

// BuildForkPostCutValidator constructs a ForkPostCutValidatorFn from
// the local fork chain's cut coordinates. It is the consumer-side
// mirror of RollingV2Publisher.SetForkCutBlock: a fork publisher must
// not advertise pre-cut files, and a fork follower must reject any
// peer manifest that carries them (per fork-spec.md § Identification
// and memory/fork-trust-cascade-ve-tests-2026-05-24).
//
// cutBlock == 0 disables the check by returning nil — a follower on a
// root chain has no cut coordinate and MUST NOT apply this validator.
// The Provider wiring treats nil as "not installed" and passes peer
// manifests through unchanged, preserving root-chain behaviour.
//
// stepToBlock is the parent chain's step→block mapping; empty is the
// safe default (unmapped state files classify as Straddle → reject).
func BuildForkPostCutValidator(cutBlock uint64, stepToBlock downloader.StepToBlock) ForkPostCutValidatorFn {
	if cutBlock == 0 {
		return nil
	}
	return func(m *downloader.ChainTomlV2) error {
		return downloader.ValidateForkManifestPostCutOnly(m, cutBlock, stepToBlock)
	}
}
