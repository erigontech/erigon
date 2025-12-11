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

package misc

import (
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/execution/chain/params"
	"github.com/erigontech/erigon/execution/consensus"
)

func ApplyBeaconRootEip4788(parentBeaconBlockRoot *common.Hash, syscall consensus.SystemCall, tracer *tracing.Hooks) {
	if tracer != nil && tracer.OnSystemCallStart != nil {
		tracer.OnSystemCallStart()
	}

	if tracer != nil && tracer.OnSystemCallEnd != nil {
		defer tracer.OnSystemCallEnd()
	}

	_, err := syscall(params.BeaconRootsAddress, parentBeaconBlockRoot.Bytes())
	if err != nil {
		log.Warn("Failed to call beacon roots contract", "err", err)
	}
}
