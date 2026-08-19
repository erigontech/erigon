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

package execution_client

import (
	"fmt"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/execution/types"
)

// DecodeAndValidateBlockAccessList decodes and validates a Caplin execution payload's BAL.
func DecodeAndValidateBlockAccessList(payload *cltypes.Eth1Block) (*types.BlockAccessListSidecar, error) {
	if payload.Version() < clparams.GloasVersion || payload.BlockAccessList == nil {
		return nil, nil
	}
	bal, err := types.DecodeBlockAccessListSidecar(payload.BlockAccessList.Bytes())
	if err != nil {
		return nil, fmt.Errorf("decode block access list: %w", err)
	}
	if err = bal.ValidateForBlock(payload.GasLimit); err != nil {
		return nil, fmt.Errorf("validate block access list: %w", err)
	}
	return bal, nil
}
