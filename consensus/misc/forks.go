// Copyright 2017 The go-ethereum Authors
// This file is part of the go-ethereum library.
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

package misc

import (
	"fmt"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/core/types"
)

// VerifyForkHashes verifies that blocks conforming to network hard-forks do have
// the correct hashes, to avoid clients going off on different chains. This is an
// optional feature.
func VerifyForkHashes(config *chain.Config, header *types.Header, uncle bool) error {
	// We don't care about uncles
	if uncle {
		return nil
	}
	// If the homestead reprice hash is set, validate it
	if config.TangerineWhistleBlock != nil && config.TangerineWhistleBlock.Cmp(header.Number) == 0 {
		if config.TangerineWhistleHash != (libcommon.Hash{}) && config.TangerineWhistleHash != header.Hash() {
			return fmt.Errorf("tangerine whistle gas reprice fork: have 0x%x, want 0x%x", header.Hash(), config.TangerineWhistleHash)
		}
	}
	// All ok, return
	return nil
}
