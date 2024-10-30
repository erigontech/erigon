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

package sync

import (
	"context"
	"time"

	lru "github.com/hashicorp/golang-lru/arc/v2"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/v3/core/types"
	"github.com/erigontech/erigon/v3/polygon/bor"
	"github.com/erigontech/erigon/v3/polygon/bor/borcfg"
)

type HeaderTimeValidator struct {
	borConfig            *borcfg.BorConfig
	signaturesCache      *lru.ARCCache[libcommon.Hash, libcommon.Address]
	blockProducersReader blockProducersReader
}

func (htv *HeaderTimeValidator) ValidateHeaderTime(
	ctx context.Context,
	header *types.Header,
	now time.Time,
	parent *types.Header,
) error {
	headerNum := header.Number.Uint64()
	producers, err := htv.blockProducersReader.Producers(ctx, headerNum)
	if err != nil {
		return err
	}

	return bor.ValidateHeaderTime(header, now, parent, producers, htv.borConfig, htv.signaturesCache)
}
