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

package builder

import (
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider"
)

// ScopedReadView is a consistent, by-block read snapshot handed to the builder
// so it reads the exact block it builds on (never the raw DB directly nor a
// mutable global). Implemented by execmodule.ScopedReadView. The build owns it
// and releases it when it finishes.
type ScopedReadView interface {
	Tx() kv.TemporalTx
	HeadSD() *execctx.SharedDomains
	BlockHash() common.Hash
	BlockNum() uint64
	Release()
}

// Parameters for PoS block building
// See also https://github.com/ethereum/execution-apis/blob/main/src/engine/amsterdam.md#payloadattributesv4
type Parameters struct {
	// ScopedView is the consistent read snapshot pinned to ParentHash. Set by
	// AssembleBlock; the build reads only through it and releases it on finish.
	ScopedView ScopedReadView

	PayloadId             uint64
	ParentHash            common.Hash
	Timestamp             uint64
	PrevRandao            common.Hash
	SuggestedFeeRecipient common.Address
	Withdrawals           []*types.Withdrawal // added in Shapella (EIP-4895)
	ParentBeaconBlockRoot *common.Hash        // added in Dencun (EIP-4788)
	SlotNumber            *uint64             // added in Amsterdam (EIP-7843)
	TargetGasLimit        *uint64             // added in Gloas (EIP-7732)
	// CustomTxnProvider overrides the block's transaction source when non-nil.
	// nil → use the injected TxnProvider (normal mempool path)
	CustomTxnProvider txnprovider.TxnProvider
	// ExtraData overrides the builder's configured extra data when non-nil.
	ExtraData []byte
}
