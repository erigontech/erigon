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

package jsonrpc

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/rpc"
)

// WitnessSubscriptionOpts are the optional debug_subscribe("executionWitnesses") params.
// Encoding reserves a slot for a future "rlp" form; "" and "json" both select JSON.
type WitnessSubscriptionOpts struct {
	Encoding string `json:"encoding"`
}

// WitnessNotification is one debug_subscription("executionWitnesses") payload: the
// completed block's number and hash plus its witness as raw pre-marshaled JSON.
type WitnessNotification struct {
	BlockNumber hexutil.Uint64  `json:"blockNumber"`
	BlockHash   common.Hash     `json:"blockHash"`
	Witness     json.RawMessage `json:"witness"`
}

func validateWitnessEncoding(opts *WitnessSubscriptionOpts) error {
	if opts == nil {
		return nil
	}
	switch opts.Encoding {
	case "", "json":
		return nil
	default:
		return fmt.Errorf("unsupported witness encoding %q (supported: json)", opts.Encoding)
	}
}

// ExecutionWitnesses implements debug_subscribe("executionWitnesses"): every witness the
// eager cache builds after the subscription starts is pushed as a subscription
// notification. Fresh-only and stateless — no past witnesses are replayed; behind-tip
// catch-up stays on the debug_executionWitness request path.
func (api *DebugAPIImpl) ExecutionWitnesses(ctx context.Context, opts *WitnessSubscriptionOpts) (*rpc.Subscription, error) {
	if err := validateWitnessEncoding(opts); err != nil {
		return nil, err
	}
	if api.witnessCache == nil {
		return nil, fmt.Errorf("executionWitnesses subscription requires the embedded eager witness cache (start the node with --witness.cache.blocks); use debug_executionWitness for on-demand witnesses")
	}
	return subscribeRPC(ctx,
		func() (<-chan witnessPush, func(), error) {
			ch := api.witnessCache.subscribe()
			return ch, func() { api.witnessCache.unsubscribe(ch) }, nil
		},
		func(emit func(payload any), p witnessPush) {
			emit(WitnessNotification{
				BlockNumber: hexutil.Uint64(p.num),
				BlockHash:   p.hash,
				Witness:     p.json,
			})
		},
		"[witness-feed] witness push channel was closed")
}
