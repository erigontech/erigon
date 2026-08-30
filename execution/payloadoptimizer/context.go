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

package payloadoptimizer

import (
	"errors"
	"reflect"

	"github.com/erigontech/erigon/execution/builder"
	protocolparams "github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
)

var ErrCustomTxnProvider = errors.New("payload optimizer build context cannot contain a custom transaction provider")

type BuildContext struct {
	params            *builder.Parameters
	forkVersion       [4]byte
	executionRequests types.FlatRequests
	parentGasLimit    uint64
}

func NewBuildContext(params *builder.Parameters, forkVersion [4]byte, executionRequests types.FlatRequests, parentGasLimit uint64) (BuildContext, error) {
	if params == nil {
		return BuildContext{}, errors.New("payload optimizer build context requires parameters")
	}
	if params.CustomTxnProvider != nil {
		return BuildContext{}, ErrCustomTxnProvider
	}
	if parentGasLimit < protocolparams.MinBlockGasLimit || parentGasLimit > protocolparams.MaxBlockGasLimit {
		return BuildContext{}, errors.New("payload optimizer build context has an invalid parent gas limit")
	}
	for _, withdrawal := range params.Withdrawals {
		if withdrawal == nil {
			return BuildContext{}, errors.New("payload optimizer build context contains a nil withdrawal")
		}
	}
	owned := params.Copy()
	owned.PayloadId = 0
	return BuildContext{
		params:            owned,
		forkVersion:       forkVersion,
		executionRequests: copyRequests(executionRequests),
		parentGasLimit:    parentGasLimit,
	}, nil
}

func (c BuildContext) Parameters() *builder.Parameters {
	if c.params == nil {
		return nil
	}
	return c.params.Copy()
}

func (c BuildContext) ForkVersion() [4]byte {
	return c.forkVersion
}

func (c BuildContext) ExecutionRequests() types.FlatRequests {
	return copyRequests(c.executionRequests)
}

func (c BuildContext) ParentGasLimit() uint64 {
	return c.parentGasLimit
}

func (c BuildContext) Equal(other BuildContext) bool {
	return c.params != nil && other.params != nil &&
		c.forkVersion == other.forkVersion &&
		c.parentGasLimit == other.parentGasLimit &&
		reflect.DeepEqual(c.params, other.params) &&
		reflect.DeepEqual(c.executionRequests, other.executionRequests)
}

func (c BuildContext) clone() BuildContext {
	return BuildContext{
		params:            c.Parameters(),
		forkVersion:       c.forkVersion,
		executionRequests: copyRequests(c.executionRequests),
		parentGasLimit:    c.parentGasLimit,
	}
}

func copyRequests(requests types.FlatRequests) types.FlatRequests {
	if requests == nil {
		return nil
	}
	owned := make(types.FlatRequests, len(requests))
	for i := range requests {
		owned[i].Type = requests[i].Type
		owned[i].RequestData = append([]byte(nil), requests[i].RequestData...)
	}
	return owned
}
