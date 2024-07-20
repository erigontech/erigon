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

package caplin1

import (
	"github.com/erigontech/erigon/cl/beacon/builder"
	"github.com/erigontech/erigon/cl/clparams"
)

type option struct {
	builderClient builder.BuilderClient
}

type CaplinOption func(*option)

func WithBuilder(mevRelayUrl string, beaconConfig *clparams.BeaconChainConfig) CaplinOption {
	return func(o *option) {
		o.builderClient = builder.NewBlockBuilderClient(mevRelayUrl, beaconConfig)
	}
}
