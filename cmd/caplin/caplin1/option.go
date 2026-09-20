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

func WithBuilder(mevRelayURL string, beaconConfig *clparams.BeaconChainConfig, policy builder.BuilderTargetPolicy) CaplinOption {
	return func(o *option) {
		if mevRelayURL == "" {
			o.builderClient = builder.NewDynamicBuilderClient(beaconConfig, policy)
			return
		}
		o.builderClient = builder.NewBlockBuilderClientWithPolicy(mevRelayURL, beaconConfig, policy)
	}
}

func builderOptionForConfig(config *clparams.CaplinConfig, beaconConfig *clparams.BeaconChainConfig) (CaplinOption, bool) {
	legacyRelayURL := ""
	skippedLegacy := false
	if config.BeaconAPIRouter.Builder {
		if config.RelayUrlExist() {
			legacyRelayURL = config.MevRelayUrl
		} else {
			config.BeaconAPIRouter.Builder = false
			skippedLegacy = true
		}
	}
	if !config.BeaconAPIRouter.Validator && !config.BeaconAPIRouter.Builder {
		return nil, skippedLegacy
	}
	return WithBuilder(legacyRelayURL, beaconConfig, builderTargetPolicyForConfig(config)), skippedLegacy
}

func builderTargetPolicyForConfig(config *clparams.CaplinConfig) builder.BuilderTargetPolicy {
	return builder.BuilderTargetPolicy{AllowPrivate: config != nil && config.AllowPrivateBuilderURLs}
}
