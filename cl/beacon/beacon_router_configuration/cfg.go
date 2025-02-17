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

package beacon_router_configuration

import (
	"fmt"
	"strings"
	"time"
)

type RouterConfiguration struct {
	Active   bool
	Protocol string
	Address  string
	// Cors data
	AllowedOrigins   []string
	AllowedMethods   []string
	AllowCredentials bool

	ReadTimeTimeout time.Duration
	IdleTimeout     time.Duration
	WriteTimeout    time.Duration

	Beacon     bool
	Builder    bool
	Config     bool
	Debug      bool
	Events     bool
	Node       bool
	Validator  bool
	Lighthouse bool
}

func (r *RouterConfiguration) UnwrapEndpointsList(l []string) error {
	r.Active = len(l) > 0
	for _, v := range l {
		// convert v to lowercase
		v = strings.ToLower(v)
		switch v {
		case "beacon":
			r.Beacon = true
		case "builder":
			r.Builder = true
		case "config":
			r.Config = true
		case "debug":
			r.Debug = true
		case "events":
			r.Events = true
		case "node":
			r.Node = true
		case "validator":
			r.Validator = true
		case "lighthouse":
			r.Lighthouse = true
		default:
			r.Active = false
			r.Beacon = false
			r.Builder = false
			r.Config = false
			r.Debug = false
			r.Events = false
			r.Node = false
			r.Validator = false
			r.Lighthouse = false
			return fmt.Errorf("unknown endpoint for beacon.api: %s. known endpoints: beacon, builder, config, debug, events, node, validator, lighthouse", v)
		}
	}
	return nil
}
