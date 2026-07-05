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

package kv

import (
	"fmt"
	"slices"
	"strings"
)

// RetireCutoffs is the txNum below which frozen history files are retired,
// per domain (PerDomain, falling back to Default for other domains and standalone
// indices); a 0 cutoff keeps the entity. The aggregator floors each txNum to its
// file step — callers stay in txNum, the block↔txNum boundary's unit.
type RetireCutoffs struct {
	Default   uint64
	PerDomain map[Domain]uint64
}

// IsNoop reports whether every cutoff is 0, so nothing would be retired.
func (c RetireCutoffs) IsNoop() bool {
	if c.Default != 0 {
		return false
	}
	for _, txNum := range c.PerDomain {
		if txNum != 0 {
			return false
		}
	}
	return true
}

func (c RetireCutoffs) String() string {
	names := make([]Domain, 0, len(c.PerDomain))
	for name := range c.PerDomain {
		names = append(names, name)
	}
	slices.Sort(names)

	var sb strings.Builder
	fmt.Fprintf(&sb, "default=%d", c.Default)
	for _, name := range names {
		fmt.Fprintf(&sb, " %s=%d", name, c.PerDomain[name])
	}
	return sb.String()
}
