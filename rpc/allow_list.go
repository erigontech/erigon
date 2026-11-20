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

package rpc

import (
	"encoding/json"
	"maps"
	"slices"
)

type AllowList map[string]struct{}

func (a *AllowList) UnmarshalJSON(data []byte) error {
	var keys []string
	err := json.Unmarshal(data, &keys)
	if err != nil {
		return err
	}

	*a = make(AllowList, len(keys))
	for _, k := range keys {
		(*a)[k] = struct{}{}
	}

	return nil
}

// MarshalJSON returns *m as the JSON encoding of
func (a *AllowList) MarshalJSON() ([]byte, error) {
	return json.Marshal(slices.Collect(maps.Keys(*a)))
}

type ForbiddenList map[string]struct{}
