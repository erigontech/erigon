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

package handler

import (
	"encoding/json"
	"fmt"
	"io"
	"slices"
)

// decodeJSONList decodes a JSON array of objects and rejects null elements.
func decodeJSONList[T any](body io.Reader) ([]*T, error) {
	var list []*T
	if err := json.NewDecoder(body).Decode(&list); err != nil {
		return nil, err
	}
	if err := checkNoNullElements(list); err != nil {
		return nil, err
	}
	return list, nil
}

// checkNoNullElements is needed because encoding/json stores a null array element as a nil
// pointer without calling the element's UnmarshalJSON, so per-type field checks never run.
func checkNoNullElements[T any](list []*T) error {
	if i := slices.Index(list, nil); i >= 0 {
		return fmt.Errorf("element at index %d is null", i)
	}
	return nil
}
