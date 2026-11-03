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

package utils

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/erigontech/erigon/execution/types"
)

// ReadGenesisFile reads and validates a JSON genesis file, as used by
// `erigon init` and --override.genesis.
func ReadGenesisFile(path string) (*types.Genesis, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read genesis file: %w", err)
	}
	genesis := new(types.Genesis)
	if err := json.Unmarshal(raw, genesis); err != nil {
		return nil, fmt.Errorf("invalid genesis file %s: %w", path, err)
	}
	if genesis.Config == nil {
		return nil, fmt.Errorf("invalid genesis file %s: missing 'config'", path)
	}
	if genesis.Config.ChainID == nil {
		return nil, fmt.Errorf("invalid genesis file %s: missing 'config.chainId'", path)
	}

	// chain.Config has no 'bor' field, so a Polygon genesis would otherwise
	// initialise with its consensus settings silently dropped.
	var probe struct {
		Config struct {
			Bor json.RawMessage `json:"bor"`
		} `json:"config"`
	}
	if err := json.Unmarshal(raw, &probe); err == nil && len(probe.Config.Bor) > 0 && string(probe.Config.Bor) != "null" {
		return nil, fmt.Errorf("%s carries a 'bor' config: Polygon is not supported, see https://github.com/0xPolygon/erigon", path)
	}
	return genesis, nil
}
