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
// Package config holds the configuration of an EEZ network that the protocol
// does not fix: the l2 section of its chain configuration.
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
)

// Name identifies the EEZ stack in chain configurations and selects its rules
// engine.
const Name = "eez"

// ErrInvalid reports an l2 section that does not describe a usable network.
var ErrInvalid = errors.New("invalid eez l2 config")

// L2Config is the l2 section of an EEZ chain configuration. Chain id, fork
// schedule and fee collector come from the standard genesis fields; protocol
// constants live in eez/protocol.
type L2Config struct {
	// RollupID is the rollup id the EEZ registry assigned to this network.
	RollupID uint64 `json:"rollupId"`
	// L1ChainID is the chain id of the settlement layer.
	L1ChainID uint64 `json:"l1ChainId"`
	// Registry is the EEZ registry contract on L1.
	Registry common.Address `json:"eezRegistry"`
	// RegistryDeployBlock is the L1 block the registry was deployed in, where
	// settlement scanning starts.
	RegistryDeployBlock uint64 `json:"registryDeployBlock"`
	// BlockInterval is the L2 block time in seconds.
	BlockInterval uint64 `json:"blockInterval"`
	// L1BlockTime is the L1 block time in seconds; it is a multiple of
	// BlockInterval, so every L1 block spans the same number of L2 blocks.
	L1BlockTime uint64 `json:"l1BlockTime"`
	// BlockGasLimit is the gas limit of every L2 block.
	BlockGasLimit uint64 `json:"blockGasLimit"`
}

var _ chain.L2Config = (*L2Config)(nil)

// Name returns the EEZ stack name.
func (c *L2Config) Name() string { return Name }

// ResolveRules keeps the standard rules: EEZ networks activate forks by the
// timestamps of their genesis, without a version ladder of their own.
func (c *L2Config) ResolveRules(l2Version, blockNum, blockTime uint64, rules *chain.Rules) {}

// BlocksPerL1Block is the number of L2 blocks one L1 block spans.
func (c *L2Config) BlocksPerL1Block() uint64 { return c.L1BlockTime / c.BlockInterval }

// Validate rejects a configuration no EEZ network can run with.
func (c *L2Config) Validate() error {
	switch {
	case c.RollupID == 0:
		return fmt.Errorf("%w: rollupId is zero", ErrInvalid)
	case c.L1ChainID == 0:
		return fmt.Errorf("%w: l1ChainId is zero", ErrInvalid)
	case c.Registry == (common.Address{}):
		return fmt.Errorf("%w: eezRegistry is the zero address", ErrInvalid)
	case c.BlockInterval == 0 || c.L1BlockTime == 0:
		return fmt.Errorf("%w: blockInterval and l1BlockTime must be positive", ErrInvalid)
	case c.L1BlockTime%c.BlockInterval != 0:
		return fmt.Errorf("%w: l1BlockTime %d is not a multiple of blockInterval %d", ErrInvalid, c.L1BlockTime, c.BlockInterval)
	case c.BlocksPerL1Block() < 2:
		// The Sync block must be distinct from the blocks around it.
		return fmt.Errorf("%w: %d L2 blocks per L1 block, need at least 2", ErrInvalid, c.BlocksPerL1Block())
	case c.BlockGasLimit == 0:
		return fmt.Errorf("%w: blockGasLimit is zero", ErrInvalid)
	}
	return nil
}

type l2JSON struct {
	Name string `json:"name"`
	L2Config
}

// FromJSON decodes and validates an l2 section. Unknown fields and a name
// other than eez are rejected.
func FromJSON(raw []byte) (*L2Config, error) {
	var v l2JSON
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	if dec.More() {
		return nil, fmt.Errorf("%w: trailing data", ErrInvalid)
	}
	if v.Name != Name {
		return nil, fmt.Errorf("%w: name %q, want %q", ErrInvalid, v.Name, Name)
	}
	if err := v.L2Config.Validate(); err != nil {
		return nil, err
	}
	return &v.L2Config, nil
}

// MarshalJSON encodes the section with its name, as FromJSON reads it.
func (c *L2Config) MarshalJSON() ([]byte, error) {
	return json.Marshal(l2JSON{Name: Name, L2Config: *c})
}
