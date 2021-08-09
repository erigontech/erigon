// Copyright 2018 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package rawdb

import (
	"encoding/json"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/params"
)

// ReadChainConfig retrieves the consensus settings based on the given genesis hash.
func ReadChainConfig(db kv.Getter, hash common.Hash) (*params.ChainConfig, error) {
	data, err := db.GetOne(kv.ConfigTable, hash[:])
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	var config params.ChainConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("invalid chain config JSON: %x, %w", hash, err)
	}
	return &config, nil
}

// WriteChainConfig writes the chain config settings to the database.
func WriteChainConfig(db kv.Putter, hash common.Hash, cfg *params.ChainConfig) error {
	if cfg == nil {
		return nil
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("failed to JSON encode chain config: %w", err)
	}
	if err := db.Put(kv.ConfigTable, hash[:], data); err != nil {
		return fmt.Errorf("failed to store chain config: %w", err)
	}
	return nil
}

// DeleteChainConfig retrieves the consensus settings based on the given genesis hash.
func DeleteChainConfig(db kv.Deleter, hash common.Hash) error {
	return db.Delete(kv.ConfigTable, hash[:], nil)
}
