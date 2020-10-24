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
	"errors"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

// ReadDatabaseVersion retrieves the version number of the database.
func ReadDatabaseVersion(db DatabaseReader) *uint64 {
	var version uint64

	enc, _ := db.Get(dbutils.DatabaseVerisionKey, []byte(dbutils.DatabaseVerisionKey))
	if len(enc) == 0 {
		return nil
	}
	if err := rlp.DecodeBytes(enc, &version); err != nil {
		return nil
	}

	return &version
}

// WriteDatabaseVersion stores the version number of the database
func WriteDatabaseVersion(db DatabaseWriter, version uint64) error {
	enc, err := rlp.EncodeToBytes(version)
	if err != nil {
		return fmt.Errorf("failed to encode database version: %w", err)
	}
	if err = db.Put(dbutils.DatabaseVerisionKey, []byte(dbutils.DatabaseVerisionKey), enc); err != nil {
		return fmt.Errorf("failed to store the database version: %w", err)
	}
	return nil
}

// ReadChainConfig retrieves the consensus settings based on the given genesis hash.
func ReadChainConfig(db DatabaseReader, hash common.Hash) (*params.ChainConfig, error) {
	data, err := db.Get(dbutils.ConfigPrefix, hash[:])
	if err != nil && errors.Is(err, ethdb.ErrKeyNotFound) {
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
func WriteChainConfig(db DatabaseWriter, hash common.Hash, cfg *params.ChainConfig) error {
	if cfg == nil {
		return nil
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("failed to JSON encode chain config: %w", err)
	}
	if err := db.Put(dbutils.ConfigPrefix, hash[:], data); err != nil {
		return fmt.Errorf("failed to store chain config: %w", err)
	}
	return nil
}

// ReadPreimage retrieves a single preimage of the provided hash.
func ReadPreimage(db DatabaseReader, hash common.Hash) []byte {
	data, _ := db.Get(dbutils.PreimagePrefix, hash.Bytes())
	return data
}

// WritePreimages writes the provided set of preimages to the database.
func WritePreimages(db DatabaseWriter, preimages map[common.Hash][]byte) {
	for hash, preimage := range preimages {
		if err := db.Put(dbutils.PreimagePrefix, hash.Bytes(), preimage); err != nil {
			log.Crit("Failed to store trie preimage", "err", err)
		}
	}
	dbutils.PreimageCounter.Inc(int64(len(preimages)))
	dbutils.PreimageHitCounter.Inc(int64(len(preimages)))
}
