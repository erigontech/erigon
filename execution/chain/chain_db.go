// Copyright 2021 The Erigon Authors
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

package chain

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/db/kv"
)

// GetConfig retrieves the consensus settings based on the given genesis hash.
func GetConfig(db kv.Getter, buf []byte) (*Config, error) {
	hash, err := CanonicalHash(db, 0, buf)
	if err != nil {
		return nil, fmt.Errorf("failed ReadCanonicalHash: %w", err)
	}
	if hash == nil {
		return nil, nil
	}
	data, err := db.GetOne(kv.ConfigTable, hash)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("invalid chain config JSON: %s, %w", data, err)
	}
	return &config, nil
}

func CanonicalHash(db kv.Getter, number uint64, buf []byte) ([]byte, error) {
	buf = common.EnsureEnoughSize(buf, 8)
	binary.BigEndian.PutUint64(buf, number)
	data, err := db.GetOne(kv.HeaderCanonical, buf)
	if err != nil {
		return nil, fmt.Errorf("failed CanonicalHash: %w, number=%d", err, number)
	}
	if len(data) == 0 {
		return nil, nil
	}

	return data, nil
}

// HeadHeaderHash retrieves the hash of the current canonical head header.
func HeadHeaderHash(db kv.Getter) ([]byte, error) {
	data, err := db.GetOne(kv.HeadHeaderKey, []byte(kv.HeadHeaderKey))
	if err != nil {
		return nil, fmt.Errorf("ReadHeadHeaderHash failed: %w", err)
	}
	return data, nil
}

func CurrentBlockNumber(db kv.Getter) (*uint64, error) {
	headHash, err := HeadHeaderHash(db)
	if err != nil {
		return nil, err
	}
	return HeaderNumber(db, headHash)
}

// HeaderNumber returns the header number assigned to a hash.
func HeaderNumber(db kv.Getter, hash []byte) (*uint64, error) {
	data, err := db.GetOne(kv.HeaderNumber, hash)
	if err != nil {
		return nil, fmt.Errorf("ReadHeaderNumber failed: %w", err)
	}
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) != 8 {
		return nil, fmt.Errorf("ReadHeaderNumber got wrong data len: %d", len(data))
	}
	number := binary.BigEndian.Uint64(data)
	return &number, nil
}
