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

package txpool

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
)

var PoolChainConfigKey = []byte("chain_config")
var PoolLastSeenBlockKey = []byte("last_seen_block")
var PoolPendingBaseFeeKey = []byte("pending_base_fee")
var PoolPendingBlobFeeKey = []byte("pending_blob_fee")
var PoolStateVersion = []byte("state_version")

func getExecutionProgress(db kv.Getter) (uint64, error) {
	data, err := db.GetOne(kv.SyncStageProgress, []byte("Execution"))
	if err != nil {
		return 0, err
	}

	if len(data) == 0 {
		return 0, nil
	}

	if len(data) < 8 {
		return 0, fmt.Errorf("value must be at least 8 bytes, got %d", len(data))
	}

	return binary.BigEndian.Uint64(data[:8]), nil
}

func LastSeenBlock(tx kv.Getter) (uint64, error) {
	v, err := tx.GetOne(kv.PoolInfo, PoolLastSeenBlockKey)
	if err != nil {
		return 0, err
	}
	if len(v) == 0 {
		return 0, nil
	}
	return binary.BigEndian.Uint64(v), nil
}

func PutLastSeenBlock(tx kv.Putter, n uint64, buf []byte) error {
	buf = common.EnsureEnoughSize(buf, 8)
	binary.BigEndian.PutUint64(buf, n)
	err := tx.Put(kv.PoolInfo, PoolLastSeenBlockKey, buf)
	if err != nil {
		return err
	}
	return nil
}

func ChainConfig(tx kv.Getter) (*chain.Config, error) {
	v, err := tx.GetOne(kv.PoolInfo, PoolChainConfigKey)
	if err != nil {
		return nil, err
	}
	if len(v) == 0 {
		return nil, nil
	}
	var config chain.Config
	if err := json.Unmarshal(v, &config); err != nil {
		return nil, fmt.Errorf("invalid chain config JSON in pool db: %w", err)
	}
	return &config, nil
}

func PutChainConfig(tx kv.Putter, cc *chain.Config, buf []byte) error {
	wr := bytes.NewBuffer(buf)
	if err := json.NewEncoder(wr).Encode(cc); err != nil {
		return fmt.Errorf("invalid chain config JSON in pool db: %w", err)
	}
	if err := tx.Put(kv.PoolInfo, PoolChainConfigKey, wr.Bytes()); err != nil {
		return err
	}
	return nil
}

func SaveChainConfigIfNeed(
	ctx context.Context,
	coreDB kv.RoDB,
	poolDB kv.RwDB,
	force bool,
	logger log.Logger,
) (cc *chain.Config, blockNum uint64, err error) {
	if err = poolDB.View(ctx, func(tx kv.Tx) error {
		cc, err = ChainConfig(tx)
		if err != nil {
			return err
		}
		blockNum, err = LastSeenBlock(tx)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, 0, err
	}
	if cc != nil && !force {
		if cc.ChainID.Uint64() == 0 {
			return nil, 0, errors.New("wrong chain config")
		}
		return cc, blockNum, nil
	}

	for {
		if err = coreDB.View(ctx, func(tx kv.Tx) error {
			cc, err = chain.GetConfig(tx, nil)
			if err != nil {
				return err
			}
			n, err := chain.CurrentBlockNumber(tx)
			if err != nil {
				return err
			}
			if n != nil {
				blockNum = *n
			}
			return nil
		}); err != nil {
			logger.Error("cant read chain config from core db", "err", err)
			time.Sleep(5 * time.Second)
			continue
		} else if cc == nil {
			logger.Error("cant read chain config from core db")
			time.Sleep(5 * time.Second)
			continue
		}
		break
	}

	if err = poolDB.Update(ctx, func(tx kv.RwTx) error {
		if err = PutChainConfig(tx, cc, nil); err != nil {
			return err
		}
		if err = PutLastSeenBlock(tx, blockNum, nil); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, 0, err
	}
	if cc.ChainID.Uint64() == 0 {
		return nil, 0, errors.New("wrong chain config")
	}
	return cc, blockNum, nil
}
