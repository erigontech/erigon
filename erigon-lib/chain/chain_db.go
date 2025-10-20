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
	"math/big"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/arb/chain/types"
	"github.com/erigontech/erigon/arb/osver"
)

// TODO remove this
func ArbitrumOneChainConfig() *Config {
	return &Config{
		ChainID:             big.NewInt(42161),
		HomesteadBlock:      big.NewInt(0),
		DAOForkBlock:        nil,
		ByzantiumBlock:      big.NewInt(0),
		ConstantinopleBlock: big.NewInt(0),
		PetersburgBlock:     big.NewInt(0),
		IstanbulBlock:       big.NewInt(0),
		MuirGlacierBlock:    big.NewInt(0),
		BerlinBlock:         big.NewInt(0),
		LondonBlock:         big.NewInt(0),
		ArbitrumChainParams: ArbitrumOneParams(),
		Clique: &CliqueConfig{
			Period: 0,
			Epoch:  0,
		},
	}
}

func ArbitrumOneParams() types.ArbitrumChainParams {
	return types.ArbitrumChainParams{
		ParentChainID:             1,
		ParentChainIsArbitrum:     false,
		ChainName:                 "arb1",
		SequencerURL:              "https://arb1-sequencer.arbitrum.io/rpc",
		FeedURL:                   "wss://arb1-feed.arbitrum.io/feed",
		EnableArbOS:               true,
		AllowDebugPrecompiles:     false,
		DataAvailabilityCommittee: false,
		InitialArbOSVersion:       osver.ArbosVersion_6,
		InitialChainOwner:         common.HexToAddress("0xd345e41ae2cb00311956aa7109fc801ae8c81a52"),
		GenesisBlockNum:           22207817,
		GenesisTxNum:              69372193, // non-canonical (E3) txnumber of first txn in given block

		Rollup: types.ArbRollupConfig{
			Bridge:                 "0x8315177ab297ba92a06054ce80a67ed4dbd7ed3a",
			Inbox:                  "0x4dbd4fc535ac27206064b68ffcf827b0a60bab3f",
			SequencerInbox:         "0x1c479675ad559dc151f6ec7ed3fbf8cee79582b6",
			Rollup:                 "0x5ef0d09d1e6204141b4d37530808ed19f60fba35",
			ValidatorUtils:         "0x9e40625f52829cf04bc4839f186d621ee33b0e67",
			ValidatorWalletCreator: "0x960953f7c69cd2bc2322db9223a815c680ccc7ea",
			StakeToken:             "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
			DeployedAt:             15411056,
		},
	}
}

// GetConfig retrieves the consensus settings based on the given genesis hash.
func GetConfig(db kv.Getter, buf []byte) (*Config, error) {
	return ArbitrumOneChainConfig(), nil

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
