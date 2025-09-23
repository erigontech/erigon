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

package bor

import (
	"encoding/hex"
	"math/big"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	"github.com/erigontech/erigon/polygon/heimdall"
)

//go:generate mockgen -typed=true -destination=./spanner_mock.go -package=bor . Spanner
type Spanner interface {
	GetCurrentSpan(syscall consensus.SystemCall) (*heimdall.Span, error)
	CommitSpan(heimdallSpan heimdall.Span, syscall consensus.SystemCall) error
}

type ABI interface {
	Pack(name string, args ...interface{}) ([]byte, error)
	UnpackIntoInterface(v interface{}, name string, data []byte) error
}

type ChainSpanner struct {
	validatorSet    ABI
	chainConfig     *chain.Config
	borConfig       *borcfg.BorConfig
	logger          log.Logger
	withoutHeimdall bool
}

func NewChainSpanner(validatorSet ABI, chainConfig *chain.Config, withoutHeimdall bool, logger log.Logger) *ChainSpanner {
	borConfig := chainConfig.Bor.(*borcfg.BorConfig)
	return &ChainSpanner{
		validatorSet:    validatorSet,
		chainConfig:     chainConfig,
		borConfig:       borConfig,
		logger:          logger,
		withoutHeimdall: withoutHeimdall,
	}
}

// GetCurrentSpan get current span from contract
func (c *ChainSpanner) GetCurrentSpan(syscall consensus.SystemCall) (*heimdall.Span, error) {
	// method
	const method = "getCurrentSpan"

	data, err := c.validatorSet.Pack(method)
	if err != nil {
		c.logger.Error("[bor] Unable to pack txn for getCurrentSpan", "error", err)
		return nil, err
	}

	result, err := syscall(common.HexToAddress(c.borConfig.ValidatorContract), data)
	if err != nil {
		return nil, err
	}

	// span result
	ret := new(struct {
		Number     *big.Int
		StartBlock *big.Int
		EndBlock   *big.Int
	})

	if err := c.validatorSet.UnpackIntoInterface(ret, method, result); err != nil {
		return nil, err
	}

	// create new span
	span := heimdall.Span{
		Id:         heimdall.SpanId(ret.Number.Uint64()),
		StartBlock: ret.StartBlock.Uint64(),
		EndBlock:   ret.EndBlock.Uint64(),
	}

	return &span, nil
}

type ChainHeaderReader interface {
	GetHeaderByNumber(number uint64) *types.Header
	GetHeader(hash common.Hash, number uint64) *types.Header
	FrozenBlocks() uint64
}

func (c *ChainSpanner) CommitSpan(heimdallSpan heimdall.Span, syscall consensus.SystemCall) error {
	// method
	const method = "commitSpan"

	// get validators bytes
	validators := make([]heimdall.MinimalVal, 0, len(heimdallSpan.ValidatorSet.Validators))
	for _, val := range heimdallSpan.ValidatorSet.Validators {
		validators = append(validators, val.MinimalVal())
	}
	validatorBytes, err := rlp.EncodeToBytes(validators)
	if err != nil {
		return err
	}

	// get producers bytes
	producers := make([]heimdall.MinimalVal, 0, len(heimdallSpan.SelectedProducers))
	for _, val := range heimdallSpan.SelectedProducers {
		producers = append(producers, val.MinimalVal())
	}
	producerBytes, err := rlp.EncodeToBytes(producers)
	if err != nil {
		return err
	}

	c.logger.Trace("[bor] ✅ Committing new span",
		"id", heimdallSpan.Id,
		"startBlock", heimdallSpan.StartBlock,
		"endBlock", heimdallSpan.EndBlock,
		"validatorBytes", hex.EncodeToString(validatorBytes),
		"producerBytes", hex.EncodeToString(producerBytes),
	)

	// get packed data
	data, err := c.validatorSet.Pack(method,
		big.NewInt(0).SetUint64(uint64(heimdallSpan.Id)),
		big.NewInt(0).SetUint64(heimdallSpan.StartBlock),
		big.NewInt(0).SetUint64(heimdallSpan.EndBlock),
		validatorBytes,
		producerBytes,
	)
	if err != nil {
		c.logger.Error("[bor] Unable to pack txn for commitSpan", "error", err)
		return err
	}

	_, err = syscall(common.HexToAddress(c.borConfig.ValidatorContract), data)

	return err
}
