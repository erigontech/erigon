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

	"github.com/erigontech/erigon/erigon-lib/log/v3"

	"github.com/erigontech/erigon/erigon-lib/chain"
	libcommon "github.com/erigontech/erigon/erigon-lib/common"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	"github.com/erigontech/erigon/polygon/bor/valset"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/rlp"
)

//go:generate mockgen -typed=true -destination=./spanner_mock.go -package=bor . Spanner
type Spanner interface {
	GetCurrentSpan(syscall consensus.SystemCall) (*heimdall.Span, error)
	GetCurrentValidators(spanId uint64, chain ChainHeaderReader) ([]*valset.Validator, error)
	GetCurrentProducers(spanId uint64, chain ChainHeaderReader) ([]*valset.Validator, error)
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

	result, err := syscall(libcommon.HexToAddress(c.borConfig.ValidatorContract), data)
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
	// bor span with given ID
	BorSpan(spanId uint64) *heimdall.Span
	GetHeaderByNumber(number uint64) *types.Header
	GetHeader(hash libcommon.Hash, number uint64) *types.Header
	FrozenBlocks() uint64
}

func (c *ChainSpanner) GetCurrentValidators(spanId uint64, chain ChainHeaderReader) ([]*valset.Validator, error) {
	// Use hardcoded bor devnet valset if chain-name = bor-devnet
	if NetworkNameVals[c.chainConfig.ChainName] != nil && c.withoutHeimdall {
		return NetworkNameVals[c.chainConfig.ChainName], nil
	}

	span := chain.BorSpan(spanId)

	return span.ValidatorSet.Validators, nil
}

func (c *ChainSpanner) GetCurrentProducers(spanId uint64, chain ChainHeaderReader) ([]*valset.Validator, error) {
	// Use hardcoded bor devnet valset if chain-name = bor-devnet
	if NetworkNameVals[c.chainConfig.ChainName] != nil && c.withoutHeimdall {
		return NetworkNameVals[c.chainConfig.ChainName], nil
	}

	span := chain.BorSpan(spanId)

	producers := make([]*valset.Validator, len(span.SelectedProducers))
	for i := range span.SelectedProducers {
		producers[i] = &span.SelectedProducers[i]
	}

	return producers, nil
}

func (c *ChainSpanner) CommitSpan(heimdallSpan heimdall.Span, syscall consensus.SystemCall) error {
	// method
	const method = "commitSpan"

	// get validators bytes
	validators := make([]valset.MinimalVal, 0, len(heimdallSpan.ValidatorSet.Validators))
	for _, val := range heimdallSpan.ValidatorSet.Validators {
		validators = append(validators, val.MinimalVal())
	}
	validatorBytes, err := rlp.EncodeToBytes(validators)
	if err != nil {
		return err
	}

	// get producers bytes
	producers := make([]valset.MinimalVal, 0, len(heimdallSpan.SelectedProducers))
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

	_, err = syscall(libcommon.HexToAddress(c.borConfig.ValidatorContract), data)

	return err
}
