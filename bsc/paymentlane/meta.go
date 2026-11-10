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

package paymentlane

import (
	_ "embed"
	"fmt"
	"math/big"
	"strings"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/abi"
)

const pageSize uint64 = 128

// Meta is the lane metadata read from the parent post-state.
type Meta struct {
	ratio  uint64
	listed map[common.Address]struct{}
}

func (m *Meta) Quota(gasLimit uint64) uint64 { return quota(m.ratio, gasLimit) }

func (m *Meta) Listed() map[common.Address]struct{} { return m.listed }

type getter func(input []byte) ([]byte, error)

// LoadMeta reads the ratio and the listed contracts through call, which runs a PaymentLane
// view function on the parent post-state.
func LoadMeta(call func(input []byte) ([]byte, error)) (*Meta, error) {
	ratio, err := loadRatio(call)
	if err != nil {
		return nil, err
	}
	listed, err := loadListed(call)
	if err != nil {
		return nil, err
	}
	return &Meta{ratio: ratio, listed: listed}, nil
}

func loadRatio(call getter) (uint64, error) {
	ret, err := callGetter(call, packGetPaymentLaneRatio())
	if err != nil {
		return 0, err
	}
	return unpackGetPaymentLaneRatio(ret)
}

func loadListed(call getter) (map[common.Address]struct{}, error) {
	ret, err := callGetter(call, packGetPaymentContracts(0, pageSize))
	if err != nil {
		return nil, err
	}
	page, total, err := unpackGetPaymentContracts(ret)
	if err != nil {
		return nil, err
	}
	if total > maxListedContracts {
		return nil, fmt.Errorf("%w: getPaymentContracts totalLength %d exceeds limit %d", ErrCorruptConfig, total, maxListedContracts)
	}
	if total == 0 {
		return nil, nil
	}
	if len(page) == 0 {
		return nil, fmt.Errorf("%w: getPaymentContracts returned an empty first page for totalLength %d", ErrCorruptConfig, total)
	}
	listed := make(map[common.Address]struct{})
	if err := appendPage(listed, 0, page, total); err != nil {
		return nil, err
	}
	for offset := uint64(len(page)); offset < total; {
		ret, err := callGetter(call, packGetPaymentContracts(offset, pageSize))
		if err != nil {
			return nil, err
		}
		page, nextTotal, err := unpackGetPaymentContracts(ret)
		if err != nil {
			return nil, err
		}
		if nextTotal != total {
			return nil, fmt.Errorf("%w: getPaymentContracts totalLength changed from %d to %d", ErrCorruptConfig, total, nextTotal)
		}
		if len(page) == 0 {
			return nil, fmt.Errorf("%w: getPaymentContracts returned an empty page at offset %d of %d", ErrCorruptConfig, offset, total)
		}
		if err := appendPage(listed, offset, page, total); err != nil {
			return nil, err
		}
		offset += uint64(len(page))
	}
	if uint64(len(listed)) != total {
		return nil, fmt.Errorf("%w: listed set size %d, want %d", ErrCorruptConfig, len(listed), total)
	}
	return listed, nil
}

func appendPage(listed map[common.Address]struct{}, offset uint64, page []common.Address, total uint64) error {
	if uint64(len(page)) > pageSize {
		return fmt.Errorf("%w: getPaymentContracts page at offset %d returned %d entries, limit %d", ErrCorruptConfig, offset, len(page), pageSize)
	}
	if offset > total {
		return fmt.Errorf("%w: page offset %d exceeds totalLength %d", ErrCorruptConfig, offset, total)
	}
	if uint64(len(page)) > total-offset {
		return fmt.Errorf("%w: page offset %d length %d exceeds totalLength %d", ErrCorruptConfig, offset, len(page), total)
	}
	for i, addr := range page {
		if _, dup := listed[addr]; dup {
			return fmt.Errorf("%w: getPaymentContracts duplicate %x at %d", ErrCorruptConfig, addr, offset+uint64(i))
		}
		listed[addr] = struct{}{}
	}
	return nil
}

func callGetter(call getter, input []byte) ([]byte, error) {
	ret, err := call(input)
	if err != nil {
		return nil, fmt.Errorf("%w: payment lane getter %x: %w", ErrCorruptConfig, input[:4], err)
	}
	return ret, nil
}

const (
	getPaymentLaneRatioMethod = "getPaymentLaneRatio"
	getPaymentContractsMethod = "getPaymentContracts"
)

//go:embed payment_lane_meta.abi.json
var paymentLaneMetaABIJSON string

var paymentLaneMetaABI = func() abi.ABI {
	parsed, err := abi.JSON(strings.NewReader(paymentLaneMetaABIJSON))
	if err != nil {
		panic(err)
	}
	return parsed
}()

func mustPack(name string, args ...any) []byte {
	input, err := paymentLaneMetaABI.Pack(name, args...)
	if err != nil {
		panic(err)
	}
	return input
}

func packGetPaymentLaneRatio() []byte {
	return mustPack(getPaymentLaneRatioMethod)
}

func packGetPaymentContracts(offset, limit uint64) []byte {
	return mustPack(getPaymentContractsMethod, new(big.Int).SetUint64(offset), new(big.Int).SetUint64(limit))
}

func unpackGetPaymentLaneRatio(ret []byte) (uint64, error) {
	values, err := paymentLaneMetaABI.Unpack(getPaymentLaneRatioMethod, ret)
	if err != nil {
		return 0, fmt.Errorf("%w: %s: %w", ErrCorruptConfig, getPaymentLaneRatioMethod, err)
	}
	if len(values) != 1 {
		return 0, unexpectedOutputCount(getPaymentLaneRatioMethod, len(values), 1)
	}
	return checkRatio(*abi.ConvertType(values[0], new(*big.Int)).(**big.Int))
}

func unpackGetPaymentContracts(ret []byte) ([]common.Address, uint64, error) {
	values, err := paymentLaneMetaABI.Unpack(getPaymentContractsMethod, ret)
	if err != nil {
		return nil, 0, fmt.Errorf("%w: %s: %w", ErrCorruptConfig, getPaymentContractsMethod, err)
	}
	if len(values) != 2 {
		return nil, 0, unexpectedOutputCount(getPaymentContractsMethod, len(values), 2)
	}
	paymentContracts := *abi.ConvertType(values[0], new([]common.Address)).(*[]common.Address)
	totalLength := *abi.ConvertType(values[1], new(*big.Int)).(**big.Int)
	if totalLength == nil || !totalLength.IsUint64() {
		return nil, 0, fmt.Errorf("%w: getPaymentContracts.totalLength does not fit uint64: %v", ErrCorruptConfig, totalLength)
	}
	return paymentContracts, totalLength.Uint64(), nil
}

func unexpectedOutputCount(method string, got, want int) error {
	return fmt.Errorf("%w: %s returned %d values, want %d", ErrCorruptConfig, method, got, want)
}
