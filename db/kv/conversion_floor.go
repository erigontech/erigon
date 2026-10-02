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

package kv

import (
	"errors"
	"fmt"
)

var ErrConversionFloor = errors.New("conversion point floor")

type ConversionFloorKind string

const (
	ConversionFloorBlock ConversionFloorKind = "block"
	ConversionFloorTx    ConversionFloorKind = "txNum"
)

type ConversionFloorError struct {
	ConversionBlockNum uint64
	ConversionTxNum    uint64
	Requested          uint64
	Kind               ConversionFloorKind
}

func NewConversionFloorError(blockNum, txNum, requested uint64, kind ConversionFloorKind) error {
	return &ConversionFloorError{
		ConversionBlockNum: blockNum,
		ConversionTxNum:    txNum,
		Requested:          requested,
		Kind:               kind,
	}
}

func (e *ConversionFloorError) Error() string {
	return fmt.Sprintf("requested %s %d reaches conversion point block %d txNum %d", e.Kind, e.Requested, e.ConversionBlockNum, e.ConversionTxNum)
}

func (e *ConversionFloorError) Unwrap() error { return ErrConversionFloor }
