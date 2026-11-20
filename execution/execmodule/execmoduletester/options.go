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

package execmoduletester

import "github.com/erigontech/erigon/common"

type UFCOpt func(o *ufcOpt)

func WithSafeHash(h common.Hash) UFCOpt {
	return func(o *ufcOpt) {
		o.safeHash = h
	}
}

func WithFinalisedHash(h common.Hash) UFCOpt {
	return func(o *ufcOpt) {
		o.finalisedHash = h
	}
}

type ufcOpt struct {
	safeHash      common.Hash
	finalisedHash common.Hash
}

func applyUfcOpts(opts ...UFCOpt) ufcOpt {
	var o ufcOpt
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

type IVUOpt func(o *ivuOpt)

func WithFcuOptSeq(seq [][]UFCOpt) IVUOpt {
	return func(o *ivuOpt) {
		o.fcuOptSeq = seq
	}
}

func WithWaitForBlockRetirement() IVUOpt {
	return func(o *ivuOpt) {
		o.waitForBlockRetirement = true
	}
}

func WithWaitForStateFiles() IVUOpt {
	return func(o *ivuOpt) {
		o.waitForStateFiles = true
	}
}

type ivuOpt struct {
	fcuOptSeq              [][]UFCOpt
	waitForBlockRetirement bool
	waitForStateFiles      bool
}

func applyIVUOpts(opts ...IVUOpt) ivuOpt {
	var o ivuOpt
	for _, opt := range opts {
		opt(&o)
	}
	return o
}
