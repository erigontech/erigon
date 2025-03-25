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

package consensus_tests

import (
	"io/fs"
	"testing"

	"github.com/erigontech/erigon/spectest"
)

type BlsAggregateVerify struct {
}

func (b *BlsAggregateVerify) Run(t *testing.T, root fs.FS, c spectest.TestCase) (err error) {
	t.Skipf("Skipping BLS because it is done by library")
	return
	//var meta struct {
	//	Input struct {
	//		Pubkeys   []hexutil.Bytes `yaml:"pubkeys"`
	//		Messages  []common.Hash      `yaml:"messages"`
	//		Signature hexutil.Bytes   `yaml:"signature"`
	//	} `yaml:"input"`
	//	Output bool `yaml:"output"`
	//}
	//if err := spectest.ReadMeta(root, "data.yaml", &meta); err != nil {
	//	return err
	//}
	//return spectest.ErrorHandlerNotImplemented
}
