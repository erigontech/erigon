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

package heimdall

import (
	"testing"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/v3/polygon/bor/valset"
	"github.com/erigontech/erigon/v3/polygon/heimdall/heimdalltest"
)

func TestSpanJsonMarshall(t *testing.T) {
	validators := []*valset.Validator{
		valset.NewValidator(libcommon.HexToAddress("deadbeef"), 1),
		valset.NewValidator(libcommon.HexToAddress("cafebabe"), 2),
	}

	validatorSet := valset.ValidatorSet{
		Validators: validators,
		Proposer:   validators[0],
	}

	span := Span{
		Id:                1,
		StartBlock:        100,
		EndBlock:          200,
		ValidatorSet:      validatorSet,
		SelectedProducers: []valset.Validator{*validators[0]},
		ChainID:           "bor",
	}

	heimdalltest.AssertJsonMarshalUnmarshal(t, &span)
}
