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
	"encoding/json"
	"testing"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/polygon/heimdall/heimdalltest"
	"github.com/stretchr/testify/require"
)

func TestSpanListResponse(t *testing.T) {
	output := []byte(`{"span_list":[{"id":"0","start_block":"0","end_block":"255","validator_set":{"validators":[{"val_id":"1","start_epoch":"0","end_epoch":"0","nonce":"1","voting_power":"10000","pub_key":"BD7rDNYnoRW2fonLir583UAzk5oRkJoUSC0J1PK+t0Y3kw6szj775oKPnO7gxzCIlLYWlidt4ixIILu8RebudD8=","signer":"20D8433DC819AF087336A725422DF4CFBEF29710","last_updated":"","jailed":false,"proposer_priority":"-10000"},{"val_id":"2","start_epoch":"0","end_epoch":"0","nonce":"1","voting_power":"10000","pub_key":"BP5UKZ9Pgw+j294NZvMSefMAtoyOhxPqNFRJKL1FxKvAHqbh/vxmo9xtLmsWDLtwCJ8rhzl9IdY3V9qVEzPw6sA=","signer":"735DE19A997EF33A090C873D1AC27F99D77B843C","last_updated":"","jailed":false,"proposer_priority":"10000"}],"proposer":{"val_id":"1","start_epoch":"0","end_epoch":"0","nonce":"1","voting_power":"10000","pub_key":"BD7rDNYnoRW2fonLir583UAzk5oRkJoUSC0J1PK+t0Y3kw6szj775oKPnO7gxzCIlLYWlidt4ixIILu8RebudD8=","signer":"20D8433DC819AF087336A725422DF4CFBEF29710","last_updated":"","jailed":false,"proposer_priority":"-10000"},"total_voting_power":"20000"},"selected_producers":[{"val_id":"1","start_epoch":"0","end_epoch":"0","nonce":"1","voting_power":"10000","pub_key":"BD7rDNYnoRW2fonLir583UAzk5oRkJoUSC0J1PK+t0Y3kw6szj775oKPnO7gxzCIlLYWlidt4ixIILu8RebudD8=","signer":"20D8433DC819AF087336A725422DF4CFBEF29710","last_updated":"","jailed":false,"proposer_priority":"-10000"},{"val_id":"2","start_epoch":"0","end_epoch":"0","nonce":"1","voting_power":"10000","pub_key":"BP5UKZ9Pgw+j294NZvMSefMAtoyOhxPqNFRJKL1FxKvAHqbh/vxmo9xtLmsWDLtwCJ8rhzl9IdY3V9qVEzPw6sA=","signer":"735DE19A997EF33A090C873D1AC27F99D77B843C","last_updated":"","jailed":false,"proposer_priority":"10000"}],"bor_chain_id":"2756"}]}`)

	var v SpanListResponseV2

	err := json.Unmarshal(output, &v)
	require.Nil(t, err)

	list, err := v.ToList()
	require.Nil(t, err)
	require.Len(t, list, 1)
}

func TestSpanJsonMarshall(t *testing.T) {
	validators := []*Validator{
		NewValidator(common.HexToAddress("deadbeef"), 1),
		NewValidator(common.HexToAddress("cafebabe"), 2),
	}

	validatorSet := ValidatorSet{
		Validators: validators,
		Proposer:   validators[0],
	}

	span := Span{
		Id:                1,
		StartBlock:        100,
		EndBlock:          200,
		ValidatorSet:      validatorSet,
		SelectedProducers: []Validator{*validators[0]},
		ChainID:           "bor",
	}

	heimdalltest.AssertJsonMarshalUnmarshal(t, &span)
}
