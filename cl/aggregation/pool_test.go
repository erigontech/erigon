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

package aggregation

import (
	"context"
	"log"
	"testing"

	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/stretchr/testify/suite"
)

var (
	attData1 = &solid.AttestationData{
		Source: &solid.Checkpoint{
			Epoch: 1,
			Root:  [32]byte{0, 4, 2, 6},
		},
		Target: &solid.Checkpoint{
			Epoch: 1,
			Root:  [32]byte{0, 4, 2, 6},
		},
	}
	att1_1 = &solid.Attestation{
		AggregationBits: solid.BitlistFromBytes([]byte{0b00000001, 0, 0, 0}, 2048),
		Data:            attData1,
		Signature:       [96]byte{'a', 'b', 'c', 'd', 'e', 'f'},
	}
	att1_2 = &solid.Attestation{
		AggregationBits: solid.BitlistFromBytes([]byte{0b00000001, 0, 0, 0}, 2048),
		Data:            attData1,
		Signature:       [96]byte{'d', 'e', 'f', 'g', 'h', 'i'},
	}
	att1_3 = &solid.Attestation{
		AggregationBits: solid.BitlistFromBytes([]byte{0b00000100, 0, 0, 0}, 2048),
		Data:            attData1,
		Signature:       [96]byte{'g', 'h', 'i', 'j', 'k', 'l'},
	}
	att1_4 = &solid.Attestation{
		AggregationBits: solid.BitlistFromBytes([]byte{0b00100000, 0, 0, 0}, 2048),
		Data:            attData1,
		Signature:       [96]byte{'m', 'n', 'o', 'p', 'q', 'r'},
	}
	attData1Root, _ = attData1.HashSSZ()

	attData2 = &solid.AttestationData{
		Source: &solid.Checkpoint{
			Epoch: 3,
			Root:  [32]byte{5, 5, 6, 6},
		},
		Target: &solid.Checkpoint{
			Epoch: 1,
			Root:  [32]byte{0, 4, 2, 6},
		},
	}
	att2_1 = &solid.Attestation{
		AggregationBits: solid.BitlistFromBytes([]byte{0b00000001, 0, 0, 0}, 2048),
		Data:            attData2,
		Signature:       [96]byte{'t', 'e', 's', 't', 'i', 'n'},
	}

	mockAggrResult = [96]byte{'m', 'o', 'c', 'k'}
)

type PoolTestSuite struct {
	suite.Suite
}

func (t *PoolTestSuite) SetupTest() {
	blsAggregate = func(sigs [][]byte) ([]byte, error) {
		ret := make([]byte, 96)
		copy(ret, mockAggrResult[:])
		return ret, nil
	}
}

func (t *PoolTestSuite) TearDownTest() {
}

func (t *PoolTestSuite) TestAddAttestation() {
	testcases := []struct {
		name     string
		atts     []*solid.Attestation
		hashRoot [32]byte
		expect   *solid.Attestation
	}{
		{
			name: "simple, different hashRoot",
			atts: []*solid.Attestation{
				att1_1,
				att2_1,
			},
			hashRoot: attData1Root,
			expect:   att1_1,
		},
		{
			name: "att1_2 is a super set of att1_1. skip att1_1",
			atts: []*solid.Attestation{
				att1_2,
				att1_1,
				att2_1, // none of its business
			},
			hashRoot: attData1Root,
			expect:   att1_2,
		},
		{
			name: "merge att1_2, att1_3, att1_4",
			atts: []*solid.Attestation{
				att1_2,
				att1_3,
				att1_4,
			},
			hashRoot: attData1Root,
			expect: &solid.Attestation{
				AggregationBits: solid.BitlistFromBytes([]byte{0b00100101, 0, 0, 0}, 2048),
				Data:            attData1,
				Signature:       mockAggrResult,
			},
		},
	}

	for _, tc := range testcases {
		log.Printf("test case: %s", tc.name)
		pool := NewAggregationPool(context.Background(), nil, nil, nil)
		for _, att := range tc.atts {
			pool.AddAttestation(att)
		}
		att := pool.GetAggregatationByRoot(tc.hashRoot)
		h1, _ := tc.expect.HashSSZ()
		h2, _ := att.HashSSZ()
		t.Equal(h1, h2, tc.name)
	}
}

func TestPool(t *testing.T) {
	suite.Run(t, new(PoolTestSuite))
}
