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

package beaconevents

import (
	"encoding/json"
	"maps"
	"slices"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/execution/types"
	"github.com/stretchr/testify/require"
)

func TestPayloadAttributesDataGloasJSON(t *testing.T) {
	parentRoot := common.HexToHash("0xcf8e0d4e9587369b2301d0790347320302cc0943d5a1884560367e8208d920f2")
	parentHash := common.HexToHash("0x9a2fefd2fdb57f74993c7780ea5b9030d2897b615b89f808011ca5aebed54eaf")
	slotNumber := hexutil.Uint64(10)
	targetGasLimit := hexutil.Uint64(60_000_000)
	data := PayloadAttributesData{
		Version: "gloas",
		Data: PayloadAttributesContent{
			ProposerIndex:      123,
			ProposalSlot:       10,
			ParentBlockRoot:    parentRoot,
			ParentBlockHash:    parentHash,
			SafeBlockHash:      &parentHash,
			FinalizedBlockHash: &parentHash,
			PayloadAttributes: NewPayloadAttributes(clparams.GloasVersion, &engine_types.PayloadAttributes{
				Timestamp:             hexutil.Uint64(123456),
				PrevRandao:            parentRoot,
				SuggestedFeeRecipient: common.Address{},
				Withdrawals: []*types.Withdrawal{{
					Index:     hexutil.Uint64(5),
					Validator: hexutil.Uint64(10),
					Address:   common.Address{},
					Amount:    hexutil.Uint64(15640),
				}},
				ParentBeaconBlockRoot: &parentRoot,
				SlotNumber:            &slotNumber,
				TargetGasLimit:        &targetGasLimit,
			}),
		},
	}

	encoded, err := json.Marshal(data)
	require.NoError(t, err)
	require.JSONEq(t, `{"version":"gloas","data":{"proposer_index":"123","proposal_slot":"10","parent_block_root":"0xcf8e0d4e9587369b2301d0790347320302cc0943d5a1884560367e8208d920f2","parent_block_hash":"0x9a2fefd2fdb57f74993c7780ea5b9030d2897b615b89f808011ca5aebed54eaf","safe_block_hash":"0x9a2fefd2fdb57f74993c7780ea5b9030d2897b615b89f808011ca5aebed54eaf","finalized_block_hash":"0x9a2fefd2fdb57f74993c7780ea5b9030d2897b615b89f808011ca5aebed54eaf","payload_attributes":{"timestamp":"123456","prev_randao":"0xcf8e0d4e9587369b2301d0790347320302cc0943d5a1884560367e8208d920f2","suggested_fee_recipient":"0x0000000000000000000000000000000000000000","withdrawals":[{"index":"5","validator_index":"10","address":"0x0000000000000000000000000000000000000000","amount":"15640"}],"parent_beacon_block_root":"0xcf8e0d4e9587369b2301d0790347320302cc0943d5a1884560367e8208d920f2","slot_number":"10","target_gas_limit":"60000000"}}}`, string(encoded))
}

func TestPayloadAttributesDataFuluJSONShape(t *testing.T) {
	parentRoot := common.Hash{0x11}
	parentHash := common.Hash{0x22}
	parentNumber := uint64(42)
	data := PayloadAttributesData{
		Version: "fulu",
		Data: PayloadAttributesContent{
			ProposerIndex:     3,
			ProposalSlot:      4,
			ParentBlockNumber: &parentNumber,
			ParentBlockRoot:   parentRoot,
			ParentBlockHash:   parentHash,
			PayloadAttributes: NewPayloadAttributes(clparams.FuluVersion, &engine_types.PayloadAttributes{
				Timestamp:             hexutil.Uint64(12),
				PrevRandao:            common.Hash{0x33},
				SuggestedFeeRecipient: common.Address{0x44},
				Withdrawals:           []*types.Withdrawal{},
				ParentBeaconBlockRoot: &parentRoot,
			}),
		},
	}

	encoded, err := json.Marshal(data)
	require.NoError(t, err)
	var fields map[string]any
	require.NoError(t, json.Unmarshal(encoded, &fields))
	content := fields["data"].(map[string]any)
	require.Equal(t, "42", content["parent_block_number"])
	require.NotContains(t, content, "safe_block_hash")
	require.NotContains(t, content, "finalized_block_hash")
	attributes := content["payload_attributes"].(map[string]any)
	require.Equal(t, []any{}, attributes["withdrawals"])
}

func TestPayloadAttributesFieldsFollowVersion(t *testing.T) {
	root := common.Hash{0x11}
	slotNumber := hexutil.Uint64(10)
	targetGasLimit := hexutil.Uint64(60_000_000)
	value := &engine_types.PayloadAttributes{
		Withdrawals:           []*types.Withdrawal{},
		ParentBeaconBlockRoot: &root,
		SlotNumber:            &slotNumber,
		TargetGasLimit:        &targetGasLimit,
	}
	base := []string{"timestamp", "prev_randao", "suggested_fee_recipient"}
	for _, test := range []struct {
		version clparams.StateVersion
		fields  []string
	}{
		{clparams.BellatrixVersion, base},
		{clparams.CapellaVersion, append(base, "withdrawals")},
		{clparams.DenebVersion, append(base, "withdrawals", "parent_beacon_block_root")},
		{clparams.FuluVersion, append(base, "withdrawals", "parent_beacon_block_root")},
		{clparams.GloasVersion, append(base, "withdrawals", "parent_beacon_block_root", "slot_number", "target_gas_limit")},
	} {
		t.Run(test.version.String(), func(t *testing.T) {
			encoded, err := json.Marshal(NewPayloadAttributes(test.version, value))
			require.NoError(t, err)
			var fields map[string]any
			require.NoError(t, json.Unmarshal(encoded, &fields))
			require.ElementsMatch(t, test.fields, slices.Collect(maps.Keys(fields)))
		})
	}
}
