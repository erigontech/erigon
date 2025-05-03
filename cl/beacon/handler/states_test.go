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

package handler

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
)

func TestGetStateFork(t *testing.T) {

	// setupTestingHandler(t, clparams.Phase0Version)
	_, blocks, _, _, postState, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), false)

	postRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)

	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot

	cases := []struct {
		blockID string
		code    int
	}{
		{
			blockID: "0x" + common.Bytes2Hex(postRoot[:]),
			code:    http.StatusOK,
		},
		{
			blockID: "head",
			code:    http.StatusOK,
		},
		{
			blockID: "0x" + common.Bytes2Hex(make([]byte, 32)),
			code:    http.StatusNotFound,
		},
		{
			blockID: strconv.FormatInt(int64(postState.Slot()), 10),
			code:    http.StatusOK,
		},
	}

	for _, c := range cases {
		t.Run(c.blockID, func(t *testing.T) {
			server := httptest.NewServer(handler.mux)
			defer server.Close()
			// Query the block in the handler with /eth/v2/beacon/blocks/{block_id}
			resp, err := http.Get(server.URL + "/eth/v1/beacon/states/" + c.blockID + "/fork")
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, c.code, resp.StatusCode)
			if resp.StatusCode != http.StatusOK {
				return
			}
			jsonVal := make(map[string]interface{})
			// unmarshal the json
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&jsonVal))
			data := jsonVal["data"].(map[string]interface{})
			require.Equal(t, "0x00000000", data["current_version"])
			require.Equal(t, "0x00000000", data["previous_version"])
			require.Equal(t, "0", data["epoch"])
		})
	}
}

func stringRPCErr(r io.Reader) string {
	b, _ := io.ReadAll(r)
	return string(b)
}

func TestGetStateRoot(t *testing.T) {

	// setupTestingHandler(t, clparams.Phase0Version)
	_, blocks, _, _, postState, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), false)

	postRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)

	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot

	fcu.FinalizedCheckpointVal = solid.Checkpoint{Epoch: fcu.HeadSlotVal / 32, Root: fcu.HeadVal}

	cases := []struct {
		blockID string
		code    int
	}{
		{
			blockID: "0x" + common.Bytes2Hex(postRoot[:]),
			code:    http.StatusOK,
		},
		{
			blockID: "finalized",
			code:    http.StatusOK,
		},
		{
			blockID: "0x" + common.Bytes2Hex(make([]byte, 32)),
			code:    http.StatusNotFound,
		},
		{
			blockID: strconv.FormatInt(int64(postState.Slot()), 10),
			code:    http.StatusOK,
		},
	}

	for _, c := range cases {
		t.Run(c.blockID, func(t *testing.T) {
			server := httptest.NewServer(handler.mux)
			defer server.Close()
			// Query the block in the handler with /eth/v2/beacon/blocks/{block_id}
			resp, err := http.Get(server.URL + "/eth/v1/beacon/states/" + c.blockID + "/root")
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, c.code, resp.StatusCode)
			if resp.StatusCode != http.StatusOK {
				return
			}
			jsonVal := make(map[string]interface{})
			// unmarshal the json
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&jsonVal))
			data := jsonVal["data"].(map[string]interface{})
			require.Equal(t, data["root"], "0x"+common.Bytes2Hex(postRoot[:]))
		})
	}
}

func TestGetStateFullHistorical(t *testing.T) {

	// setupTestingHandler(t, clparams.Phase0Version)
	_, blocks, _, _, postState, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), true)

	fmt.Println("AX")
	postRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)

	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot

	fcu.FinalizedCheckpointVal = solid.Checkpoint{Epoch: fcu.HeadSlotVal / 32, Root: fcu.HeadVal}

	cases := []struct {
		blockID string
		code    int
	}{
		{
			blockID: "0x" + common.Bytes2Hex(postRoot[:]),
			code:    http.StatusOK,
		},
		{
			blockID: "finalized",
			code:    http.StatusOK,
		},
		{
			blockID: "0x" + common.Bytes2Hex(make([]byte, 32)),
			code:    http.StatusNotFound,
		},
		{
			blockID: strconv.FormatInt(int64(postState.Slot()), 10),
			code:    http.StatusOK,
		},
	}

	for _, c := range cases {
		t.Run(c.blockID, func(t *testing.T) {
			server := httptest.NewServer(handler.mux)
			defer server.Close()
			// Query the block in the handler with /eth/v2/beacon/states/{block_id} with content-type octet-stream
			req, err := http.NewRequest("GET", server.URL+"/eth/v2/debug/beacon/states/"+c.blockID, nil)
			require.NoError(t, err)
			req.Header.Set("Accept", "application/octet-stream")

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()
			require.Equal(t, c.code, resp.StatusCode)
			if resp.StatusCode != http.StatusOK {
				return
			}
			// read the all of the octect
			out, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			other := state.New(&clparams.MainnetBeaconConfig)
			require.NoError(t, other.DecodeSSZ(out, int(clparams.Phase0Version)))
			for i := 0; i < other.ValidatorLength(); i++ {
				if other.ValidatorSet().Get(i).PublicKey() != postState.ValidatorSet().Get(i).PublicKey() {
					fmt.Println("difference in validator", i, other.ValidatorSet().Get(i).PublicKey(), postState.ValidatorSet().Get(i).PublicKey())
				}
				if other.ValidatorSet().Get(i).WithdrawalCredentials() != postState.ValidatorSet().Get(i).WithdrawalCredentials() {
					fmt.Println("difference in withdrawal", i, other.ValidatorSet().Get(i).WithdrawalCredentials(), postState.ValidatorSet().Get(i).WithdrawalCredentials())
				}
				if other.ValidatorSet().Get(i).EffectiveBalance() != postState.ValidatorSet().Get(i).EffectiveBalance() {
					fmt.Println("difference in effective", i, other.ValidatorSet().Get(i).EffectiveBalance(), postState.ValidatorSet().Get(i).EffectiveBalance())
				}
				if other.ValidatorSet().Get(i).Slashed() != postState.ValidatorSet().Get(i).Slashed() {
					fmt.Println("difference in slashed", i, other.ValidatorSet().Get(i).Slashed(), postState.ValidatorSet().Get(i).Slashed())
				}
				if other.ValidatorSet().Get(i).ActivationEligibilityEpoch() != postState.ValidatorSet().Get(i).ActivationEligibilityEpoch() {
					fmt.Println("difference in activation", i, other.ValidatorSet().Get(i).ActivationEligibilityEpoch(), postState.ValidatorSet().Get(i).ActivationEligibilityEpoch())
				}
				if other.ValidatorSet().Get(i).ActivationEpoch() != postState.ValidatorSet().Get(i).ActivationEpoch() {
					fmt.Println("difference in activation", i, other.ValidatorSet().Get(i).ActivationEpoch(), postState.ValidatorSet().Get(i).ActivationEpoch())
				}
				if other.ValidatorSet().Get(i).ExitEpoch() != postState.ValidatorSet().Get(i).ExitEpoch() {
					fmt.Println("difference in exit", i, other.ValidatorSet().Get(i).ExitEpoch(), postState.ValidatorSet().Get(i).ExitEpoch())
				}
				if other.ValidatorSet().Get(i).WithdrawableEpoch() != postState.ValidatorSet().Get(i).WithdrawableEpoch() {
					fmt.Println("difference in withdrawable", i, other.ValidatorSet().Get(i).WithdrawableEpoch(), postState.ValidatorSet().Get(i).WithdrawableEpoch())
				}
			}
			otherRoot, err := other.HashSSZ()
			require.NoError(t, err)
			require.Equal(t, postRoot, otherRoot)
		})
	}
}

func TestGetStateFullForkchoice(t *testing.T) {

	// setupTestingHandler(t, clparams.Phase0Version)
	_, blocks, _, _, postState, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), false)

	postRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)

	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot

	fcu.FinalizedCheckpointVal = solid.Checkpoint{Epoch: fcu.HeadSlotVal / 32, Root: fcu.HeadVal}

	fcu.StateAtBlockRootVal[fcu.HeadVal] = postState

	cases := []struct {
		blockID string
		code    int
	}{
		{
			blockID: "0x" + common.Bytes2Hex(postRoot[:]),
			code:    http.StatusOK,
		},
		{
			blockID: "finalized",
			code:    http.StatusOK,
		},
		{
			blockID: "0x" + common.Bytes2Hex(make([]byte, 32)),
			code:    http.StatusNotFound,
		},
		{
			blockID: strconv.FormatInt(int64(postState.Slot()), 10),
			code:    http.StatusOK,
		},
	}

	for _, c := range cases {
		t.Run(c.blockID, func(t *testing.T) {
			server := httptest.NewServer(handler.mux)
			defer server.Close()
			// Query the block in the handler with /eth/v2/beacon/states/{block_id} with content-type octet-stream
			req, err := http.NewRequest("GET", server.URL+"/eth/v2/debug/beacon/states/"+c.blockID, nil)
			require.NoError(t, err)
			req.Header.Set("Accept", "application/octet-stream")

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()
			require.Equal(t, c.code, resp.StatusCode)
			if resp.StatusCode != http.StatusOK {
				return
			}
			// read the all of the octect
			out, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			other := state.New(&clparams.MainnetBeaconConfig)
			require.NoError(t, other.DecodeSSZ(out, int(clparams.Phase0Version)))

			otherRoot, err := other.HashSSZ()
			require.NoError(t, err)
			require.Equal(t, postRoot, otherRoot)
		})
	}
}

func TestGetStateSyncCommittees(t *testing.T) {

	// setupTestingHandler(t, clparams.Phase0Version)
	_, blocks, _, _, postState, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)

	postRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)

	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot

	cSyncCommittee := postState.CurrentSyncCommittee().Copy()
	nSyncCommittee := postState.NextSyncCommittee().Copy()

	fcu.GetSyncCommitteesVal[fcu.HeadSlotVal] = [2]*solid.SyncCommittee{
		cSyncCommittee,
		nSyncCommittee,
	}

	fcu.JustifiedCheckpointVal = solid.Checkpoint{Epoch: fcu.HeadSlotVal / 32, Root: fcu.HeadVal}

	cases := []struct {
		blockID string
		code    int
	}{
		{
			blockID: "0x" + common.Bytes2Hex(postRoot[:]),
			code:    http.StatusOK,
		},
		{
			blockID: "justified",
			code:    http.StatusOK,
		},
		{
			blockID: "0x" + common.Bytes2Hex(make([]byte, 32)),
			code:    http.StatusNotFound,
		},
		{
			blockID: strconv.FormatInt(int64(postState.Slot()), 10),
			code:    http.StatusOK,
		},
	}
	expected := `{"data":{"validators":["109","134","145","89","181","81","159","168","34","251","3","205","213","202","99","121","80","149","18","65","201","227","116","69","100","74","160","198","16","131","0","73","210","122","209","217","97","237","136","98","229","248","176","95","150","171","238","191","200","220","33","219","126","9","214","124","56","86","169","208","125","85","25","88","13","190","153","183","96","165","180","90","164","104","240","123","118","196","163","222","231","127","241","77","68","32","62","79","44","58","14","187","151","243","139","142","174","106","228","102","223","31","120","5","43","255","179","66","119","170","60","152","167","194","4","112","156","233","254","203","1","55","53","19","92","21","28","42","141","162","146","57","23","45","158","93","212","38","2","206","246","225","195","189","47","193","224","242","76","138","84","140","111","51","135","113","41","133","207","30","82","175","161","6","249","83","234","155","244","177","108","252","94","143","173","8","154","75","50","49","39","36","182","101","48","12","172","87","250","59","24","157","215","218","72","185","71","7","253","114","230","226","110","46","166","91","130","20","137","117","132","204","221","52","197","188","11","232","67","115","245","26","35","103","186","37","27","235","64","40","70","239","236","211","61","29","216","199","63","54","78","105","184","15","10","147","247","22","144","107","128","17","178","148","129","192","109","134","145","89","181","81","159","168","34","251","3","205","213","202","99","121","80","149","18","65","201","227","116","69","100","74","160","198","16","131","0","73","210","122","209","217","97","237","136","98","229","248","176","95","150","171","238","191","200","220","33","219","126","9","214","124","56","86","169","208","125","85","25","88","13","190","153","183","96","165","180","90","164","104","240","123","118","196","163","222","231","127","241","77","68","32","62","79","44","58","14","187","151","243","139","142","174","106","228","102","223","31","120","5","43","255","179","66","119","170","60","152","167","194","4","112","156","233","254","203","1","55","53","19","92","21","28","42","141","162","146","57","23","45","158","93","212","38","2","206","246","225","195","189","47","193","224","242","76","138","84","140","111","51","135","113","41","133","207","30","82","175","161","6","249","83","234","155","244","177","108","252","94","143","173","8","154","75","50","49","39","36","182","101","48","12","172","87","250","59","24","157","215","218","72","185","71","7","253","114","230","226","110","46","166","91","130","20","137","117","132","204","221","52","197","188","11","232","67","115","245","26","35","103","186","37","27","235","64","40","70","239","236","211","61","29","216","199","63","54","78","105","184","15","10","147","247","22","144","107","128","17","178","148","129","192"],"validator_aggregates":[["109","134","145","89","181","81","159","168","34","251","3","205","213","202","99","121","80","149","18","65","201","227","116","69","100","74","160","198","16","131","0","73","210","122","209","217","97","237","136","98","229","248","176","95","150","171","238","191","200","220","33","219","126","9","214","124","56","86","169","208","125","85","25","88","13","190","153","183","96","165","180","90","164","104","240","123","118","196","163","222","231","127","241","77","68","32","62","79","44","58","14","187","151","243","139","142","174","106","228","102","223","31","120","5","43","255","179","66","119","170","60","152","167","194","4","112","156","233","254","203","1","55","53","19","92","21","28","42"],["141","162","146","57","23","45","158","93","212","38","2","206","246","225","195","189","47","193","224","242","76","138","84","140","111","51","135","113","41","133","207","30","82","175","161","6","249","83","234","155","244","177","108","252","94","143","173","8","154","75","50","49","39","36","182","101","48","12","172","87","250","59","24","157","215","218","72","185","71","7","253","114","230","226","110","46","166","91","130","20","137","117","132","204","221","52","197","188","11","232","67","115","245","26","35","103","186","37","27","235","64","40","70","239","236","211","61","29","216","199","63","54","78","105","184","15","10","147","247","22","144","107","128","17","178","148","129","192"],["109","134","145","89","181","81","159","168","34","251","3","205","213","202","99","121","80","149","18","65","201","227","116","69","100","74","160","198","16","131","0","73","210","122","209","217","97","237","136","98","229","248","176","95","150","171","238","191","200","220","33","219","126","9","214","124","56","86","169","208","125","85","25","88","13","190","153","183","96","165","180","90","164","104","240","123","118","196","163","222","231","127","241","77","68","32","62","79","44","58","14","187","151","243","139","142","174","106","228","102","223","31","120","5","43","255","179","66","119","170","60","152","167","194","4","112","156","233","254","203","1","55","53","19","92","21","28","42"],["141","162","146","57","23","45","158","93","212","38","2","206","246","225","195","189","47","193","224","242","76","138","84","140","111","51","135","113","41","133","207","30","82","175","161","6","249","83","234","155","244","177","108","252","94","143","173","8","154","75","50","49","39","36","182","101","48","12","172","87","250","59","24","157","215","218","72","185","71","7","253","114","230","226","110","46","166","91","130","20","137","117","132","204","221","52","197","188","11","232","67","115","245","26","35","103","186","37","27","235","64","40","70","239","236","211","61","29","216","199","63","54","78","105","184","15","10","147","247","22","144","107","128","17","178","148","129","192"]]},"execution_optimistic":false,"finalized":false}` + "\n"
	for _, c := range cases {
		t.Run(c.blockID, func(t *testing.T) {
			server := httptest.NewServer(handler.mux)
			defer server.Close()
			resp, err := http.Get(server.URL + "/eth/v1/beacon/states/" + c.blockID + "/sync_committees")
			require.NoError(t, err)

			defer resp.Body.Close()
			require.Equal(t, c.code, resp.StatusCode)
			if resp.StatusCode != http.StatusOK {
				return
			}
			// read the all of the octect
			out, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, expected, string(out))
		})
	}
}

func TestGetStateSyncCommitteesHistorical(t *testing.T) {

	// setupTestingHandler(t, clparams.Phase0Version)
	_, blocks, _, _, postState, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)

	postRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)

	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot

	//fcu.JustifiedCheckpointVal = solid.NewCheckpointFromParameters(fcu.HeadVal, fcu.HeadSlotVal/32)
	fcu.JustifiedCheckpointVal = solid.Checkpoint{Epoch: fcu.HeadSlotVal / 32, Root: fcu.HeadVal}

	cases := []struct {
		blockID string
		code    int
	}{
		{
			blockID: "0x" + common.Bytes2Hex(postRoot[:]),
			code:    http.StatusOK,
		},
		{
			blockID: "justified",
			code:    http.StatusOK,
		},
		{
			blockID: "0x" + common.Bytes2Hex(make([]byte, 32)),
			code:    http.StatusNotFound,
		},
		{
			blockID: strconv.FormatInt(int64(postState.Slot()), 10),
			code:    http.StatusOK,
		},
	}
	expected := `{"data":{"validators":["109","134","145","89","181","81","159","168","34","251","3","205","213","202","99","121","80","149","18","65","201","227","116","69","100","74","160","198","16","131","0","73","210","122","209","217","97","237","136","98","229","248","176","95","150","171","238","191","200","220","33","219","126","9","214","124","56","86","169","208","125","85","25","88","13","190","153","183","96","165","180","90","164","104","240","123","118","196","163","222","231","127","241","77","68","32","62","79","44","58","14","187","151","243","139","142","174","106","228","102","223","31","120","5","43","255","179","66","119","170","60","152","167","194","4","112","156","233","254","203","1","55","53","19","92","21","28","42","141","162","146","57","23","45","158","93","212","38","2","206","246","225","195","189","47","193","224","242","76","138","84","140","111","51","135","113","41","133","207","30","82","175","161","6","249","83","234","155","244","177","108","252","94","143","173","8","154","75","50","49","39","36","182","101","48","12","172","87","250","59","24","157","215","218","72","185","71","7","253","114","230","226","110","46","166","91","130","20","137","117","132","204","221","52","197","188","11","232","67","115","245","26","35","103","186","37","27","235","64","40","70","239","236","211","61","29","216","199","63","54","78","105","184","15","10","147","247","22","144","107","128","17","178","148","129","192","109","134","145","89","181","81","159","168","34","251","3","205","213","202","99","121","80","149","18","65","201","227","116","69","100","74","160","198","16","131","0","73","210","122","209","217","97","237","136","98","229","248","176","95","150","171","238","191","200","220","33","219","126","9","214","124","56","86","169","208","125","85","25","88","13","190","153","183","96","165","180","90","164","104","240","123","118","196","163","222","231","127","241","77","68","32","62","79","44","58","14","187","151","243","139","142","174","106","228","102","223","31","120","5","43","255","179","66","119","170","60","152","167","194","4","112","156","233","254","203","1","55","53","19","92","21","28","42","141","162","146","57","23","45","158","93","212","38","2","206","246","225","195","189","47","193","224","242","76","138","84","140","111","51","135","113","41","133","207","30","82","175","161","6","249","83","234","155","244","177","108","252","94","143","173","8","154","75","50","49","39","36","182","101","48","12","172","87","250","59","24","157","215","218","72","185","71","7","253","114","230","226","110","46","166","91","130","20","137","117","132","204","221","52","197","188","11","232","67","115","245","26","35","103","186","37","27","235","64","40","70","239","236","211","61","29","216","199","63","54","78","105","184","15","10","147","247","22","144","107","128","17","178","148","129","192"],"validator_aggregates":[["109","134","145","89","181","81","159","168","34","251","3","205","213","202","99","121","80","149","18","65","201","227","116","69","100","74","160","198","16","131","0","73","210","122","209","217","97","237","136","98","229","248","176","95","150","171","238","191","200","220","33","219","126","9","214","124","56","86","169","208","125","85","25","88","13","190","153","183","96","165","180","90","164","104","240","123","118","196","163","222","231","127","241","77","68","32","62","79","44","58","14","187","151","243","139","142","174","106","228","102","223","31","120","5","43","255","179","66","119","170","60","152","167","194","4","112","156","233","254","203","1","55","53","19","92","21","28","42"],["141","162","146","57","23","45","158","93","212","38","2","206","246","225","195","189","47","193","224","242","76","138","84","140","111","51","135","113","41","133","207","30","82","175","161","6","249","83","234","155","244","177","108","252","94","143","173","8","154","75","50","49","39","36","182","101","48","12","172","87","250","59","24","157","215","218","72","185","71","7","253","114","230","226","110","46","166","91","130","20","137","117","132","204","221","52","197","188","11","232","67","115","245","26","35","103","186","37","27","235","64","40","70","239","236","211","61","29","216","199","63","54","78","105","184","15","10","147","247","22","144","107","128","17","178","148","129","192"],["109","134","145","89","181","81","159","168","34","251","3","205","213","202","99","121","80","149","18","65","201","227","116","69","100","74","160","198","16","131","0","73","210","122","209","217","97","237","136","98","229","248","176","95","150","171","238","191","200","220","33","219","126","9","214","124","56","86","169","208","125","85","25","88","13","190","153","183","96","165","180","90","164","104","240","123","118","196","163","222","231","127","241","77","68","32","62","79","44","58","14","187","151","243","139","142","174","106","228","102","223","31","120","5","43","255","179","66","119","170","60","152","167","194","4","112","156","233","254","203","1","55","53","19","92","21","28","42"],["141","162","146","57","23","45","158","93","212","38","2","206","246","225","195","189","47","193","224","242","76","138","84","140","111","51","135","113","41","133","207","30","82","175","161","6","249","83","234","155","244","177","108","252","94","143","173","8","154","75","50","49","39","36","182","101","48","12","172","87","250","59","24","157","215","218","72","185","71","7","253","114","230","226","110","46","166","91","130","20","137","117","132","204","221","52","197","188","11","232","67","115","245","26","35","103","186","37","27","235","64","40","70","239","236","211","61","29","216","199","63","54","78","105","184","15","10","147","247","22","144","107","128","17","178","148","129","192"]]},"execution_optimistic":false,"finalized":false}` + "\n"
	for _, c := range cases {
		t.Run(c.blockID, func(t *testing.T) {
			server := httptest.NewServer(handler.mux)
			defer server.Close()
			resp, err := http.Get(server.URL + "/eth/v1/beacon/states/" + c.blockID + "/sync_committees")
			require.NoError(t, err)

			defer resp.Body.Close()
			require.Equal(t, c.code, resp.StatusCode)
			if resp.StatusCode != http.StatusOK {
				return
			}
			// read the all of the octect
			out, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, expected, string(out))
		})
	}
}

func TestGetStateFinalityCheckpoints(t *testing.T) {

	// setupTestingHandler(t, clparams.Phase0Version)
	_, blocks, _, _, postState, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), false)

	postRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)

	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot

	//fcu.JustifiedCheckpointVal = solid.NewCheckpointFromParameters(fcu.HeadVal, fcu.HeadSlotVal/32)
	fcu.JustifiedCheckpointVal = solid.Checkpoint{Epoch: fcu.HeadSlotVal / 32, Root: fcu.HeadVal}

	cases := []struct {
		blockID string
		code    int
	}{
		{
			blockID: "0x" + common.Bytes2Hex(postRoot[:]),
			code:    http.StatusOK,
		},
		{
			blockID: "justified",
			code:    http.StatusOK,
		},
		{
			blockID: "0x" + common.Bytes2Hex(make([]byte, 32)),
			code:    http.StatusNotFound,
		},
		{
			blockID: strconv.FormatInt(int64(postState.Slot()), 10),
			code:    http.StatusOK,
		},
	}
	expected := `{"data":{"finalized":{"epoch":"1","root":"0xde46b0f2ed5e72f0cec20246403b14c963ec995d7c2825f3532b0460c09d5693"},"current_justified":{"epoch":"3","root":"0xa6e47f164b1a3ca30ea3b2144bd14711de442f51e5b634750a12a1734e24c987"},"previous_justified":{"epoch":"2","root":"0x4c3ee7969e485696669498a88c17f70e6999c40603e2f4338869004392069063"}},"execution_optimistic":false,"finalized":false}` + "\n"
	for _, c := range cases {
		t.Run(c.blockID, func(t *testing.T) {
			server := httptest.NewServer(handler.mux)
			defer server.Close()
			resp, err := http.Get(server.URL + "/eth/v1/beacon/states/" + c.blockID + "/finality_checkpoints")
			require.NoError(t, err)

			defer resp.Body.Close()
			require.Equal(t, c.code, resp.StatusCode)
			if resp.StatusCode != http.StatusOK {
				return
			}
			// read the all of the octect
			out, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, expected, string(out))
		})
	}
}

func TestGetRandao(t *testing.T) {

	// setupTestingHandler(t, clparams.Phase0Version)
	_, blocks, _, _, postState, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), false)

	postRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)

	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot

	//fcu.JustifiedCheckpointVal = solid.NewCheckpointFromParameters(fcu.HeadVal, fcu.HeadSlotVal/32)
	fcu.JustifiedCheckpointVal = solid.Checkpoint{Epoch: fcu.HeadSlotVal / 32, Root: fcu.HeadVal}
	cases := []struct {
		blockID string
		code    int
	}{
		{
			blockID: "0x" + common.Bytes2Hex(postRoot[:]),
			code:    http.StatusOK,
		},
		{
			blockID: "justified",
			code:    http.StatusOK,
		},
		{
			blockID: "0x" + common.Bytes2Hex(make([]byte, 32)),
			code:    http.StatusNotFound,
		},
		{
			blockID: strconv.FormatInt(int64(postState.Slot()), 10),
			code:    http.StatusOK,
		},
	}
	expected := `{"data":{"randao":"0xdeec617717272914bfd73e02ca1da113a83cf4cf33cd4939486509e2da4ccf4e"},"execution_optimistic":false,"finalized":false}` + "\n"
	for _, c := range cases {
		t.Run(c.blockID, func(t *testing.T) {
			server := httptest.NewServer(handler.mux)
			defer server.Close()
			resp, err := http.Get(server.URL + "/eth/v1/beacon/states/" + c.blockID + "/randao")
			require.NoError(t, err)

			defer resp.Body.Close()
			require.Equal(t, c.code, resp.StatusCode)
			if resp.StatusCode != http.StatusOK {
				return
			}
			// read the all of the octect
			out, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, expected, string(out))
		})
	}
}
