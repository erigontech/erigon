package handler

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/common"
	"github.com/stretchr/testify/require"
)

func TestGetStateFork(t *testing.T) {

	// setupTestingHandler(t, clparams.Phase0Version)
	_, blocks, _, _, postState, handler, _, _, fcu := setupTestingHandler(t, clparams.Phase0Version)

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
			require.Equal(t, data["current_version"], "0x00000000")
			require.Equal(t, data["previous_version"], "0x00000000")
			require.Equal(t, data["epoch"], "0")
		})
	}
}

func TestGetStateRoot(t *testing.T) {

	// setupTestingHandler(t, clparams.Phase0Version)
	_, blocks, _, _, postState, handler, _, _, fcu := setupTestingHandler(t, clparams.Phase0Version)

	postRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)

	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot

	fcu.FinalizedCheckpointVal = solid.NewCheckpointFromParameters(fcu.HeadVal, fcu.HeadSlotVal/32)

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
	_, blocks, _, _, postState, handler, _, _, fcu := setupTestingHandler(t, clparams.Phase0Version)

	postRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)

	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot

	fcu.FinalizedCheckpointVal = solid.NewCheckpointFromParameters(fcu.HeadVal, fcu.HeadSlotVal/32)

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

func TestGetStateFullForkchoice(t *testing.T) {

	// setupTestingHandler(t, clparams.Phase0Version)
	_, blocks, _, _, postState, handler, _, _, fcu := setupTestingHandler(t, clparams.Phase0Version)

	postRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)

	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot

	fcu.FinalizedCheckpointVal = solid.NewCheckpointFromParameters(fcu.HeadVal, fcu.HeadSlotVal/32)

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
	_, blocks, _, _, postState, handler, _, _, fcu := setupTestingHandler(t, clparams.BellatrixVersion)

	postRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)

	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot

	cSyncCommittee := postState.CurrentSyncCommittee().Copy()
	nSyncCommittee := postState.NextSyncCommittee().Copy()

	fcu.GetSyncCommitteesVal[fcu.HeadVal] = [2]*solid.SyncCommittee{
		cSyncCommittee,
		nSyncCommittee,
	}

	fcu.JustifiedCheckpointVal = solid.NewCheckpointFromParameters(fcu.HeadVal, fcu.HeadSlotVal/32)

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
	expected := `{"data":{"validators":["109","134","145","89","181","81","159","168","34","251","3","205","213","202","99","121","80","149","18","65","201","227","116","69","100","74","160","198","16","131","0","73","210","122","209","217","97","237","136","98","229","248","176","95","150","171","238","191","200","220","33","219","126","9","214","124","56","86","169","208","125","85","25","88","13","190","153","183","96","165","180","90","164","104","240","123","118","196","163","222","231","127","241","77","68","32","62","79","44","58","14","187","151","243","139","142","174","106","228","102","223","31","120","5","43","255","179","66","119","170","60","152","167","194","4","112","156","233","254","203","1","55","53","19","92","21","28","42","141","162","146","57","23","45","158","93","212","38","2","206","246","225","195","189","47","193","224","242","76","138","84","140","111","51","135","113","41","133","207","30","82","175","161","6","249","83","234","155","244","177","108","252","94","143","173","8","154","75","50","49","39","36","182","101","48","12","172","87","250","59","24","157","215","218","72","185","71","7","253","114","230","226","110","46","166","91","130","20","137","117","132","204","221","52","197","188","11","232","67","115","245","26","35","103","186","37","27","235","64","40","70","239","236","211","61","29","216","199","63","54","78","105","184","15","10","147","247","22","144","107","128","17","178","148","129","192","109","134","145","89","181","81","159","168","34","251","3","205","213","202","99","121","80","149","18","65","201","227","116","69","100","74","160","198","16","131","0","73","210","122","209","217","97","237","136","98","229","248","176","95","150","171","238","191","200","220","33","219","126","9","214","124","56","86","169","208","125","85","25","88","13","190","153","183","96","165","180","90","164","104","240","123","118","196","163","222","231","127","241","77","68","32","62","79","44","58","14","187","151","243","139","142","174","106","228","102","223","31","120","5","43","255","179","66","119","170","60","152","167","194","4","112","156","233","254","203","1","55","53","19","92","21","28","42","141","162","146","57","23","45","158","93","212","38","2","206","246","225","195","189","47","193","224","242","76","138","84","140","111","51","135","113","41","133","207","30","82","175","161","6","249","83","234","155","244","177","108","252","94","143","173","8","154","75","50","49","39","36","182","101","48","12","172","87","250","59","24","157","215","218","72","185","71","7","253","114","230","226","110","46","166","91","130","20","137","117","132","204","221","52","197","188","11","232","67","115","245","26","35","103","186","37","27","235","64","40","70","239","236","211","61","29","216","199","63","54","78","105","184","15","10","147","247","22","144","107","128","17","178","148","129","192"],"validator_aggregates":[["109","134","145","89","181","81","159","168","34","251","3","205","213","202","99","121","80","149","18","65","201","227","116","69","100","74","160","198","16","131","0","73","210","122","209","217","97","237","136","98","229","248","176","95","150","171","238","191","200","220","33","219","126","9","214","124","56","86","169","208","125","85","25","88","13","190","153","183","96","165","180","90","164","104","240","123","118","196","163","222","231","127","241","77","68","32","62","79","44","58","14","187","151","243","139","142","174","106","228","102","223","31","120","5","43","255","179","66","119","170","60","152","167","194","4","112","156","233","254","203","1","55","53","19","92","21","28","42"],["141","162","146","57","23","45","158","93","212","38","2","206","246","225","195","189","47","193","224","242","76","138","84","140","111","51","135","113","41","133","207","30","82","175","161","6","249","83","234","155","244","177","108","252","94","143","173","8","154","75","50","49","39","36","182","101","48","12","172","87","250","59","24","157","215","218","72","185","71","7","253","114","230","226","110","46","166","91","130","20","137","117","132","204","221","52","197","188","11","232","67","115","245","26","35","103","186","37","27","235","64","40","70","239","236","211","61","29","216","199","63","54","78","105","184","15","10","147","247","22","144","107","128","17","178","148","129","192"],["109","134","145","89","181","81","159","168","34","251","3","205","213","202","99","121","80","149","18","65","201","227","116","69","100","74","160","198","16","131","0","73","210","122","209","217","97","237","136","98","229","248","176","95","150","171","238","191","200","220","33","219","126","9","214","124","56","86","169","208","125","85","25","88","13","190","153","183","96","165","180","90","164","104","240","123","118","196","163","222","231","127","241","77","68","32","62","79","44","58","14","187","151","243","139","142","174","106","228","102","223","31","120","5","43","255","179","66","119","170","60","152","167","194","4","112","156","233","254","203","1","55","53","19","92","21","28","42"],["141","162","146","57","23","45","158","93","212","38","2","206","246","225","195","189","47","193","224","242","76","138","84","140","111","51","135","113","41","133","207","30","82","175","161","6","249","83","234","155","244","177","108","252","94","143","173","8","154","75","50","49","39","36","182","101","48","12","172","87","250","59","24","157","215","218","72","185","71","7","253","114","230","226","110","46","166","91","130","20","137","117","132","204","221","52","197","188","11","232","67","115","245","26","35","103","186","37","27","235","64","40","70","239","236","211","61","29","216","199","63","54","78","105","184","15","10","147","247","22","144","107","128","17","178","148","129","192"]]},"finalized":false,"execution_optimistic":false}` + "\n"
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
			require.Equal(t, string(out), expected)
		})
	}
}

func TestGetStateSyncCommitteesHistorical(t *testing.T) {

	// setupTestingHandler(t, clparams.Phase0Version)
	_, blocks, _, _, postState, handler, _, _, fcu := setupTestingHandler(t, clparams.BellatrixVersion)

	postRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)

	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot

	fcu.JustifiedCheckpointVal = solid.NewCheckpointFromParameters(fcu.HeadVal, fcu.HeadSlotVal/32)

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
	expected := `{"data":{"validators":["109","134","145","89","181","81","159","168","34","251","3","205","213","202","99","121","80","149","18","65","201","227","116","69","100","74","160","198","16","131","0","73","210","122","209","217","97","237","136","98","229","248","176","95","150","171","238","191","200","220","33","219","126","9","214","124","56","86","169","208","125","85","25","88","13","190","153","183","96","165","180","90","164","104","240","123","118","196","163","222","231","127","241","77","68","32","62","79","44","58","14","187","151","243","139","142","174","106","228","102","223","31","120","5","43","255","179","66","119","170","60","152","167","194","4","112","156","233","254","203","1","55","53","19","92","21","28","42","141","162","146","57","23","45","158","93","212","38","2","206","246","225","195","189","47","193","224","242","76","138","84","140","111","51","135","113","41","133","207","30","82","175","161","6","249","83","234","155","244","177","108","252","94","143","173","8","154","75","50","49","39","36","182","101","48","12","172","87","250","59","24","157","215","218","72","185","71","7","253","114","230","226","110","46","166","91","130","20","137","117","132","204","221","52","197","188","11","232","67","115","245","26","35","103","186","37","27","235","64","40","70","239","236","211","61","29","216","199","63","54","78","105","184","15","10","147","247","22","144","107","128","17","178","148","129","192","109","134","145","89","181","81","159","168","34","251","3","205","213","202","99","121","80","149","18","65","201","227","116","69","100","74","160","198","16","131","0","73","210","122","209","217","97","237","136","98","229","248","176","95","150","171","238","191","200","220","33","219","126","9","214","124","56","86","169","208","125","85","25","88","13","190","153","183","96","165","180","90","164","104","240","123","118","196","163","222","231","127","241","77","68","32","62","79","44","58","14","187","151","243","139","142","174","106","228","102","223","31","120","5","43","255","179","66","119","170","60","152","167","194","4","112","156","233","254","203","1","55","53","19","92","21","28","42","141","162","146","57","23","45","158","93","212","38","2","206","246","225","195","189","47","193","224","242","76","138","84","140","111","51","135","113","41","133","207","30","82","175","161","6","249","83","234","155","244","177","108","252","94","143","173","8","154","75","50","49","39","36","182","101","48","12","172","87","250","59","24","157","215","218","72","185","71","7","253","114","230","226","110","46","166","91","130","20","137","117","132","204","221","52","197","188","11","232","67","115","245","26","35","103","186","37","27","235","64","40","70","239","236","211","61","29","216","199","63","54","78","105","184","15","10","147","247","22","144","107","128","17","178","148","129","192"],"validator_aggregates":[["109","134","145","89","181","81","159","168","34","251","3","205","213","202","99","121","80","149","18","65","201","227","116","69","100","74","160","198","16","131","0","73","210","122","209","217","97","237","136","98","229","248","176","95","150","171","238","191","200","220","33","219","126","9","214","124","56","86","169","208","125","85","25","88","13","190","153","183","96","165","180","90","164","104","240","123","118","196","163","222","231","127","241","77","68","32","62","79","44","58","14","187","151","243","139","142","174","106","228","102","223","31","120","5","43","255","179","66","119","170","60","152","167","194","4","112","156","233","254","203","1","55","53","19","92","21","28","42"],["141","162","146","57","23","45","158","93","212","38","2","206","246","225","195","189","47","193","224","242","76","138","84","140","111","51","135","113","41","133","207","30","82","175","161","6","249","83","234","155","244","177","108","252","94","143","173","8","154","75","50","49","39","36","182","101","48","12","172","87","250","59","24","157","215","218","72","185","71","7","253","114","230","226","110","46","166","91","130","20","137","117","132","204","221","52","197","188","11","232","67","115","245","26","35","103","186","37","27","235","64","40","70","239","236","211","61","29","216","199","63","54","78","105","184","15","10","147","247","22","144","107","128","17","178","148","129","192"],["109","134","145","89","181","81","159","168","34","251","3","205","213","202","99","121","80","149","18","65","201","227","116","69","100","74","160","198","16","131","0","73","210","122","209","217","97","237","136","98","229","248","176","95","150","171","238","191","200","220","33","219","126","9","214","124","56","86","169","208","125","85","25","88","13","190","153","183","96","165","180","90","164","104","240","123","118","196","163","222","231","127","241","77","68","32","62","79","44","58","14","187","151","243","139","142","174","106","228","102","223","31","120","5","43","255","179","66","119","170","60","152","167","194","4","112","156","233","254","203","1","55","53","19","92","21","28","42"],["141","162","146","57","23","45","158","93","212","38","2","206","246","225","195","189","47","193","224","242","76","138","84","140","111","51","135","113","41","133","207","30","82","175","161","6","249","83","234","155","244","177","108","252","94","143","173","8","154","75","50","49","39","36","182","101","48","12","172","87","250","59","24","157","215","218","72","185","71","7","253","114","230","226","110","46","166","91","130","20","137","117","132","204","221","52","197","188","11","232","67","115","245","26","35","103","186","37","27","235","64","40","70","239","236","211","61","29","216","199","63","54","78","105","184","15","10","147","247","22","144","107","128","17","178","148","129","192"]]},"finalized":false,"execution_optimistic":false}` + "\n"
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
			require.Equal(t, string(out), expected)
		})
	}
}

func TestGetStateFinalityCheckpoints(t *testing.T) {

	// setupTestingHandler(t, clparams.Phase0Version)
	_, blocks, _, _, postState, handler, _, _, fcu := setupTestingHandler(t, clparams.BellatrixVersion)

	postRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)

	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot

	fcu.JustifiedCheckpointVal = solid.NewCheckpointFromParameters(fcu.HeadVal, fcu.HeadSlotVal/32)

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
	expected := `{"data":{"finalized_checkpoint":{"epoch":"1","root":"0xde46b0f2ed5e72f0cec20246403b14c963ec995d7c2825f3532b0460c09d5693"},"current_justified_checkpoint":{"epoch":"3","root":"0xa6e47f164b1a3ca30ea3b2144bd14711de442f51e5b634750a12a1734e24c987"},"previous_justified_checkpoint":{"epoch":"2","root":"0x4c3ee7969e485696669498a88c17f70e6999c40603e2f4338869004392069063"}},"finalized":false,"version":2,"execution_optimistic":false}` + "\n"
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
			require.Equal(t, string(out), expected)
		})
	}
}

func TestGetRandao(t *testing.T) {

	// setupTestingHandler(t, clparams.Phase0Version)
	_, blocks, _, _, postState, handler, _, _, fcu := setupTestingHandler(t, clparams.BellatrixVersion)

	postRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)

	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot

	fcu.JustifiedCheckpointVal = solid.NewCheckpointFromParameters(fcu.HeadVal, fcu.HeadSlotVal/32)

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
	expected := `{"data":{"randao":"0xdeec617717272914bfd73e02ca1da113a83cf4cf33cd4939486509e2da4ccf4e"},"finalized":false,"execution_optimistic":false}` + "\n"
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
			require.Equal(t, string(out), expected)
		})
	}
}
