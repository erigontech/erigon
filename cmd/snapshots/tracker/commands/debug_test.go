package commands

import (
	"encoding/json"
	"github.com/anacrolix/torrent/bencode"
	"github.com/davecgh/go-spew/spew"
	"github.com/ledgerwatch/turbo-geth/common/math"
	"strconv"
	"testing"
)



func TestName(t *testing.T) {
	v:=ScrapeResponse{
		Files: map[string]*ScrapeData{
			"data.mdb": &ScrapeData{
				2, 5, 6,
			},
		},
	}



	b,err:=bencode.Marshal(v)
	t.Log(err)
	t.Log(b)
	t.Log(string(b))
	vv,err:=json.Marshal(v)
	t.Log(err)
	t.Log(string(vv))

	mp:=map[string]interface{}{}
	err = bencode.Unmarshal([]byte("d5:filesd20:xxxxxxxxxxxxxxxxxxxxd8:completei11e10:downloadedi13772e10:incompletei19e\n20:yyyyyyyyyyyyyyyyyyyyd8:completei21e10:downloadedi206e10:incompletei20eee"), mp)
	t.Log(err)
	spew.Dump(mp)
}
func TestName2(t *testing.T) {
	var v uint64
	t.Log(231928233984)
	t.Log(math.MaxInt64)
	t.Log(v)
	t.Log(v-1)
	t.Log(v-2)
	t.Log(strconv.ParseUint("18446744073709551615", 10, 64))

}

/*
t=43597&supportcrypto=1&uploaded=8110080"
Mar 02 17:01:16 snapshot-tracker tracker[611]: INFO [03-02|17:01:16.357] call                                     url="/announce?compact=1&downloaded=0&info_hash=M%CE%BD%F2%0Fg%CE%0AG%8F%D5%05%9ALa%3A%C9a%E18&left=0&peer_id=-GT0002-%80tA%9F%CE%B5%AE%B9%F6%8F%1D%04&port=32941&supportcrypto=1&uploaded=3733929984"
Mar 02 17:01:16 snapshot-tracker tracker[611]: INFO [03-02|17:01:16.362] Skipped                                  k=4dcebdf20f67ce0a478fd5059a4c613ac961e1382d5452323834302d756a397938716a3675747a37 last updated=2021-03-02T16:52:02+0000 now=2021-03-02T17:01:16+0000
Mar 02 17:01:16 snapshot-tracker tracker[611]: INFO [03-02|17:01:16.362] Skipped                                  k=4dcebdf20f67ce0a478fd5059a4c613ac961e1382d5452323834302d766875793970776f79736f30 last updated=2021-03-02T16:55:28+0000 now=2021-03-02T17:01:16+0000
Mar 02 17:01:19 snapshot-tracker tracker[611]: INFO [03-02|17:01:19.276] call                                     url="/announce?compact=1&downloaded=0&info_hash=%8F%02G%11%B2%C2%C2w%10%9BD%05%3F%CA%AB%1B%134ni&left=0&peer_id=-GT0002-d%B7c%25%D0%0E%D9%DB%26%C7%9F%E0&port=33859&supportcrypto=1&uploaded=30809423872"
Mar 02 17:01:19 snapshot-tracker tracker[611]: INFO [03-02|17:01:19.282] call                                     url="/announce?compact=1&downloaded=0&info_hash=M%CE%BD%F2%0Fg%CE%0AG%8F%D5%05%9ALa%3A%C9a%E18&left=0&peer_id=-GT0002-d%B7c%25%D0%0E%D9%DB%26%C7%9F%E0&port=33859&supportcrypto=1&uploaded=11315085312"
Mar 02 17:01:19 snapshot-tracker tracker[611]: INFO [03-02|17:01:19.286] Skipped                                  k=4dcebdf20f67ce0a478fd5059a4c613ac961e1382d5452323834302d756a397938716a3675747a37 last updated=2021-03-02T16:52:02+0000 now=2021-03-02T17:01:19+0000
Mar 02 17:01:19 snapshot-tracker tracker[611]: INFO [03-02|17:01:19.286] Skipped                                  k=4dcebdf20f67ce0a478fd5059a4c613ac961e1382d5452323834302d766875793970776f79736f30 last updated=2021-03-02T16:55:28+0000 now=2021-03-02T17:01:19+0000
Mar 02 17:01:21 snapshot-tracker tracker[611]: INFO [03-02|17:01:21.898] call                                     url="/announce?compact=1&downloaded=0&info_hash=%29o%17%03%F6%8A%FBF%C3%DF%04%0B%09~%26%28%FC%27%A6m&left=101130465280&peer_id=-GT0002-d%B7c%25%D0%0E%D9%DB%26%C7%9F%E0&port=33859&supportcrypto=1&uploaded=5457199104"
Mar 02 17:01:25 snapshot-tracker tracker[611]: INFO [03-02|17:01:25.769] call                                     url="/announce?info_hash=M%ce%bd%f2%0fg%ce%0aG%8f%d5%05%9aLa%3a%c9a%e18&peer_id=-TR2840-vhuy9pwoyso0&port=51413&uploaded=0&downloaded=0&left=18446744073709551615&numwant=80&key=77de6423&compact=1&supportcrypto=1&event=started"
Mar 02 17:01:25 snapshot-tracker tracker[611]: WARN [03-02|17:01:25.769] left                                     err="strconv.ParseInt: parsing \"18446744073709551615\": value out of range"
Mar 02 17:01:25 snapshot-tracker tracker[611]: INFO [03-02|17:01:25.774] Skipped                                  k=4dcebdf20f67ce0a478fd5059a4c613ac961e1382d5452323834302d756a397938716a3675747a37 last updated=2021-03-02T16:52:02+0000 now=2021-03-02T17:01:25+0000
Mar 02 17:01:26 snapshot-tracker tracker[611]: INFO [03-02|17:01:26.740] call                                     url="/announce?compact=1&downloaded=0&info_hash=%29o%17%03%F6%8A%FBF%C3%DF%04%0B%09~%26%28%FC%27%A6m&left=216236847104&peer_id=-GT0002-%80tA%9F%CE%B5%AE%B9%F6%8F%1D%04&port=32941&supportcrypto=1&uploaded=2752512"
Mar 02 17:01:45 snapshot-tracker tracker[611]: INFO [03-02|17:01:45.707] call                                     url="/announce?info_hash=M%ce%bd%f2%0fg%ce%0aG%8f%d5%05%9aLa%3a%c9a%e18&peer_id=-TR2840-vhuy9pwoyso0&port=51413&uploaded=0&downloaded=0&left=18446744073709551615&numwant=80&key=77de6423&compact=1&supportcrypto=1&event=started"
Mar 02 17:01:45 snapshot-tracker tracker[611]: WARN [03-02|17:01:45.707] left                                     err="strconv.ParseInt: parsing \"18446744073709551615\": value out of range"
Mar 02 17:01:45 snapshot-tracker tracker[611]: INFO [03-02|17:01:45.712] Skipped                                  k=4dcebdf20f67ce0a478fd5059a4c613ac961e1382d5452323834302d756a397938716a3675747a37 last updated=2021-03-02T16:52:02+0000 now=2021-03-02T17:01:45+0000


 17:05:13 snapshot-tracker tracker[879]: INFO [03-02|17:05:13.064] Skipped                                  k=4dcebdf20f67ce0a478fd5059a4c613ac961e1382d5452323834302d756a397938716a3675747a37 last updated=2021-03-02T16:52:02+0000 now=2021-03-02T17:05:13+0000
Mar 02 17:05:13 snapshot-tracker tracker[879]: INFO [03-02|17:05:13.064] Skipped                                  k=4dcebdf20f67ce0a478fd5059a4c613ac961e1382d5452323834302d766875793970776f79736f30 last updated=2021-03-02T17:01:45+0000 now=2021-03-02T17:05:13+0000
Mar 02 17:05:13 snapshot-tracker tracker[879]: INFO [03-02|17:05:13.064] announce resp                            v="{\"FailureReason\":\"\",\"Interval\":60,\"TrackerId\":\"tg snapshot tracker\",\"Complete\":3,\"Incomplete\":1,\"Peers\":[{\"ip\":\"34.65.246.27\",\"peer id\":\"LUdUMDAwMi1G7XWzl6OINX3XPGo=\",\"port\":43597},{\"ip\":\"34.65.70.130\",\"peer id\":\"LUdUMDAwMi1kt2Ml0A7Z2ybHn+A=\",\"port\":33859},{\"ip\":\"35.230.136.173\",\"peer id\":\"LUdUMDAwMi2AdEGfzrWuufaPHQQ=\",\"port\":32941},{\"ip\":\"217.138.221.85\",\"peer id\":\"LVRSMjg0MC0xYW1scmk3MHppMGw=\",\"port\":51413}]}"
Mar 02 17:05:16 snapshot-tracker tracker[879]: INFO [03-02|17:05:16.408] call                                     url="/announce?compact=1&downloaded=0&info_hash=M%CE%BD%F2%0Fg%CE%0AG%8F%D5%05%9ALa%3A%C9a%E18&left=0&peer_id=-GT0002-%80tA%9F%CE%B5%AE%B9%F6%8F%1D%04&port=32941&supportcrypto=1&uploaded=3733929984"
Mar 02 17:05:16 snapshot-tracker tracker[879]: INFO [03-02|17:05:16.415] Skipped                                  k=4dcebdf20f67ce0a478fd5059a4c613ac961e1382d5452323834302d756a397938716a3675747a37 last updated=2021-03-02T16:52:02+0000 now=2021-03-02T17:05:16+0000
Mar 02 17:05:16 snapshot-tracker tracker[879]: INFO [03-02|17:05:16.415] Skipped                                  k=4dcebdf20f67ce0a478fd5059a4c613ac961e1382d5452323834302d766875793970776f79736f30 last updated=2021-03-02T17:01:45+0000 now=2021-03-02T17:05:16+0000
Mar 02 17:05:16 snapshot-tracker tracker[879]: INFO [03-02|17:05:16.416] announce resp                            v="{\"FailureReason\":\"\",\"Interval\":60,\"TrackerId\":\"tg snapshot tracker\",\"Complete\":3,\"Incomplete\":1,\"Peers\":[{\"ip\":\"34.65.246.27\",\"peer id\":\"LUdUMDAwMi1G7XWzl6OINX3XPGo=\",\"port\":43597},{\"ip\":\"34.65.70.130\",\"peer id\":\"LUdUMDAwMi1kt2Ml0A7Z2ybHn+A=\",\"port\":33859},{\"ip\":\"35.230.136.173\",\"peer id\":\"LUdUMDAwMi2AdEGfzrWuufaPHQQ=\",\"port\":32941},{\"ip\":\"217.138.221.85\",\"peer id\":\"LVRSMjg0MC0xYW1scmk3MHppMGw=\",\"port\":51413}]}"
Mar 02 17:05:17 snapshot-tracker tracker[879]: INFO [03-02|17:05:17.212] call                                     url="/announce?compact=1&downloaded=5041864704&info_hash=%29o%17%03%F6%8A%FBF%C3%DF%04%0B%09~%26%28%FC%27%A6m&left=211197079552&peer_id=-GT0002-F%EDu%B3%97%A3%885%7D%D7%3Cj&port=43597&supportcrypto=1&uploaded=8110080"
Mar 02 17:05:17 snapshot-tracker tracker[879]: INFO [03-02|17:05:17.219] announce resp                            v="{\"FailureReason\":\"\",\"Interval\":60,\"TrackerId\":\"tg snapshot tracker\",\"Complete\":0,\"Incomplete\":3,\"Peers\":[{\"ip\":\"34.65.246.27\",\"peer id\":\"LUdUMDAwMi1G7XWzl6OINX3XPGo=\",\"port\":43597},{\"ip\":\"34.65.70.130\",\"peer id\":\"LUdUMDAwMi1kt2Ml0A7Z2ybHn+A=\",\"port\":33859},{\"ip\":\"35.230.136.173\",\"peer id\":\"LUdUMDAwMi2AdEGfzrWuufaPHQQ=\",\"port\":32941}]}"
Mar 02 17:05:19 snapshot-tracker tracker[879]: INFO [03-02|17:05:19.467] call                                     url="/announce?compact=1&downloaded=0&info_hash=%8F%02G%11%B2%C2%C2w%10%9BD%05%3F%CA%AB%1B%134ni&left=0&peer_id=-GT0002-d%B7c%25%D0%0E%D9%DB%26%C7%9F%E0&port=33859&supportcrypto=1&uploaded=30859264000"
Mar 02 17:05:19 snapshot-tracker tracker[879]: INFO [03-02|17:05:19.473] announce resp                            v="{\"FailureReason\":\"\",\"Interval\":60,\"TrackerId\":\"tg snapshot tracker\",\"Complete\":2,\"Incomplete\":1,\"Peers\":[{\"ip\":\"34.65.246.27\",\"peer id\":\"LUdUMDAwMi1G7XWzl6OINX3XPGo=\",\"port\":43597},{\"ip\":\"34.65.70.130\",\"peer id\":\"LUdUMDAwMi1kt2Ml0A7Z2ybHn+A=\",\"port\":33859},{\"ip\":\"35.230.136.173\",\"peer id\":\"LUdUMDAwMi2AdEGfzrWuufaPHQQ=\",\"port\":32941}]}"
Mar 02 17:05:19 snapshot-tracker tracker[879]: INFO [03-02|17:05:19.478] call                                     url="/announce?compact=1&downloaded=0&info_hash=M%CE%BD%F2%0Fg%CE%0AG%8F%D5%05%9ALa%3A%C9a%E18&left=0&peer_id=-GT0002-d%B7c%25%D0%0E%D9%DB%26%C7%9F%E0&port=33859&supportcrypto=1&uploaded=11315085312"
Mar 02 17:05:19 snapshot-tracker tracker[879]: INFO [03-02|17:05:19.486] Skipped                                  k=4dcebdf20f67ce0a478fd5059a4c613ac961e1382d5452323834302d756a397938716a3675747a37 last updated=2021-03-02T16:52:02+0000 now=2021-03-02T17:05:19+0000
Mar 02 17:05:19 snapshot-tracker tracker[879]: INFO [03-02|17:05:19.486] Skipped                                  k=4dcebdf20f67ce0a478fd5059a4c613ac961e1382d5452323834302d766875793970776f79736f30 last updated=2021-03-02T17:01:45+0000 now=2021-03-02T17:05:19+0000
Mar 02 17:05:19 snapshot-tracker tracker[879]: INFO [03-02|17:05:19.486] announce resp                            v="{\"FailureReason\":\"\",\"Interval\":60,\"TrackerId\":\"tg snapshot tracker\",\"Complete\":3,\"Incomplete\":1,\"Peers\":[{\"ip\":\"34.65.246.27\",\"peer id\":\"LUdUMDAwMi1G7XWzl6OINX3XPGo=\",\"port\":43597},{\"ip\":\"34.65.70.130\",\"peer id\":\"LUdUMDAwMi1kt2Ml0A7Z2ybHn+A=\",\"port\":33859},{\"ip\":\"35.230.136.173\",\"peer id\":\"LUdUMDAwMi2AdEGfzrWuufaPHQQ=\",\"port\":32941},{\"ip\":\"217.138.221.85\",\"peer id\":\"LVRSMjg0MC0xYW1scmk3MHppMGw=\",\"port\":51413}]}"
Mar 02 17:05:22 snapshot-tracker tracker[879]: INFO [03-02|17:05:22.110] call                                     url="/announce?compact=1&downloaded=0&info_hash=%29o%17%03%F6%8A%FBF%C3%DF%04%0B%09~%26%28%FC%27%A6m&left=101130465280&peer_id=-GT0002-d%B7c%25%D0%0E%D9%DB%26%C7%9F%E0&port=33859&supportcrypto=1&uploaded=5466963968"
Mar 02 17:05:22 snapshot-tracker tracker[879]: INFO [03-02|17:05:22.114] announce resp                            v="{\"FailureReason\":\"\",\"Interval\":60,\"TrackerId\":\"tg snapshot tracker\",\"Complete\":0,\"Incomplete\":3,\"Peers\":[{\"ip\":\"34.65.246.27\",\"peer id\":\"LUdUMDAwMi1G7XWzl6OINX3XPGo=\",\"port\":43597},{\"ip\":\"34.65.70.130\",\"peer id\":\"LUdUMDAwMi1kt2Ml0A7Z2ybHn+A=\",\"port\":33859},{\"ip\":\"35.230.136.173\",\"peer id\":\"LUdUMDAwMi2AdEGfzrWuufaPHQQ=\",\"port\":32941}]}"


Mar 02 17:32:32 snapshot-tracker tracker[1701]: WARN [03-02|17:32:32.013] scrape                                   url="/scrape?info_hash=M%ce%bd%f2%0fg%ce%0aG%8f%d5%05%9aLa%3a%c9a%e18"
Mar 02 17:32:32 snapshot-tracker tracker[1701]: INFO [03-02|17:32:32.013] scrape resp                              v="{\"files\":{\"Mν\\ufffd\\u000fg\\ufffd\\nG\\ufffd\\ufffd\\u0005\\ufffdLa:\\ufffda\\ufffd8\":{\"complete\":3,\"downloaded\":3,\"incomplete\":7}}}"
Mar 02 17:32:36 snapshot-tracker tracker[1701]: INFO [03-02|17:32:36.767] call                                     url="/announce?compact=1&downloaded=5101436928&info_hash=%29o%17%03%F6%8A%FBF%C3%DF%04%0B%09~%26%28%FC%27%A6m&left=211137507328&peer_id=-GT0002-F%EDu%B3%97%A3%885%7D%D7%3Cj&port=43597&supportcrypto=1&uploaded=8110080"
Mar 02 17:32:36 snapshot-tracker tracker[1701]: INFO [03-02|17:32:36.783] announce resp                            v="{\"interval\":60,\"tracker_id\":\"tg snapshot tracker\",\"complete\":0,\"incomplete\":3,\"peers\":[{\"ip\":\"34.65.246.27\",\"peer id\":\"LUdUMDAwMi1G7XWzl6OINX3XPGo=\",\"port\":43597},{\"ip\":\"34.65.70.130\",\"peer id\":\"LUdUMDAwMi1kt2Ml0A7Z2ybHn+A=\",\"port\":33859},{\"ip\":\"35.230.136.173\",\"peer id\":\"LUdUMDAwMi2AdEGfzrWuufaPHQQ=\",\"port\":32941}]}"
^FMar 02 17:32:51 snapshot-tracker tracker[1701]: INFO [03-02|17:32:51.225] call                                     url="/announce?compact=1&downloaded=0&info_hash=%8F%02G%11%B2%C2%C2w%10%9BD%05%3F%CA%AB%1B%134ni&left=0&peer_id=-GT0002-%80tA%9F%CE%B5%AE%B9%F6%8F%1D%04&port=32941&supportcrypto=1&uploaded=964657152"
Mar 02 17:32:51 snapshot-tracker tracker[1701]: INFO [03-02|17:32:51.230] announce resp                            v="{\"interval\":60,\"tracker_id\":\"tg snapshot tracker\",\"complete\":3,\"incomplete\":0,\"peers\":[{\"ip\":\"34.65.246.27\",\"peer id\":\"LUdUMDAwMi1G7XWzl6OINX3XPGo=\",\"port\":43597},{\"ip\":\"34.65.70.130\",\"peer id\":\"LUdUMDAwMi1kt2Ml0A7Z2ybHn+A=\",\"port\":33859},{\"ip\":\"35.230.136.173\",\"peer id\":\"LUdUMDAwMi2AdEGfzrWuufaPHQQ=\",\"port\":32941}]}"
Mar 02 17:33:10 snapshot-tracker tracker[1701]: INFO [03-02|17:33:10.046] call                                     url="/announce?compact=1&downloaded=32161648640&info_hash=%8F%02G%11%B2%C2%C2w%10%9BD%05%3F%CA%AB%1B%134ni&left=0&peer_id=-GT0002-F%EDu%B3%97%A3%885%7D%D7%3Cj&port=43597&supportcrypto=1&uploaded=0"
Mar 02 17:33:10 snapshot-tracker tracker[1701]: INFO [03-02|17:33:10.051] announce resp                            v="{\"interval\":60,\"tracker_id\":\"tg snapshot tracker\",\"complete\":3,\"incomplete\":0,\"peers\":[{\"ip\":\"34.65.246.27\",\"peer id\":\"LUdUMDAwMi1G7XWzl6OINX3XPGo=\",\"port\":43597},{\"ip\":\"34.65.70.130\",\"peer id\":\"LUdUMDAwMi1kt2Ml0A7Z2ybHn+A=\",\"port\":33859},{\"ip\":\"35.230.136.173\",\"peer id\":\"LUdUMDAwMi2AdEGfzrWuufaPHQQ=\",\"port\":32941}]}"
Mar 02 17:33:16 snapshot-tracker tracker[1701]: INFO [03-02|17:33:16.780] call                                     url="/announce?compact=1&downloaded=0&info_hash=M%CE%BD%F2%0Fg%CE%0AG%8F%D5%05%9ALa%3A%C9a%E18&left=0&peer_id=-GT0002-%80tA%9F%CE%B5%AE%B9%F6%8F%1D%04&port=32941&supportcrypto=1&uploaded=3733929984"
Mar 02 17:33:16 snapshot-tracker tracker[1701]: INFO [03-02|17:33:16.786] announce resp                            v="{\"interval\":60,\"tracker_id\":\"tg snapshot tracker\",\"complete\":3,\"incomplete\":1,\"peers\":[{\"ip\":\"34.65.246.27\",\"peer id\":\"LUdUMDAwMi1G7XWzl6OINX3XPGo=\",\"port\":43597},{\"ip\":\"34.65.70.130\",\"peer id\":\"LUdUMDAwMi1kt2Ml0A7Z2ybHn+A=\",\"port\":33859},{\"ip\":\"35.230.136.173\",\"peer id\":\"LUdUMDAwMi2AdEGfzrWuufaPHQQ=\",\"port\":32941},{\"ip\":\"217.138.221.85\",\"peer id\":\"LVRSMjg0MC0yazB3aTR1MWlpczk=\",\"port\":51413}]}"
Mar 02 17:33:20 snapshot-tracker tracker[1701]: INFO [03-02|17:33:20.818] call                                     url="/announce?compact=1&downloaded=0&info_hash=M%CE%BD%F2%0Fg%CE%0AG%8F%D5%05%9ALa%3A%C9a%E18&left=0&peer_id=-GT0002-d%B7c%25%D0%0E%D9%DB%26%C7%9F%E0&port=33859&supportcrypto=1&uploaded=11333238784"
Mar 02 17:33:20 snapshot-tracker tracker[1701]: INFO [03-02|17:33:20.823] call                                     url="/announce?compact=1&downloaded=0&info_hash=%8F%02G%11%B2%C2%C2w%10%9BD%05%3F%CA%AB%1B%134ni&left=0&peer_id=-GT0002-d%B7c%25%D0%0E%D9%DB%26%C7%9F%E0&port=33859&supportcrypto=1&uploaded=31342317568"
Mar 02 17:33:20 snapshot-tracker tracker[1701]: INFO [03-02|17:33:20.828] announce resp                            v="{\"interval\":60,\"tracker_id\":\"tg snapshot tracker\",\"complete\":3,\"incomplete\":1,\"peers\":[{\"ip\":\"34.65.246.27\",\"peer id\":\"LUdUMDAwMi1G7XWzl6OINX3XPGo=\",\"port\":43597},{\"ip\":\"34.65.70.130\",\"peer id\":\"LUdUMDAwMi1kt2Ml0A7Z2ybHn+A=\",\"port\":33859},{\"ip\":\"35.230.136.173\",\"peer id\":\"LUdUMDAwMi2AdEGfzrWuufaPHQQ=\",\"port\":32941},{\"ip\":\"217.138.221.85\",\"peer id\":\"LVRSMjg0MC0yazB3aTR1MWlpczk=\",\"port\":51413}]}"
Mar 02 17:33:20 snapshot-tracker tracker[1701]: INFO [03-02|17:33:20.837] announce resp                            v="{\"interval\":60,\"tracker_id\":\"tg snapshot tracker\",\"complete\":3,\"incomplete\":0,\"peers\":[{\"ip\":\"34.65.246.27\",\"peer id\":\"LUdUMDAwMi1G7XWzl6OINX3XPGo=\",\"port\":43597},{\"ip\":\"34.65.70.130\",\"peer id\":\"LUdUMDAwMi1kt2Ml0A7Z2ybHn+A=\",\"port\":33859},{\"ip\":\"35.230.136.173\",\"peer id\":\"LUdUMDAwMi2AdEGfzrWuufaPHQQ=\",\"port\":32941}]}"
Mar 02 17:33:23 snapshot-tracker tracker[1701]: INFO [03-02|17:33:23.563] call                                     url="/announce?compact=1&downloaded=0&info_hash=%29o%17%03%F6%8A%FBF%C3%DF%04%0B%09~%26%28%FC%27%A6m&left=101130465280&peer_id=-GT0002-d%B7c%25%D0%0E%D9%DB%26%C7%9F%E0&port=33859&supportcrypto=1&uploaded=5534121984"
Mar 02 17:33:23 snapshot-tracker tracker[1701]: INFO [03-02|17:33:23.571] announce resp                            v="{\"interval\":60,\"tracker_id\":\"tg snapshot tracker\",\"complete\":0,\"incomplete\":3,\"peers\":[{\"ip\":\"34.65.246.27\",\"peer id\":\"LUdUMDAwMi1G7XWzl6OINX3XPGo=\",\"port\":43597},{\"ip\":\"34.65.70.130\",\"peer id\":\"LUdUMDAwMi1kt2Ml0A7Z2ybHn+A=\",\"port\":33859},{\"ip\":\"35.230.136.173\",\"peer id\":\"LUdUMDAwMi2AdEGfzrWuufaPHQQ=\",\"port\":32941}]}"
Mar 02 17:33:24 snapshot-tracker tracker[1701]: INFO [03-02|17:33:24.341] call                                     url="/announce?compact=1&downloaded=7341309952&info_hash=M%CE%BD%F2%0Fg%CE%0AG%8F%D5%05%9ALa%3A%C9a%E18&left=0&peer_id=-GT0002-F%EDu%B3%97%A3%885%7D%D7%3Cj&port=43597&supportcrypto=1&uploaded=311115776"
Mar 02 17:33:24 snapshot-tracker tracker[1701]: INFO [03-02|17:33:24.350] announce resp                            v="{\"interval\":60,\"tracker_id\":\"tg snapshot tracker\",\"complete\":3,\"incomplete\":1,\"peers\":[{\"ip\":\"34.65.246.27\",\"peer id\":\"LUdUMDAwMi1G7XWzl6OINX3XPGo=\",\"port\":43597},{\"ip\":\"34.65.70.130\",\"peer id\":\"LUdUMDAwMi1kt2Ml0A7Z2ybHn+A=\",\"port\":33859},{\"ip\":\"35.230.136.173\",\"peer id\":\"LUdUMDAwMi2AdEGfzrWuufaPHQQ=\",\"port\":32941},{\"ip\":\"217.138.221.85\",\"peer id\":\"LVRSMjg0MC0yazB3aTR1MWlpczk=\",\"port\":51413}]}"
Mar 02 17:33:27 snapshot-tracker tracker[1701]: INFO [03-02|17:33:27.528] call                                     url="/announce?compact=1&downloaded=0&info_hash=%29o%17%03%F6%8A%FBF%C3%DF%04%0B%09~%26%28%FC%27%A6m&left=216236847104&peer_id=-GT0002-%80tA%9F%CE%B5%AE%B9%F6%8F%1D%04&port=32941&supportcrypto=1&uploaded=2752512"
Mar 02 17:33:27 snapshot-tracker tracker[1701]: INFO [03-02|17:33:27.533] announce resp                            v="{\"interval\":60,\"tracker_id\":\"tg snapshot tracker\",\"complete\":0,\"incomplete\":3,\"peers\":[{\"ip\":\"34.65.246.27\",\"peer id\":\"LUdUMDAwMi1G7XWzl6OINX3XPGo=\",\"port\":43597},{\"ip\":\"34.65.70.130\",\"peer id\":\"LUdUMDAwMi1kt2Ml0A7Z2ybHn+A=\",\"port\":33859},{\"ip\":\"35.230.136.173\",\"peer id\":\"LUdUMDAwMi2AdEGfzrWuufaPHQQ=\",\"port\":32941}]}"
Mar 02 17:33:37 snapshot-tracker tracker[1701]: INFO [03-02|17:33:37.326] call                                     url="/announce?compact=1&downloaded=5103583232&info_hash=%29o%17%03%F6%8A%FBF%C3%DF%04%0B%09~%26%28%FC%27%A6m&left=211135361024&peer_id=-GT0002-F%EDu%B3%97%A3%885%7D%D7%3Cj&port=43597&supportcrypto=1&uploaded=8110080"
Mar 02 17:33:37 snapshot-tracker tracker[1701]: INFO [03-02|17:33:37.331] announce resp                            v="{\"interval\":60,\"tracker_id\":\"tg snapshot tracker\",\"complete\":0,\"incomplete\":3,\"peers\":[{\"ip\":\"34.65.246.27\",\"peer id\":\"LUdUMDAwMi1G7XWzl6OINX3XPGo=\",\"port\":43597},{\"ip\":\"34.65.70.130\",\"peer id\":\"LUdUMDAwMi1kt2Ml0A7Z2ybHn+A=\",\"port\":33859},{\"ip\":\"35.230.136.173\",\"peer id\":\"LUdUMDAwMi2AdEGfzrWuufaPHQQ=\",\"port\":32941}]}"

*/