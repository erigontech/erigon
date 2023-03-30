package rpctest

import (
	"fmt"
	"net/http"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

func Bench7(erigonURL, gethURL string) {
	setRoutes(erigonURL, gethURL)
	var client = &http.Client{
		Timeout: time.Second * 600,
	}

	blockhash := libcommon.HexToHash("0xdd3eb495312b11621669be45a2d50f8a66f2616bc72a610e2cbf1aebf9e4a9aa")
	reqID := 1
	to := libcommon.HexToAddress("0xbb9bc244d798123fde783fcc1c72d3bb8c189413")
	var sm map[libcommon.Hash]storageEntry
	var smg map[libcommon.Hash]storageEntry
	//start := libcommon.HexToHash("0x4a17477338cba00d8a94336ef62ea15f68e77ad0ca738fa405daa13bf0874134")
	start := libcommon.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000")

	reqID++
	template := `
{"jsonrpc":"2.0","method":"debug_storageRangeAt","params":["0x%x", %d,"0x%x","0x%x",%d],"id":%d}
	`
	i := 2
	nextKey := &start
	nextKeyG := &start
	sm = make(map[libcommon.Hash]storageEntry)
	smg = make(map[libcommon.Hash]storageEntry)
	for nextKey != nil {
		var sr DebugStorageRange
		if err := post(client, erigonURL, fmt.Sprintf(template, blockhash, i, to, *nextKey, 1024, reqID), &sr); err != nil {
			fmt.Printf("Could not get storageRange: %v\n", err)
			return
		}
		if sr.Error != nil {
			fmt.Printf("Error getting storageRange: %d %s\n", sr.Error.Code, sr.Error.Message)
			break
		} else {
			for k, v := range sr.Result.Storage {
				sm[k] = v
				if v.Key == nil {
					fmt.Printf("%x: %x", k, v)
				}
			}
			nextKey = sr.Result.NextKey
		}
	}

	for nextKeyG != nil {
		var srg DebugStorageRange
		if err := post(client, gethURL, fmt.Sprintf(template, blockhash, i, to, *nextKeyG, 1024, reqID), &srg); err != nil {
			fmt.Printf("Could not get storageRange: %v\n", err)
			return
		}
		if srg.Error != nil {
			fmt.Printf("Error getting storageRange: %d %s\n", srg.Error.Code, srg.Error.Message)
			break
		} else {
			for k, v := range srg.Result.Storage {
				smg[k] = v
				if v.Key == nil {
					fmt.Printf("%x: %x", k, v)
				}
			}
			nextKeyG = srg.Result.NextKey
		}
	}
	if !compareStorageRanges(sm, smg) {
		fmt.Printf("len(sm) %d, len(smg) %d\n", len(sm), len(smg))
		fmt.Printf("================sm\n")
		printStorageRange(sm)
		fmt.Printf("================smg\n")
		printStorageRange(smg)
		return
	}
	fmt.Printf("storageRanges: %d\n", len(sm))
}
