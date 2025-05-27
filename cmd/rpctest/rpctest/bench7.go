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

package rpctest

import (
	"errors"
	"fmt"

	"github.com/erigontech/erigon-lib/common"
)

func Bench7(erigonURL, gethURL string) error {
	setRoutes(erigonURL, gethURL)

	blockhash := common.HexToHash("0xdd3eb495312b11621669be45a2d50f8a66f2616bc72a610e2cbf1aebf9e4a9aa")
	reqID := 1
	to := common.HexToAddress("0xbb9bc244d798123fde783fcc1c72d3bb8c189413")
	var sm map[common.Hash]storageEntry
	var smg map[common.Hash]storageEntry
	//start := common.HexToHash("0x4a17477338cba00d8a94336ef62ea15f68e77ad0ca738fa405daa13bf0874134")
	start := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000")

	reqID++
	template := `
{"jsonrpc":"2.0","method":"debug_storageRangeAt","params":["0x%x", %d,"0x%x","0x%x",%d],"id":%d}
	`
	i := 2
	nextKey := &start
	nextKeyG := &start
	sm = make(map[common.Hash]storageEntry)
	smg = make(map[common.Hash]storageEntry)
	for nextKey != nil {
		var sr DebugStorageRange
		if err := post(client, erigonURL, fmt.Sprintf(template, blockhash, i, to, *nextKey, 1024, reqID), &sr); err != nil {
			return fmt.Errorf("Could not get storageRange: %v\n", err)
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
			return fmt.Errorf("Could not get storageRange: %v\n", err)
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
		return errors.New("storage are different")
	}
	fmt.Printf("storageRanges: %d\n", len(sm))
	return nil
}
