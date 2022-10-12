/*
   Copyright 2022 Erigon-Lightclient contributors
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package main

import (
	"context"

	"github.com/ledgerwatch/erigon/cmd/lightclient/lightclient"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/log/v3"
)

const DefaultUri = "https://beaconstate.ethstaker.cc/eth/v2/debug/beacon/states/finalized"

func main() {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))

	bs, err := lightclient.RetrieveBeaconState(context.Background(), DefaultUri)
	if err != nil {
		log.Error("[Checkpoint Sync] Failed", "reason", err)
		return
	}
	log.Info("Finalized Checkpoint", "Epoch", bs.FinalizedCheckpoint.Epoch,
		"Root", common.Bytes2Hex(bs.FinalizedCheckpoint.Root[:]))
}
