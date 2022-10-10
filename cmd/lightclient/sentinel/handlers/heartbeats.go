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

package handlers

import (
	"github.com/ledgerwatch/erigon/cmd/lightclient/rpc/lightrpc"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/communication"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/communication/p2p"
	"github.com/ledgerwatch/erigon/cmd/lightclient/utils"
	"github.com/ledgerwatch/log/v3"
)

// type safe handlers which all have access to the original stream & decompressed data
// ping handler
func pingHandler(ctx *communication.StreamContext, dat *p2p.Ping) error {
	// since packets are just structs, they can be resent with no issue
	_, err := ctx.Codec.WritePacket(dat, SuccessfullResponsePrefix)
	if err != nil {
		return err
	}
	return nil
}

func (c *ConsensusHandlers) metadataV1Handler(ctx *communication.StreamContext, _ *communication.EmptyPacket) error {
	// since packets are just structs, they can be resent with no issue
	_, err := ctx.Codec.WritePacket(&lightrpc.MetadataV1{
		SeqNumber: c.metadata.SeqNumber,
		Attnets:   c.metadata.Attnets,
	}, SuccessfullResponsePrefix)
	return err
}

func (c *ConsensusHandlers) metadataV2Handler(ctx *communication.StreamContext, _ *communication.EmptyPacket) error {
	// since packets are just structs, they can be resent with no issue
	_, err := ctx.Codec.WritePacket(c.metadata, SuccessfullResponsePrefix)
	return err
}

// does nothing
func nilHandler(ctx *communication.StreamContext, dat *communication.EmptyPacket) error {
	return nil
}

// TODO: Actually respond with proper status
func statusHandler(ctx *communication.StreamContext, dat *p2p.Status) error {
	log.Debug("[ReqResp] Status",
		"epoch", dat.FinalizedEpoch,
		"final root", utils.BytesToHex(dat.FinalizedRoot),
		"head root", utils.BytesToHex(dat.HeadRoot),
		"head slot", dat.HeadSlot,
		"fork digest", utils.BytesToHex(dat.ForkDigest),
	)
	_, err := ctx.Codec.WritePacket(dat, SuccessfullResponsePrefix)
	if err != nil {
		return err
	}
	return nil
}
