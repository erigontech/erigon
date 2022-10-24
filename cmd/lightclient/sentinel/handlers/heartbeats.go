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
	"github.com/ledgerwatch/erigon/cmd/lightclient/cltypes"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/communication"
)

// type safe handlers which all have access to the original stream & decompressed data
// ping handler
func pingHandler(ctx *communication.StreamContext, dat *cltypes.Ping) error {
	// since packets are just structs, they can be resent with no issue
	return ctx.Codec.WritePacket(dat, SuccessfullResponsePrefix)
}

func (c *ConsensusHandlers) metadataV1Handler(ctx *communication.StreamContext, _ *communication.EmptyPacket) error {
	// since packets are just structs, they can be resent with no issue
	return ctx.Codec.WritePacket(&cltypes.MetadataV1{
		SeqNumber: c.metadata.SeqNumber,
		Attnets:   c.metadata.Attnets,
	}, SuccessfullResponsePrefix)
}

func (c *ConsensusHandlers) metadataV2Handler(ctx *communication.StreamContext, _ *communication.EmptyPacket) error {
	// since packets are just structs, they can be resent with no issue
	return ctx.Codec.WritePacket(c.metadata, SuccessfullResponsePrefix)
}

// does nothing
func nilHandler(ctx *communication.StreamContext, dat *communication.EmptyPacket) error {
	return nil
}

// TODO: Actually respond with proper status
func statusHandler(ctx *communication.StreamContext, dat *cltypes.Status) error {
	return ctx.Codec.WritePacket(dat, SuccessfullResponsePrefix)
}
