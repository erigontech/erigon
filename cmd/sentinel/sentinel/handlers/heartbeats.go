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
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/communication"
)

// Type safe handlers which all have access to the original stream & decompressed data.
// Since packets are just structs, they can be resent with no issue

func (c *ConsensusHandlers) pingHandler(ctx *communication.StreamContext, _ *communication.EmptyPacket) error {
	return ctx.Codec.WritePacket(&cltypes.Ping{
		Id: c.metadata.SeqNumber,
	}, SuccessfulResponsePrefix)
}

func (c *ConsensusHandlers) goodbyeHandler(ctx *communication.StreamContext, _ *communication.EmptyPacket) error {
	// From the spec, these are the valid goodbye numbers. Start with just
	// sending 1, but we should think about when the others need to be sent.
	// 1: Client shut down.
	// 2: Irrelevant network.
	// 3: Fault/error.
	return ctx.Codec.WritePacket(&cltypes.Ping{
		Id: 1,
	}, SuccessfulResponsePrefix)
}

func (c *ConsensusHandlers) metadataV1Handler(ctx *communication.StreamContext, _ *communication.EmptyPacket) error {
	return ctx.Codec.WritePacket(&cltypes.MetadataV1{
		SeqNumber: c.metadata.SeqNumber,
		Attnets:   c.metadata.Attnets,
	}, SuccessfulResponsePrefix)
}

func (c *ConsensusHandlers) metadataV2Handler(ctx *communication.StreamContext, _ *communication.EmptyPacket) error {
	return ctx.Codec.WritePacket(c.metadata, SuccessfulResponsePrefix)
}

// TODO: Actually respond with proper status
func (c *ConsensusHandlers) statusHandler(ctx *communication.StreamContext, dat *cltypes.Status) error {
	return ctx.Codec.WritePacket(dat, SuccessfulResponsePrefix)
}
