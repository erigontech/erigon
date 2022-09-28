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

package sentinel

import (
	"encoding/hex"
	"reflect"
	"strings"
	"time"

	"github.com/ledgerwatch/erigon/cmd/lightclient/clparams"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto/p2p"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto/ssz_snappy"

	"github.com/ledgerwatch/log/v3"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
)

var ProtocolPrefix = "/eth2/beacon_chain/req"

func getHandlers(s *Sentinel) map[protocol.ID]network.StreamHandler {
	return map[protocol.ID]network.StreamHandler{
		protocol.ID(ProtocolPrefix + "/ping/1/ssz_snappy"):                   curryStreamHandler(ssz_snappy.NewStreamCodec, pingHandler),
		protocol.ID(ProtocolPrefix + "/status/1/ssz_snappy"):                 curryStreamHandler(ssz_snappy.NewStreamCodec, statusHandler),
		protocol.ID(ProtocolPrefix + "/goodbye/1/ssz_snappy"):                curryStreamHandler(ssz_snappy.NewStreamCodec, s.goodbyeHandler),
		protocol.ID(ProtocolPrefix + "/metadata/1/ssz_snappy"):               curryStreamHandler(ssz_snappy.NewStreamCodec, metadataHandler),
		protocol.ID(ProtocolPrefix + "/beacon_blocks_by_range/1/ssz_snappy"): s.blocksByRangeHandler,
		protocol.ID(ProtocolPrefix + "/beacon_blocks_by_root/1/ssz_snappy"):  s.beaconBlocksByRootHandler,
	}
}

func setDeadLines(stream network.Stream) {
	if err := stream.SetReadDeadline(time.Now().Add(clparams.TtfbTimeout)); err != nil {
		log.Error("failed to set stream read dead line", "err", err)
	}
	if err := stream.SetWriteDeadline(time.Now().Add(clparams.RespTimeout)); err != nil {
		log.Error("failed to set stream write dead line", "err", err)
	}
}

// curryStreamHandler converts a func(ctx *proto.StreamContext, dat proto.Packet) error to func(network.Stream)
// this allows us to write encoding non specific type safe handler without performance overhead
func curryStreamHandler[T proto.Packet](newcodec func(network.Stream) proto.StreamCodec, fn func(ctx *proto.StreamContext, v T) error) func(network.Stream) {
	return func(s network.Stream) {
		setDeadLines(s)
		sd := newcodec(s)
		val := (*new(T)).Clone().(T)
		ctx, err := sd.Decode(val)
		if err != nil {
			// the stream reset error is ignored, because
			if !strings.Contains(err.Error(), "stream reset") {
				log.Warn("fail to decode packet", "err", err, "path", ctx.Protocol, "pkt", reflect.TypeOf(val))
			}
			return
		}
		err = fn(ctx, val)
		if err != nil {
			log.Warn("failed handling packet", "err", err, "path", ctx.Protocol, "pkt", reflect.TypeOf(val))
			return
		}
		log.Info("[ReqResp] Req->Host", "from", ctx.Stream.ID(), "endpount", ctx.Protocol, "msg", val)
	}
}

// type safe handlers which all have access to the original stream & decompressed data
// ping handler
func pingHandler(ctx *proto.StreamContext, dat *p2p.Ping) error {
	// since packets are just structs, they can be resent with no issue
	_, err := ctx.Codec.WritePacket(dat)
	if err != nil {
		return err
	}
	return nil
}

// TODO: respond with proper metadata
func metadataHandler(ctx *proto.StreamContext, dat *proto.EmptyPacket) error {
	return nil
}

func statusHandler(ctx *proto.StreamContext, dat *p2p.Status) error {
	log.Info("[ReqResp] Status",
		"epoch", dat.FinalizedEpoch,
		"final root", hexb(dat.FinalizedRoot),
		"head root", hexb(dat.HeadRoot),
		"head slot", dat.HeadSlot,
		"fork digest", hexb(dat.ForkDigest),
	)
	return nil
}

func hexb(b []byte) string {
	return hex.EncodeToString(b)
}

func (s *Sentinel) goodbyeHandler(ctx *proto.StreamContext, dat *p2p.Goodbye) error {
	//log.Info("[Lightclient] Received", "goodbye", dat.Reason)
	_, err := ctx.Codec.WritePacket(dat)
	if err != nil {
		return err
	}
	s.peers.DisconnectPeer(ctx.Stream.Conn().RemotePeer())
	return nil
}

func (s *Sentinel) blocksByRangeHandler(stream network.Stream) {
	log.Info("Got block by range handler call")
}

func (s *Sentinel) beaconBlocksByRootHandler(stream network.Stream) {
	log.Info("Got beacon block by root handler call")
}

func (s *Sentinel) setupHandlers() {
	for id, handler := range getHandlers(s) {
		s.host.SetStreamHandler(id, handler)
	}
}
