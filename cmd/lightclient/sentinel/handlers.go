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
	"reflect"
	"time"

	"github.com/ledgerwatch/erigon/cmd/lightclient/clparams"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto/p2p"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto/snappy_ssz"

	"github.com/ledgerwatch/log/v3"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/protocol"
)

var (
	ProtocolPrefix = "/eth2/beacon_chain/req"
	reservedBytes  = 128
)

var handlers map[protocol.ID]network.StreamHandler = map[protocol.ID]network.StreamHandler{
	protocol.ID(ProtocolPrefix + "/ping/1/ssz_snappy"):                   curryHandler(pingHandler),
	protocol.ID(ProtocolPrefix + "/status/1/ssz_snappy"):                 statusHandler,
	protocol.ID(ProtocolPrefix + "/goodbye/1/ssz_snappy"):                curryHandler(goodbyeHandler),
	protocol.ID(ProtocolPrefix + "/metadata/1/ssz_snappy"):               curryHandler(metadataHandler),
	protocol.ID(ProtocolPrefix + "/beacon_blocks_by_range/1/ssz_snappy"): blocksByRangeHandler,
	protocol.ID(ProtocolPrefix + "/beacon_blocks_by_root/1/ssz_snappy"):  beaconBlocksByRootHandler,
}

func setDeadLines(stream network.Stream) {
	if err := stream.SetReadDeadline(time.Now().Add(clparams.TtfbTimeout)); err != nil {
		log.Error("failed to set stream read dead line", "err", err)
	}
	if err := stream.SetWriteDeadline(time.Now().Add(clparams.RespTimeout)); err != nil {
		log.Error("failed to set stream write dead line", "err", err)
	}
}

func curryHandler[T proto.Packet](fn func(ctx *proto.Context, v T) error) func(network.Stream) {
	return func(s network.Stream) {
		setDeadLines(s)
		sd := snappy_ssz.NewCodec(s)
		val := (*new(T)).Clone().(T)
		ctx, err := sd.Decode(val)
		if err != nil {
			log.Warn("fail to decode packet", "err", err, "path", ctx.Protocol, "pkt", reflect.TypeOf(val))
			return
		}
		err = fn(ctx, val)
		if err != nil {
			log.Warn("failed handling packet", "err", err, "path", ctx.Protocol, "pkt", reflect.TypeOf(val))
			return
		}
	}
}

func pingHandler(ctx *proto.Context, dat *p2p.Ping) error {
	log.Info("[Lightclient] Received", "ping", dat.Id)
	_, err := ctx.Codec.WritePacket(dat)
	if err != nil {
		return err
	}
	return nil
}

func metadataHandler(ctx *proto.Context, dat *proto.EmptyPacket) error {
	log.Info("Got metadata call", "metadata", dat)
	return nil
}

func statusHandler(stream network.Stream) {
	setDeadLines(stream)
	log.Info("Got status request")
}

func goodbyeHandler(ctx *proto.Context, dat *p2p.Goodbye) error {
	setDeadLines(ctx.Stream)
	log.Info("[Lightclient] Received", "goodbye", dat.Reason)
	_, err := ctx.Stream.Write(ctx.Raw)
	if err != nil {
		return err
	}
	peerId := ctx.Stream.Conn().RemotePeer()
	disconnectPeerCh <- peerId
	return nil
}

func blocksByRangeHandler(stream network.Stream) {
	setDeadLines(stream)
	log.Info("Got block by range handler call")
}

func beaconBlocksByRootHandler(stream network.Stream) {
	setDeadLines(stream)
	log.Info("Got beacon block by root handler call")
}

func (s *Sentinel) setupHandlers() {
	for id, handler := range handlers {
		s.host.SetStreamHandler(id, handler)
	}
}
