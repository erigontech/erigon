// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package p2p

import (
	"bytes"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/ledgerwatch/erigon/rlp"

	"github.com/davecgh/go-spew/spew"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/p2p/simulations/pipes"
)

func TestProtocolHandshake(t *testing.T) {
	var (
		prv0, _ = crypto.GenerateKey()
		pub0    = crypto.MarshalPubkey(&prv0.PublicKey)
		hs0     = &protoHandshake{Version: 3, Pubkey: pub0, Caps: []Cap{{"a", 0}, {"b", 2}}}

		prv1, _ = crypto.GenerateKey()
		pub1    = crypto.MarshalPubkey(&prv1.PublicKey)
		hs1     = &protoHandshake{Version: 3, Pubkey: pub1, Caps: []Cap{{"c", 1}, {"d", 3}}}

		wg sync.WaitGroup
	)

	fd0, fd1, err := pipes.TCPPipe()
	if err != nil {
		t.Fatal(err)
	}

	wg.Add(2)
	go func() {
		defer wg.Done()
		defer fd0.Close()
		frame := newRLPX(fd0, &prv1.PublicKey)
		rpubkey, err := frame.doEncHandshake(prv0)
		if err != nil {
			t.Errorf("dial side enc handshake failed: %v", err)
			return
		}
		if !reflect.DeepEqual(rpubkey, &prv1.PublicKey) {
			t.Errorf("dial side remote pubkey mismatch: got %v, want %v", rpubkey, &prv1.PublicKey)
			return
		}

		phs, err := frame.doProtoHandshake(hs0)
		if err != nil {
			t.Errorf("dial side proto handshake error: %v", err)
			return
		}
		phs.Rest = nil
		if !reflect.DeepEqual(phs, hs1) {
			t.Errorf("dial side proto handshake mismatch:\ngot: %s\nwant: %s\n", spew.Sdump(phs), spew.Sdump(hs1))
			return
		}
		frame.close(DiscQuitting)
	}()
	go func() {
		defer wg.Done()
		defer fd1.Close()
		rlpx := newRLPX(fd1, nil)
		rpubkey, err := rlpx.doEncHandshake(prv1)
		if err != nil {
			t.Errorf("listen side enc handshake failed: %v", err)
			return
		}
		if !reflect.DeepEqual(rpubkey, &prv0.PublicKey) {
			t.Errorf("listen side remote pubkey mismatch: got %v, want %v", rpubkey, &prv0.PublicKey)
			return
		}

		phs, err := rlpx.doProtoHandshake(hs1)
		if err != nil {
			t.Errorf("listen side proto handshake error: %v", err)
			return
		}
		phs.Rest = nil
		if !reflect.DeepEqual(phs, hs0) {
			t.Errorf("listen side proto handshake mismatch:\ngot: %s\nwant: %s\n", spew.Sdump(phs), spew.Sdump(hs0))
			return
		}

		if err := ExpectMsg(rlpx, discMsg, []DiscReason{DiscQuitting}); err != nil {
			t.Errorf("error receiving disconnect: %v", err)
		}
	}()
	wg.Wait()
}

func TestProtocolHandshakeErrors(t *testing.T) {
	tests := []struct {
		code uint64
		msg  interface{}
		err  error
	}{
		{
			code: discMsg,
			msg:  []DiscReason{DiscQuitting},
			err:  DiscQuitting,
		},
		{
			code: 0x989898,
			msg:  []byte{1},
			err:  errors.New("expected handshake, got 989898"),
		},
		{
			code: handshakeMsg,
			msg:  make([]byte, baseProtocolMaxMsgSize+2),
			err:  errors.New("message too big"),
		},
		{
			code: handshakeMsg,
			msg:  []byte{1, 2, 3},
			err:  NewPeerError(PeerErrorInvalidMessage, DiscProtocolError, rlp.WrapStreamError(rlp.ErrExpectedList, reflect.TypeOf(protoHandshake{})), "(code 0) (size 4)"),
		},
		{
			code: handshakeMsg,
			msg:  &protoHandshake{Version: 3},
			err:  DiscInvalidIdentity,
		},
	}

	for i, test := range tests {
		p1, p2 := MsgPipe()
		go Send(p1, test.code, test.msg) //nolint:errcheck
		_, err := readProtocolHandshake(p2)
		if !reflect.DeepEqual(err, test.err) {
			t.Errorf("test %d: error mismatch: got %q, want %q", i, err, test.err)
		}
	}
}

func TestDisconnectMessagePayloadDecode(t *testing.T) {
	var buffer bytes.Buffer
	err := DisconnectMessagePayloadEncode(&buffer, DiscTooManyPeers)
	if err != nil {
		t.Error(err)
	}
	reason, err := DisconnectMessagePayloadDecode(&buffer)
	if err != nil {
		t.Error(err)
	}
	if reason != DiscTooManyPeers {
		t.Fail()
	}

	// plain integer
	reason, err = DisconnectMessagePayloadDecode(bytes.NewBuffer([]byte{uint8(DiscTooManyPeers)}))
	if err != nil {
		t.Error(err)
	}
	if reason != DiscTooManyPeers {
		t.Fail()
	}

	// single-element RLP list
	reason, err = DisconnectMessagePayloadDecode(bytes.NewBuffer([]byte{0xC1, uint8(DiscTooManyPeers)}))
	if err != nil {
		t.Error(err)
	}
	if reason != DiscTooManyPeers {
		t.Fail()
	}

	// empty RLP list
	reason, err = DisconnectMessagePayloadDecode(bytes.NewBuffer([]byte{0xC0}))
	if err != nil {
		t.Error(err)
	}
	if reason != DiscRequested {
		t.Fail()
	}

	// empty payload
	reason, err = DisconnectMessagePayloadDecode(bytes.NewBuffer([]byte{}))
	if err != nil {
		t.Error(err)
	}
	if reason != DiscRequested {
		t.Fail()
	}
}
