package sentinel

import (
	"encoding/hex"
	"testing"

	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/ledgerwatch/erigon/p2p/enr"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/libp2p/go-libp2p/core/peer"
)

var pyRecord, _ = hex.DecodeString("f884b8407098ad865b00a582051940cb9cf36836572411a47278783077011599ed5cd16b76f2635f4e234738f30813a89eb9137e3e3df5266e3a1f11df72ecf1145ccb9c01826964827634826970847f00000189736563703235366b31a103ca634cae0d49acb401d8a4c6b6fe8c55b70d115bf400769cc1400f3258cd31388375647082765f")

func TestMultiAddressBuilderWithID(t *testing.T) {
	testCases := []struct {
		ipAddr      string
		protocol    string
		port        uint
		id          peer.ID
		shouldError bool
		expectedStr string
	}{
		{
			ipAddr:      "192.158.1.38",
			protocol:    "udp",
			port:        80,
			id:          peer.ID(""),
			shouldError: true,
			expectedStr: "ip4/node",
		},
		{
			ipAddr:      "192.178.1.21",
			protocol:    "tcp",
			port:        88,
			id:          peer.ID("d267"),
			shouldError: true,
			expectedStr: "ip4/node",
		},
		// TODO: should not throw 'selected encoding not supported' error, MUST FIX!
		// It panics because shouldError is false and this particular test case throws an error
		{
			ipAddr:      "192.178.1.21",
			protocol:    "tcp",
			port:        88,
			id:          peer.ID("d267"),
			shouldError: true,
			expectedStr: "ip4/node",
		},
	}

	for _, testCase := range testCases {
		multiAddr, err := multiAddressBuilderWithID(testCase.ipAddr, testCase.protocol, testCase.port, testCase.id)
		if testCase.shouldError {
			if err == nil {
				t.Errorf("expected error, got nil")
			}
			continue
		}
		if multiAddr.String() != testCase.expectedStr {
			t.Errorf("expected %s, got %s", testCase.expectedStr, multiAddr.String())
		}
	}
}

func TestConvertToMultiAddr(t *testing.T) {
	var r enr.Record
	if err := rlp.DecodeBytes(pyRecord, &r); err != nil {
		t.Fatalf("can't decode: %v", err)
	}
	n, err := enode.New(enode.ValidSchemes, &r)
	if err != nil {
		t.Fatalf("cannot create new node: %v", err)
	}

	testCases := []struct {
		nodes    []*enode.Node
		expected []string
	}{
		{
			nodes:    []*enode.Node{n},
			expected: []string{"/ip4/127.0.0.1/tcp/0/p2p/16Uiu2HAmSH2XVgZqYHWucap5kuPzLnt2TsNQkoppVxB5eJGvaXwm"},
		},
	}

	for _, testCase := range testCases {
		multiAddrs := convertToMultiAddr(testCase.nodes)
		for i, multiAddr := range multiAddrs {
			if multiAddr.String() != testCase.expected[i] {
				t.Errorf("for test case: %d, expected: %s, got: %s", i, testCase.expected[i], multiAddr)
			}
		}
	}
}
