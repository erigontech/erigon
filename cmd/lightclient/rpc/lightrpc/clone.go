package lightrpc

import "github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto"

func (*SignedBeaconBlockBellatrix) Clone() proto.Packet {
	return &SignedBeaconBlockBellatrix{}
}
