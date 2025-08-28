package core

import (
	"crypto/ecdsa"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
)

// Pre-calculated version of:
//
//	DevnetSignPrivateKey = crypto.HexToECDSA(sha256.Sum256([]byte("erigon devnet key")))
//	DevnetEtherbase=crypto.PubkeyToAddress(DevnetSignPrivateKey.PublicKey)
var DevnetSignPrivateKey, _ = crypto.HexToECDSA("26e86e45f6fc45ec6e2ecd128cec80fa1d1505e5507dcd2ae58c3130a7a97b48")
var DevnetEtherbase = common.HexToAddress("67b1d87101671b127f5f8714789c7192f7ad340e")

// DevnetSignKey is defined like this to allow the devnet process to pre-allocate keys
// for nodes and then pass the address via --miner.etherbase - the function will be called
// to retieve the mining key
var DevnetSignKey = func(address common.Address) *ecdsa.PrivateKey {
	return DevnetSignPrivateKey
}
