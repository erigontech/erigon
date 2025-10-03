package main

import (
	erigon_lib_common "github.com/erigontech/erigon/common"
	erigon_crypto "github.com/erigontech/erigon/common/crypto"
	erigon_version "github.com/erigontech/erigon/db/version"
	geth_params "github.com/ethereum/go-ethereum/params"
)

func main() {
	println("Erigon version: ", erigon_version.VersionNoMeta)
	println("geth version: ", geth_params.Version)
	println("Erigon lib common eth Wei: ", erigon_lib_common.Wei)
	println("Erigon crypto secp256k1 S256 BitSize: ", erigon_crypto.S256().Params().BitSize)
	// not working due to duplicate symbols errors
	// println("geth crypto secp256k1 S256 BitSize: ", geth_crypto.S256().Params().BitSize)
}
