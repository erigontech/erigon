package main

import (
	geth_params "github.com/ethereum/go-ethereum/params"
	// geth_crypto "github.com/ethereum/go-ethereum/crypto"
	erigon_lib_common "github.com/erigontech/erigon-lib/common"
	erigon_crypto "github.com/erigontech/erigon-lib/crypto"
	erigon_params "github.com/erigontech/erigon/v3/params"
)

func main() {
	println("Erigon version: ", erigon_params.Version)
	println("geth version: ", geth_params.Version)
	println("Erigon lib common eth Wei: ", erigon_lib_common.Wei)
	println("Erigon crypto secp256k1 S256 BitSize: ", erigon_crypto.S256().Params().BitSize)
	// not working due to duplicate symbols errors
	// println("geth crypto secp256k1 S256 BitSize: ", geth_crypto.S256().Params().BitSize)
}
