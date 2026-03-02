package protocol

import "github.com/erigontech/erigon/common"

// RevertedTxGasUsed maps known Arbitrum transaction hashes that reverted
// to their actual gas used. These are historical mainnet transactions
// where gas accounting differs from standard EVM execution.
var RevertedTxGasUsed = map[common.Hash]uint64{
	common.HexToHash("0x58df300a7f04fe31d41d24672786cbe1c58b4f3d8329d0d74392d814dd9f7e40"): 45174,
}
