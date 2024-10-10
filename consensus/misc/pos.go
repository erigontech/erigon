package misc

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/types"
)

// Constants for The Merge as specified by EIP-3675: Upgrade consensus to Proof-of-Stake
var (
	ProofOfStakeDifficulty = libcommon.Big0     // PoS block's difficulty is always 0
	ProofOfStakeNonce      = types.BlockNonce{} // PoS block's have all-zero nonces
)

// IsPoSHeader reports the header belongs to the PoS-stage with some special fields.
// This function is not suitable for a part of APIs like Prepare or CalcDifficulty
// because the header difficulty is not set yet.
func IsPoSHeader(header *types.Header) bool {
	if header.Difficulty == nil {
		panic("IsPoSHeader called with invalid difficulty")
	}
	return header.Difficulty.Cmp(ProofOfStakeDifficulty) == 0
}
