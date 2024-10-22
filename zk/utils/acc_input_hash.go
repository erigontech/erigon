package utils

import (
	"math/big"

	"github.com/iden3/go-iden3-crypto/keccak256"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/crypto"
)

// calculates the new accInputHash based on the old one and data frem one new batch
// this returns the accInputHash for the current batch
// oldAccInputHash - the accInputHash from the previous batch
func CalculateEtrogValidiumAccInputHash(
	oldAccInputHash common.Hash,
	batchTransactionData common.Hash,
	l1InfoRoot common.Hash,
	limitTimestamp uint64,
	sequencerAddress common.Address,
	forcedBlockHashL1 common.Hash,
) *common.Hash {
	return calculateEtrogAccInputHash(
		oldAccInputHash,
		batchTransactionData.Bytes(),
		l1InfoRoot,
		limitTimestamp,
		sequencerAddress,
		forcedBlockHashL1,
	)
}

// calculates the new accInputHash based on the old one and data frem one new batch
// this returns the accInputHash for the current batch
// oldAccInputHash - the accInputHash from the previous batch
func CalculateEtrogAccInputHash(
	oldAccInputHash common.Hash,
	batchTransactionData []byte,
	l1InfoRoot common.Hash,
	limitTimestamp uint64,
	sequencerAddress common.Address,
	forcedBlockHashL1 common.Hash,
) *common.Hash {
	return calculateEtrogAccInputHash(
		oldAccInputHash,
		CalculateBatchHashData(batchTransactionData),
		l1InfoRoot,
		limitTimestamp,
		sequencerAddress,
		forcedBlockHashL1,
	)
}

func calculateEtrogAccInputHash(
	oldAccInputHash common.Hash,
	batchDataHash []byte,
	l1InfoRoot common.Hash,
	limitTimestamp uint64,
	sequencerAddress common.Address,
	forcedBlockHashL1 common.Hash,
) *common.Hash {
	v1 := oldAccInputHash.Bytes()
	v2 := batchDataHash
	v3 := l1InfoRoot.Bytes()
	v4 := big.NewInt(0).SetUint64(limitTimestamp).Bytes()
	v5 := sequencerAddress.Bytes()
	v6 := forcedBlockHashL1.Bytes()

	// Add 0s to make values 32 bytes long
	for len(v1) < 32 {
		v1 = append([]byte{0}, v1...)
	}
	for len(v3) < 32 {
		v3 = append([]byte{0}, v3...)
	}
	for len(v4) < 8 {
		v4 = append([]byte{0}, v4...)
	}
	for len(v5) < 20 {
		v5 = append([]byte{0}, v5...)
	}
	for len(v6) < 32 {
		v6 = append([]byte{0}, v6...)
	}

	hash := common.BytesToHash(keccak256.Hash(v1, v2, v3, v4, v5, v6))

	return &hash
}

func CalculatePreEtrogValidiumAccInputHash(
	oldAccInputHash common.Hash,
	batchHashData common.Hash,
	globalExitRoot common.Hash,
	timestamp uint64,
	sequencerAddress common.Address,
) *common.Hash {
	return calculatePreEtrogAccInputHash(
		oldAccInputHash,
		batchHashData.Bytes(),
		globalExitRoot,
		timestamp,
		sequencerAddress,
	)
}

func CalculatePreEtrogAccInputHash(
	oldAccInputHash common.Hash,
	batchTransactionData []byte,
	globalExitRoot common.Hash,
	timestamp uint64,
	sequencerAddress common.Address,
) *common.Hash {
	batchHashData := CalculateBatchHashData(batchTransactionData)
	return calculatePreEtrogAccInputHash(
		oldAccInputHash,
		batchHashData,
		globalExitRoot,
		timestamp,
		sequencerAddress,
	)
}

// calculates the new accInputHash based on the old one and data frem one new batch
// this returns the accInputHash for the current batch
// oldAccInputHash - the accInputHash from the previous batch
func calculatePreEtrogAccInputHash(
	oldAccInputHash common.Hash,
	batchTransactionHash []byte,
	globalExitRoot common.Hash,
	timestamp uint64,
	sequencerAddress common.Address,
) *common.Hash {
	v1 := oldAccInputHash.Bytes()
	v2 := batchTransactionHash
	v3 := globalExitRoot.Bytes()
	v4 := big.NewInt(0).SetUint64(timestamp).Bytes()
	v5 := sequencerAddress.Bytes()

	// Add 0s to make values 32 bytes long
	for len(v1) < 32 {
		v1 = append([]byte{0}, v1...)
	}
	for len(v3) < 32 {
		v3 = append([]byte{0}, v3...)
	}
	for len(v4) < 8 {
		v4 = append([]byte{0}, v4...)
	}
	for len(v5) < 20 {
		v5 = append([]byte{0}, v5...)
	}

	hash := common.BytesToHash(keccak256.Hash(v1, v2, v3, v4, v5))

	return &hash
}

// parses batch transactions bytes into a batchHashData
// used for accInputHash calculation
// transactionBytes are as taken from the sequenceBatches calldata
func CalculateBatchHashData(transactions []byte) []byte {
	return crypto.Keccak256(transactions)
}
