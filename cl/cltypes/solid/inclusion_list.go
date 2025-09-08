package solid

import (
	"crypto/sha256"
	"encoding/binary"
	"strconv"
)

type SignedInclusionList struct {
	Message   *InclusionList `json:"message"`
	Signature string         `json:"signature"`
}

type InclusionList struct {
	Slot                       string   `json:"slot"`
	ValidatorIndex             string   `json:"validator_index"`
	InclusionListCommitteeRoot string   `json:"inclusion_list_committee_root"`
	Transactions               []string `json:"transactions"`
}

// HashSSZ implements the ssz.HashableSSZ interface
func (il *InclusionList) HashSSZ() ([32]byte, error) {
	// Parse slot and validator index
	slot, err := strconv.ParseUint(il.Slot, 10, 64)
	if err != nil {
		return [32]byte{}, err
	}

	validatorIndex, err := strconv.ParseUint(il.ValidatorIndex, 10, 64)
	if err != nil {
		return [32]byte{}, err
	}

	// Create a hasher
	hasher := sha256.New()

	// Write slot (8 bytes, little endian)
	slotBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(slotBytes, slot)
	hasher.Write(slotBytes)

	// Write validator index (8 bytes, little endian)
	validatorIndexBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(validatorIndexBytes, validatorIndex)
	hasher.Write(validatorIndexBytes)

	// Write inclusion list committee root
	hasher.Write([]byte(il.InclusionListCommitteeRoot))

	// Write transactions
	for _, tx := range il.Transactions {
		hasher.Write([]byte(tx))
	}

	// Return the hash
	hash := hasher.Sum(nil)
	var result [32]byte
	copy(result[:], hash)
	return result, nil
}
