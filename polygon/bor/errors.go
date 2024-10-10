package bor

import (
	"fmt"
)

type MaxCheckpointLengthExceededError struct {
	Start uint64
	End   uint64
}

func (e *MaxCheckpointLengthExceededError) Error() string {
	return fmt.Sprintf(
		"Start: %d and end block: %d exceed max allowed checkpoint length: %d",
		e.Start,
		e.End,
		MaxCheckpointLength,
	)
}

// MismatchingValidatorsError is returned if a last block in sprint contains a
// list of validators different from the one that local node calculated
type MismatchingValidatorsError struct {
	Number             uint64
	ValidatorSetSnap   []byte
	ValidatorSetHeader []byte
}

func (e *MismatchingValidatorsError) Error() string {
	return fmt.Sprintf(
		"Mismatching validators at block %d\nValidatorBytes from snapshot: 0x%x\nValidatorBytes in Header: 0x%x\n",
		e.Number,
		e.ValidatorSetSnap,
		e.ValidatorSetHeader,
	)
}

type BlockTooSoonError struct {
	Number     uint64
	Succession int
}

func (e *BlockTooSoonError) Error() string {
	return fmt.Sprintf(
		"Block %d was created too soon. Signer turn-ness number is %d\n",
		e.Number,
		e.Succession,
	)
}

// WrongDifficultyError is returned if the difficulty of a block doesn't match the
// turn of the signer.
type WrongDifficultyError struct {
	Number   uint64
	Expected uint64
	Actual   uint64
	Signer   []byte
}

func (e *WrongDifficultyError) Error() string {
	return fmt.Sprintf(
		"Wrong difficulty at block %d, expected: %d, actual %d. Signer was %x\n",
		e.Number,
		e.Expected,
		e.Actual,
		e.Signer,
	)
}
