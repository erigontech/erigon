package valset

import "fmt"

// TotalVotingPowerExceededError is returned when the maximum allowed total voting power is exceeded
type TotalVotingPowerExceededError struct {
	Sum        int64
	Validators []*Validator
}

func (e *TotalVotingPowerExceededError) Error() string {
	return fmt.Sprintf(
		"Total voting power should be guarded to not exceed %v; got: %v; for validator set: %v",
		MaxTotalVotingPower,
		e.Sum,
		e.Validators,
	)
}

type InvalidStartEndBlockError struct {
	Start         uint64
	End           uint64
	CurrentHeader uint64
}

func (e *InvalidStartEndBlockError) Error() string {
	return fmt.Sprintf(
		"Invalid parameters start: %d and end block: %d params",
		e.Start,
		e.End,
	)
}

// UnauthorizedProposerError is returned if a header is [being] signed by an unauthorized entity.
type UnauthorizedProposerError struct {
	Number   uint64
	Proposer []byte
}

func (e *UnauthorizedProposerError) Error() string {
	return fmt.Sprintf(
		"Proposer 0x%x is not a part of the producer set at block %d",
		e.Proposer,
		e.Number,
	)
}

// UnauthorizedSignerError is returned if a header is [being] signed by an unauthorized entity.
type UnauthorizedSignerError struct {
	Number uint64
	Signer []byte
}

func (e *UnauthorizedSignerError) Error() string {
	return fmt.Sprintf(
		"Signer 0x%x is not a part of the producer set at block %d",
		e.Signer,
		e.Number,
	)
}
