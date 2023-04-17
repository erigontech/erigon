package state

import "time"

// Proof struct
type Proof struct {
	BatchNumber      uint64
	BatchNumberFinal uint64
	Proof            string
	InputProver      string
	ProofID          *string
	// Prover name, unique identifier across prover reboots.
	Prover *string
	// ProverID prover process identifier.
	ProverID *string
	// GeneratingSince holds the timestamp for the moment in which the
	// proof generation has started by a prover. Nil if the proof is not
	// currently generating.
	GeneratingSince *time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
}
