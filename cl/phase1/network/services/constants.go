package services

import (
	"errors"
	"time"
)

const (
	validatorAttestationCacheSize = 100_000
	proposerSlashingCacheSize     = 100
	seenBlockCacheSize            = 1000 // SeenBlockCacheSize is the size of the cache for seen blocks.
	blockJobsIntervalTick         = 50 * time.Millisecond
	blobJobsIntervalTick          = 5 * time.Millisecond
	singleAttestationIntervalTick = 10 * time.Millisecond
	attestationJobsIntervalTick   = 100 * time.Millisecond
	blockJobExpiry                = 7 * time.Minute
	blobJobExpiry                 = 7 * time.Minute
	attestationJobExpiry          = 30 * time.Minute
	singleAttestationJobExpiry    = 6 * time.Second
)

var (
	ErrIgnore                          = errors.New("ignore") // ErrIgnore is used to indicate that the message should be ignored.
	ErrBlockYoungerThanParent          = errors.New("block is younger than parent")
	ErrInvalidCommitmentsCount         = errors.New("invalid commitments count")
	ErrCommitmentsInclusionProofFailed = errors.New("commitments inclusion proof failed")
	ErrInvalidSidecarSlot              = errors.New("invalid sidecar slot")
	ErrBlobIndexOutOfRange             = errors.New("blob index out of range")
)
