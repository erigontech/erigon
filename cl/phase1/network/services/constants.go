package services

import (
	"errors"
	"time"
)

const (
	SeenBlockCacheSize   = 1000 // SeenBlockCacheSize is the size of the cache for seen blocks.
	jobsIntervalTick     = 50 * time.Millisecond
	blockJobExpiry       = 64 * time.Second
	attestationJobExpiry = 30 * time.Minute
)

var (
	ErrIgnore                  = errors.New("ignore") // ErrIgnore is used to indicate that the message should be ignored.
	ErrBlockYoungerThanParent  = errors.New("block is younger than parent")
	ErrInvalidCommitmentsCount = errors.New("invalid commitments count")
)
