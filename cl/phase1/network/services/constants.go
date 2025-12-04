// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package services

import (
	"errors"
	"time"

	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/utils/bls"
)

const (
	validatorAttestationCacheSize = 100_000
	proposerSlashingCacheSize     = 100
	seenBlockCacheSize            = 1000 // SeenBlockCacheSize is the size of the cache for seen blocks.
	blockJobsIntervalTick         = 50 * time.Millisecond
	blobJobsIntervalTick          = 5 * time.Millisecond
	singleAttestationIntervalTick = 10 * time.Millisecond
	attestationJobsIntervalTick   = 100 * time.Millisecond
	blockJobExpiry                = 30 * time.Second
	blobJobExpiry                 = 30 * time.Second
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

var (
	computeSigningRoot = fork.ComputeSigningRoot
	blsVerify          = bls.Verify
)
