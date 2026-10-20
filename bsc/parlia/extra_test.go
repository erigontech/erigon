// Copyright 2026 The Erigon Authors
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

package parlia

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const chapelEpochLength = 1000

func firstEpochHeader(t *testing.T) uint64 {
	t.Helper()
	headers := loadChapelHeaders(t)
	first := headers[0].Number.Uint64()
	return (first/chapelEpochLength + 1) * chapelEpochLength
}

func TestParseValidatorsFromEpochHeader(t *testing.T) {
	t.Parallel()
	headers := loadChapelHeaders(t)
	h := headerAt(t, headers, firstEpochHeader(t))

	validators, voteAddrs, err := ParseValidators(h, chapelEpochLength)
	require.NoError(t, err)
	require.NotEmpty(t, validators)
	require.Len(t, voteAddrs, len(validators))
	require.Equal(t, int(h.Extra[extraVanity]), len(validators))
	require.Contains(t, validators, h.Coinbase)
	for _, va := range voteAddrs {
		require.NotEqual(t, BLSPublicKey{}, va)
	}

	turnLength, err := ParseTurnLength(h, chapelEpochLength)
	require.NoError(t, err)
	require.Positive(t, turnLength)
}

func TestParseValidatorsRejectsNonEpochHeader(t *testing.T) {
	t.Parallel()
	headers := loadChapelHeaders(t)
	h := headerAt(t, headers, firstEpochHeader(t)+1)

	_, _, err := ParseValidators(h, chapelEpochLength)
	require.ErrorIs(t, err, ErrNotEpochHeader)
	_, err = ParseTurnLength(h, chapelEpochLength)
	require.ErrorIs(t, err, ErrNotEpochHeader)
}

func TestParseVoteAttestation(t *testing.T) {
	t.Parallel()
	headers := loadChapelHeaders(t)
	epoch := firstEpochHeader(t)

	for _, num := range []uint64{epoch, epoch + 1, epoch + 2} {
		h := headerAt(t, headers, num)
		att, err := ParseVoteAttestation(h, chapelEpochLength)
		require.NoError(t, err, num)
		require.NotNil(t, att, num)
		require.NotNil(t, att.Data, num)
		require.NotZero(t, att.VoteAddressSet, num)
		require.Less(t, att.Data.SourceNumber, att.Data.TargetNumber, num)
		require.Less(t, att.Data.TargetNumber, num, num)
		require.Equal(t, headerAt(t, headers, att.Data.TargetNumber).Hash(), att.Data.TargetHash, num)
		require.Equal(t, headerAt(t, headers, att.Data.SourceNumber).Hash(), att.Data.SourceHash, num)
	}
}

func TestParseVoteAttestationAbsent(t *testing.T) {
	t.Parallel()
	headers := loadChapelHeaders(t)
	h := *headerAt(t, headers, firstEpochHeader(t)+1)
	h.Extra = append(append([]byte{}, h.Extra[:extraVanity]...), h.Extra[len(h.Extra)-extraSeal:]...)

	att, err := ParseVoteAttestation(&h, chapelEpochLength)
	require.NoError(t, err)
	require.Nil(t, att)
}

func TestParseVoteAttestationCorrupt(t *testing.T) {
	t.Parallel()
	headers := loadChapelHeaders(t)
	h := *headerAt(t, headers, firstEpochHeader(t)+1)
	h.Extra = append([]byte{}, h.Extra...)
	h.Extra[extraVanity] = 0xff

	_, err := ParseVoteAttestation(&h, chapelEpochLength)
	require.Error(t, err)
}
