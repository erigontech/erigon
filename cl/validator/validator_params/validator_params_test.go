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

package validator_params

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

func TestAttachedValidatorsAreIndependentFromFeeRecipients(t *testing.T) {
	params := NewValidatorParams()

	params.AddAttachedValidator(7)

	require.ElementsMatch(t, []uint64{7}, params.GetValidators())
	_, found := params.GetFeeRecipient(7)
	require.False(t, found)
	require.Zero(t, params.Generation())

	params.SetFeeRecipient(7, common.Address{0x11})
	params.SetFeeRecipient(8, common.Address{0x12})
	params.AddAttachedValidator(9)

	require.ElementsMatch(t, []uint64{7, 8, 9}, params.GetValidators())
	require.Equal(t, uint64(2), params.Generation())
	for _, index := range []uint64{7, 8, 9} {
		require.True(t, params.HasAttachedValidator(index))
	}
	require.False(t, params.HasAttachedValidator(10))
}

func newTestValidatorParams() (*ValidatorParams, *time.Time) {
	params := NewValidatorParams()
	now := time.Unix(1_700_000_000, 0)
	params.now = func() time.Time { return now }
	return params, &now
}

func TestAttachedValidatorExpiresWithoutRefresh(t *testing.T) {
	params, now := newTestValidatorParams()
	params.AddAttachedValidator(7)
	params.SetFeeRecipient(8, common.Address{0x12})

	*now = now.Add(attachmentTTL - time.Nanosecond)
	require.True(t, params.HasAttachedValidator(7))
	require.ElementsMatch(t, []uint64{7, 8}, params.GetValidators())

	*now = now.Add(time.Nanosecond)
	require.False(t, params.HasAttachedValidator(7))
	require.False(t, params.HasAttachedValidator(8))
	require.Empty(t, params.GetValidators())
	_, kept := params.attachedValidators.Load(uint64(7))
	require.False(t, kept, "GetValidators drops expired entries")
	feeRecipient, found := params.GetFeeRecipient(8)
	require.True(t, found, "fee recipients do not expire")
	require.Equal(t, common.Address{0x12}, feeRecipient)
}

func TestRefreshedValidatorStaysAttached(t *testing.T) {
	params, now := newTestValidatorParams()
	params.AddAttachedValidator(7)
	params.SetFeeRecipient(8, common.Address{0x12})

	*now = now.Add(attachmentTTL - time.Minute)
	params.AddAttachedValidator(7)
	params.SetFeeRecipient(8, common.Address{0x12})
	*now = now.Add(attachmentTTL - time.Minute)

	require.True(t, params.HasAttachedValidator(7))
	require.True(t, params.HasAttachedValidator(8))
	require.ElementsMatch(t, []uint64{7, 8}, params.GetValidators())
}

func TestExpiredValidatorIsAttachedAgain(t *testing.T) {
	params, now := newTestValidatorParams()
	params.AddAttachedValidator(7)
	*now = now.Add(attachmentTTL)
	require.Empty(t, params.GetValidators())

	params.AddAttachedValidator(7)

	require.True(t, params.HasAttachedValidator(7))
	require.ElementsMatch(t, []uint64{7}, params.GetValidators())
}

func TestRefreshDuringCleanupKeepsValidatorAttached(t *testing.T) {
	const validators = 2000
	for range 200 {
		params, now := newTestValidatorParams()
		for index := range uint64(validators) {
			params.AddAttachedValidator(index)
		}
		*now = now.Add(attachmentTTL)

		var wg sync.WaitGroup
		wg.Go(func() {
			for index := range uint64(validators) {
				params.AddAttachedValidator(index)
			}
		})
		wg.Go(func() { params.GetValidators() })
		wg.Wait()

		require.Len(t, params.GetValidators(), validators)
	}
}
