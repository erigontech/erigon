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

package state_accessors

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
)

type validatorCursorTx struct {
	kv.Tx
	cursor kv.Cursor
}

func (tx validatorCursorTx) Cursor(string) (kv.Cursor, error) { return tx.cursor, nil }

type validatorErrorCursor struct {
	kv.Cursor
	row      []byte
	firstErr error
	nextErr  error
}

func (c validatorErrorCursor) First() ([]byte, []byte, error) {
	if c.firstErr != nil {
		return []byte{}, nil, c.firstErr
	}
	return []byte{0, 0, 0, 0}, c.row, nil
}

func (c validatorErrorCursor) Next() ([]byte, []byte, error) { return []byte{}, nil, c.nextErr }
func (c validatorErrorCursor) Close()                        {}

func TestReadValidatorsTableReturnsCursorError(t *testing.T) {
	var row bytes.Buffer
	require.NoError(t, NewStaticValidatorFromValidator(solid.NewValidator(), 0).Serialize(&row))

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	wantErr := errors.New("cursor failed")
	for _, test := range []struct {
		name   string
		cursor validatorErrorCursor
	}{
		{name: "First", cursor: validatorErrorCursor{firstErr: wantErr}},
		{name: "Next after a row", cursor: validatorErrorCursor{row: row.Bytes(), nextErr: wantErr}},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := db.View(t.Context(), func(tx kv.Tx) error {
				return ReadValidatorsTable(validatorCursorTx{Tx: tx, cursor: test.cursor}, NewStaticValidatorTable())
			})
			require.ErrorIs(t, err, wantErr)
		})
	}
}

func TestStaticValidatorTable(t *testing.T) {
	// Make 5 validators.
	vals := []solid.Validator{solid.NewValidator(), solid.NewValidator(), solid.NewValidator(), solid.NewValidator(), solid.NewValidator()}

	table := NewStaticValidatorTable()
	for i := range vals {
		require.NoError(t, table.AddValidator(vals[i].View(), uint64(i), 0))
	}
	// Now let us play with this
	require.NoError(t, table.AddExitEpoch(1, 32, 34))
	require.NoError(t, table.AddExitEpoch(1, 38, 35))
	require.NoError(t, table.AddExitEpoch(1, 2000, 500))

	require.Equal(t, uint64(0), table.validatorTable[1].ExitEpoch(31))
	require.Equal(t, uint64(34), table.validatorTable[1].ExitEpoch(32))
	require.Equal(t, uint64(34), table.validatorTable[1].ExitEpoch(37))
	require.Equal(t, uint64(35), table.validatorTable[1].ExitEpoch(38))
	require.Equal(t, uint64(35), table.validatorTable[1].ExitEpoch(450))
	require.Equal(t, uint64(500), table.validatorTable[1].ExitEpoch(1_000_000))
	// do the same for withdrawable epoch
	require.NoError(t, table.AddWithdrawableEpoch(1, 32, 34))
	require.NoError(t, table.AddWithdrawableEpoch(1, 38, 35))
	require.NoError(t, table.AddWithdrawableEpoch(1, 2000, 500))

	require.Equal(t, uint64(0), table.validatorTable[1].WithdrawableEpoch(31))
	require.Equal(t, uint64(34), table.validatorTable[1].WithdrawableEpoch(32))
	require.Equal(t, uint64(34), table.validatorTable[1].WithdrawableEpoch(37))
	require.Equal(t, uint64(35), table.validatorTable[1].WithdrawableEpoch(38))
	require.Equal(t, uint64(35), table.validatorTable[1].WithdrawableEpoch(450))
	require.Equal(t, uint64(500), table.validatorTable[1].WithdrawableEpoch(1_000_000))
	// now for withdrawal credentials
	require.NoError(t, table.AddWithdrawalCredentials(1, 32, common.HexToHash("0x2")))
	require.NoError(t, table.AddWithdrawalCredentials(1, 38, common.HexToHash("0x3")))
	require.NoError(t, table.AddWithdrawalCredentials(1, 2000, common.HexToHash("0x40")))
	require.Equal(t, common.Hash{}, table.validatorTable[1].WithdrawalCredentials(31))
	require.Equal(t, table.validatorTable[1].WithdrawalCredentials(32), common.HexToHash("0x2"))
	require.Equal(t, table.validatorTable[1].WithdrawalCredentials(37), common.HexToHash("0x2"))
	require.Equal(t, table.validatorTable[1].WithdrawalCredentials(38), common.HexToHash("0x3"))
	require.Equal(t, table.validatorTable[1].WithdrawalCredentials(450), common.HexToHash("0x3"))
	require.Equal(t, table.validatorTable[1].WithdrawalCredentials(1_000_000), common.HexToHash("0x40"))
	// Now lets try to get the validator at a specific epoch
	new := solid.NewValidator()
	table.validatorTable[1].ToValidator(new, 38)
	require.Equal(t, uint64(35), new.WithdrawableEpoch())
	require.Equal(t, uint64(35), new.ExitEpoch())
	require.Equal(t, new.WithdrawalCredentials(), common.HexToHash("0x3"))
	// Lastly serialize and deserialization
	table.ForEach(func(validatorIndex uint64, validator *StaticValidator) bool {
		var b bytes.Buffer
		require.NoError(t, validator.Serialize(&b))
		tmp := &StaticValidator{}
		require.NoError(t, tmp.Deserialize(&b))
		require.Equal(t, validator, tmp)
		return true
	})
}
