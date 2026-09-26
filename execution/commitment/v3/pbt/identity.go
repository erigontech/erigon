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

package pbt

import (
	"encoding/binary"
	"fmt"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
)

type StoredRecord struct {
	Key   []byte
	Value []byte
}

func ValidateEngineIdentity(state []byte, records []StoredRecord) error {
	if err := ValidateEngineStateBlob(state); err != nil {
		return err
	}
	for i, record := range records {
		if len(record.Value) == 0 || commitment.IsCommitmentStateKey(record.Key) {
			continue
		}
		if _, err := DecodeRecord(record.Key, record.Value); err != nil {
			return fmt.Errorf("stored record %d key %x: %w", i, record.Key, err)
		}
	}
	return nil
}

func ValidateEngineStateBlob(state []byte) error {
	if commitment.IsPBinState(state) {
		return commitment.PBinValidateRowStateFormat(state)
	}
	if len(state) < 18 {
		return fmt.Errorf("commitment state is %d bytes, want a pbin blob or envelope", len(state))
	}
	stateLen := int(binary.BigEndian.Uint16(state[16:18]))
	if len(state) != 18+stateLen {
		return fmt.Errorf("commitment state envelope claims %d bytes, %d present", stateLen, len(state)-18)
	}
	return commitment.PBinValidateRowStateFormat(state[18:])
}

func ValidateEngineIdentityFromTx(tx kv.TemporalTx, domain kv.Domain) error {
	var state []byte
	for _, key := range [][]byte{commitment.KeyCommitmentV3State, commitment.KeyCommitmentState} {
		value, _, err := tx.GetLatest(domain, key, kv.GetLatestOptions{})
		if err != nil {
			return err
		}
		if len(value) == 0 {
			continue
		}
		state = value
		break
	}
	iterator, err := tx.Debug().RangeLatest(domain, nil, nil, kv.Unlim)
	if err != nil {
		return err
	}
	defer iterator.Close()
	records := make([]StoredRecord, 0)
	for iterator.HasNext() {
		key, value, err := iterator.Next()
		if err != nil {
			return err
		}
		records = append(records, StoredRecord{Key: append([]byte(nil), key...), Value: append([]byte(nil), value...)})
	}
	return ValidateEngineIdentity(state, records)
}
