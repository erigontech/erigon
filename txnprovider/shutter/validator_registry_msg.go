// Copyright 2025 The Erigon Authors
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

package shutter

import (
	"encoding/binary"
	"fmt"

	"github.com/erigontech/erigon-lib/common"
)

const (
	LegacyValidatorRegistrationMessageVersion    = 0
	AggregateValidatorRegistrationMessageVersion = 1
)

type AggregateRegistrationMessage struct {
	Version                  uint8
	ChainId                  uint64
	ValidatorRegistryAddress common.Address
	ValidatorIndex           uint64
	Nonce                    uint32
	Count                    uint32
	IsRegistration           bool
}

func (m *AggregateRegistrationMessage) Marshal() []byte {
	b := make([]byte, 0)
	b = append(b, m.Version)
	b = binary.BigEndian.AppendUint64(b, m.ChainId)
	b = append(b, m.ValidatorRegistryAddress.Bytes()...)
	b = binary.BigEndian.AppendUint64(b, m.ValidatorIndex)
	b = binary.BigEndian.AppendUint32(b, m.Count)
	b = binary.BigEndian.AppendUint32(b, m.Nonce)
	if m.IsRegistration {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	return b
}

func (m *AggregateRegistrationMessage) Unmarshal(b []byte) error {
	expectedLength := 1 + 8 + 20 + 8 + 4 + 4 + 1
	if len(b) != expectedLength {
		return fmt.Errorf("invalid registration message length %d, expected %d", len(b), expectedLength)
	}

	m.Version = b[0]
	m.ChainId = binary.BigEndian.Uint64(b[1:9])
	m.ValidatorRegistryAddress = common.BytesToAddress(b[9:29])
	m.ValidatorIndex = binary.BigEndian.Uint64(b[29:37])
	m.Count = binary.BigEndian.Uint32(b[37:41])
	m.Nonce = binary.BigEndian.Uint32(b[41:45])
	switch b[45] {
	case 0:
		m.IsRegistration = false
	case 1:
		m.IsRegistration = true
	default:
		return fmt.Errorf("invalid registration message type byte %d", b[45])
	}
	return nil
}

func (m *AggregateRegistrationMessage) ValidatorIndices() []int64 {
	if m.Version == LegacyValidatorRegistrationMessageVersion {
		return []int64{int64(m.ValidatorIndex)}
	}
	indices := make([]int64, 0)
	for i := 0; i < int(m.Count); i++ {
		indices = append(indices, int64(m.ValidatorIndex)+int64(i))
	}
	return indices
}

type LegacyRegistrationMessage struct {
	Version                  uint8
	ChainId                  uint64
	ValidatorRegistryAddress common.Address
	ValidatorIndex           uint64
	Nonce                    uint64
	IsRegistration           bool
}

func (m *LegacyRegistrationMessage) Marshal() []byte {
	b := make([]byte, 0)
	b = append(b, m.Version)
	b = binary.BigEndian.AppendUint64(b, m.ChainId)
	b = append(b, m.ValidatorRegistryAddress.Bytes()...)
	b = binary.BigEndian.AppendUint64(b, m.ValidatorIndex)
	b = binary.BigEndian.AppendUint64(b, m.Nonce)
	if m.IsRegistration {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	return b
}

func (m *LegacyRegistrationMessage) Unmarshal(b []byte) error {
	expectedLength := 1 + 8 + 20 + 8 + 8 + 1
	if len(b) != expectedLength {
		return fmt.Errorf("invalid registration message length %d, expected %d", len(b), expectedLength)
	}

	m.Version = b[0]
	m.ChainId = binary.BigEndian.Uint64(b[1:9])
	m.ValidatorRegistryAddress = common.BytesToAddress(b[9:29])
	m.ValidatorIndex = binary.BigEndian.Uint64(b[29:37])
	m.Nonce = binary.BigEndian.Uint64(b[37:45])
	switch b[45] {
	case 0:
		m.IsRegistration = false
	case 1:
		m.IsRegistration = true
	default:
		return fmt.Errorf("invalid registration message type byte %d", b[45])
	}
	return nil
}
