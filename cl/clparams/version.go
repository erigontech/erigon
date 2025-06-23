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

package clparams

import "fmt"

type StateVersion uint8

const (
	Phase0Version    StateVersion = 0
	AltairVersion    StateVersion = 1
	BellatrixVersion StateVersion = 2
	CapellaVersion   StateVersion = 3
	DenebVersion     StateVersion = 4
	ElectraVersion   StateVersion = 5
	FuluVersion      StateVersion = 6
)

func (v StateVersion) String() string {
	switch v {
	case Phase0Version:
		return "phase0"
	case AltairVersion:
		return "altair"
	case BellatrixVersion:
		return "bellatrix"
	case CapellaVersion:
		return "capella"
	case DenebVersion:
		return "deneb"
	case ElectraVersion:
		return "electra"
	case FuluVersion:
		return "fulu"
	default:
		panic("unsupported fork version")
	}
}

func (v StateVersion) Before(other StateVersion) bool {
	return v < other
}

func (v StateVersion) After(other StateVersion) bool {
	return v > other
}

func (v StateVersion) Equal(other StateVersion) bool {
	return v == other
}

func (v StateVersion) BeforeOrEqual(other StateVersion) bool {
	return v <= other
}

func (v StateVersion) AfterOrEqual(other StateVersion) bool {
	return v >= other
}

// stringToClVersion converts the string to the current state version.
func StringToClVersion(s string) (StateVersion, error) {
	switch s {
	case "phase0":
		return Phase0Version, nil
	case "altair":
		return AltairVersion, nil
	case "bellatrix":
		return BellatrixVersion, nil
	case "capella":
		return CapellaVersion, nil
	case "deneb":
		return DenebVersion, nil
	case "electra":
		return ElectraVersion, nil
	case "fulu":
		return FuluVersion, nil
	default:
		return 0, fmt.Errorf("unsupported fork version %s", s)
	}
}

func ClVersionToString(s StateVersion) string {
	return s.String()
}
