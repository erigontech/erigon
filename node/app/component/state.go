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

package component

import "strings"

type State int

const (
	Unknown = State(iota)
	Instantiated
	Configured
	Initialised
	Activating
	Recovering
	Active
	Deactivating
	Deactivated
	Failed
)

// UnmarshalText implements the encoding.TextUnmarshaler interface for XML/JSON
// deserialization.
func (e *State) UnmarshalText(text []byte) (err error) {
	*e, err = ParseState(string(text))
	return err
}

// MarshalText implements the encoding.TextMarshaler interface for XML/JSON
// serialization.
func (e State) MarshalText() (text []byte, err error) {
	return []byte(e.String()), nil
}

func (state State) IsConfigured() bool {
	return !(state == Unknown || state == Instantiated)
}

func (state State) IsActive() bool {
	return state == Active
}

func (state State) IsInitialized() bool {
	return !(state == Unknown || state == Instantiated ||
		state == Configured || state == Deactivated ||
		state == Failed)
}

func (state State) IsActivated() bool {
	return !(state == Unknown || state == Instantiated ||
		state == Configured || state == Initialised ||
		state == Deactivated || state == Failed)
}

func (state State) IsDeactivated() bool {
	return (state == Deactivating || state == Deactivated || state == Failed)
}

func (state State) String() string {
	switch state {
	case Instantiated:
		return "Instantiated"
	case Configured:
		return "Configured"
	case Initialised:
		return "Initialised"
	case Activating:
		return "Activating"
	case Recovering:
		return "Recovering"
	case Active:
		return "Active"
	case Deactivating:
		return "Deactivating"
	case Deactivated:
		return "Deactivated"
	case Failed:
		return "Failed"
	}

	return "Unknown"
}

func ParseState(stateName string) (State, error) {
	switch strings.ToUpper(stateName) {
	case "INSTANTIATED":
		return Instantiated, nil
	case "CONFIGURED":
		return Configured, nil
	case "INITIALISED", "INITIALIZED":
		return Initialised, nil
	case "ACTIVATING":
		return Activating, nil
	case "RECOVERING":
		return Recovering, nil
	case "ACTIVE":
		return Active, nil
	case "DEACTIVATING":
		return Deactivating, nil
	case "DEACTIVATED":
		return Deactivated, nil
	case "FAILED":
		return Failed, nil
	}

	return Unknown, nil
}
