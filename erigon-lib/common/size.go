// Copyright 2014 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package common

import (
	"fmt"
)

// StorageSize is a wrapper around a float value that supports user friendly
// formatting.
type StorageSize float64

// String implements the stringer interface.
func (s StorageSize) String() string {
	if s > 1099511627776 {
		return fmt.Sprintf("%.2f TiB", s/1099511627776)
	} else if s > 1073741824 {
		return fmt.Sprintf("%.2f GiB", s/1073741824)
	} else if s > 1048576 {
		return fmt.Sprintf("%.2f MiB", s/1048576)
	} else if s > 1024 {
		return fmt.Sprintf("%.2f KiB", s/1024)
	} else {
		return fmt.Sprintf("%.2f B", s)
	}
}

func (s StorageSize) MarshalJSON() ([]byte, error) {
	return []byte("\"" + s.String() + "\""), nil
}

// TerminalString implements log.TerminalStringer, formatting a string for console
// output during logging.
func (s StorageSize) TerminalString() string {
	if s > 1099511627776 {
		return fmt.Sprintf("%.2fTiB", s/1099511627776)
	} else if s > 1073741824 {
		return fmt.Sprintf("%.2fGiB", s/1073741824)
	} else if s > 1048576 {
		return fmt.Sprintf("%.2fMiB", s/1048576)
	} else if s > 1024 {
		return fmt.Sprintf("%.2fKiB", s/1024)
	} else {
		return fmt.Sprintf("%.2fB", s)
	}
}

// Counter
type StorageCounter float64

// String implements the stringer interface.
func (s StorageCounter) String() string {
	if s > 1_000_000_000 {
		return fmt.Sprintf("%.2fB", s/1_000_000_000)
	} else if s > 1_000_000 {
		return fmt.Sprintf("%.2fM", s/1_000_000)
	} else if s > 1_000 {
		return fmt.Sprintf("%.2fK", s/1_000)
	} else {
		return fmt.Sprintf("%.2f", s)
	}
}

func (s StorageCounter) MarshalJSON() ([]byte, error) {
	return []byte("\"" + s.String() + "\""), nil
}

type StorageBucketWriteStats struct {
	KeyN             StorageCounter // total number of keys
	KeyBytesN        StorageSize    // total number of bytes owned by keys
	ValueBytesN      StorageSize    // total number of bytes owned by values
	TotalPut         StorageCounter
	TotalDelete      StorageCounter
	TotalBytesPut    StorageSize
	TotalBytesDelete StorageSize
}
