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

package hexutil

import "fmt"

// These errors are from go-ethereum in order to keep compatibility with geth error codes.
var (
	ErrEmptyString      = &decError{"empty hex string"}
	ErrSyntax           = &decError{"invalid hex string"}
	ErrMissingPrefix    = &decError{"hex string without 0x prefix"}
	ErrOddLength        = &decError{"hex string of odd length"}
	ErrEmptyNumber      = &decError{"hex string \"0x\""}
	ErrLeadingZero      = &decError{"hex number with leading zero digits"}
	ErrUint64Range      = &decError{"hex number > 64 bits"}
	ErrUintRange        = &decError{fmt.Sprintf("hex number > %d bits", uintBits)}
	ErrBig256Range      = &decError{"hex number > 256 bits"}
	ErrTooBigHexString  = &decError{"hex string too long, want at most 32 bytes"}
	ErrHexStringInvalid = &decError{"hex string invalid"}
)

type decError struct{ msg string }

func (err decError) Error() string { return err.msg }
