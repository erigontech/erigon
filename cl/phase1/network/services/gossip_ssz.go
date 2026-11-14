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

package services

import (
	"bytes"
	"errors"

	"github.com/erigontech/erigon/cl/clparams"
)

var errNonCanonicalSSZ = errors.New("non-canonical SSZ encoding")

type gossipSSZObject interface {
	DecodeSSZ(buf []byte, version int) error
	EncodeSSZ(buf []byte) ([]byte, error)
}

func decodeGossipSSZ(obj gossipSSZObject, data []byte, version clparams.StateVersion) error {
	if err := obj.DecodeSSZ(data, int(version)); err != nil {
		return err
	}
	canonical, err := obj.EncodeSSZ(make([]byte, 0, len(data)))
	if err != nil {
		return err
	}
	if !bytes.Equal(canonical, data) {
		return errNonCanonicalSSZ
	}
	return nil
}
