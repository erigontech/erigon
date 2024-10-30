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

package utils

import (
	"crypto/ecdsa"
	"encoding/hex"
	"fmt"

	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon/v3/p2p/enode"
)

func ParseHexPublicKey(keyStr string) (*ecdsa.PublicKey, error) {
	nodeWithPubkey, err := enode.ParseV4("enode://" + keyStr)
	if err != nil {
		return nil, fmt.Errorf("failed to decode a public key: %w", err)
	}
	return nodeWithPubkey.Pubkey(), nil
}

func ParseHexPublicKeys(hexKeys []string) ([]*ecdsa.PublicKey, error) {
	if hexKeys == nil {
		return nil, nil
	}
	keys := make([]*ecdsa.PublicKey, 0, len(hexKeys))
	for _, keyStr := range hexKeys {
		key, err := ParseHexPublicKey(keyStr)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

func HexEncodePublicKey(key *ecdsa.PublicKey) string {
	return hex.EncodeToString(crypto.MarshalPubkey(key))
}

func HexEncodePublicKeys(keys []*ecdsa.PublicKey) []string {
	if keys == nil {
		return nil
	}
	hexKeys := make([]string, 0, len(keys))
	for _, key := range keys {
		keyStr := HexEncodePublicKey(key)
		hexKeys = append(hexKeys, keyStr)
	}
	return hexKeys
}
