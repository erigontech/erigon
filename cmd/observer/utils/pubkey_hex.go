package utils

import (
	"crypto/ecdsa"
	"encoding/hex"
	"fmt"

	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/p2p/enode"
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
