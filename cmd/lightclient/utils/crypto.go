/*
   Copyright 2022 Erigon-Lightclient contributors
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package utils

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"sync"

	blst "github.com/supranational/blst/bindings/go"
)

var hasherPool = sync.Pool{
	New: func() interface{} {
		return sha256.New()
	},
}

func Keccak256(data []byte) [32]byte {
	h, ok := hasherPool.Get().(hash.Hash)
	if !ok {
		h = sha256.New()
	}
	defer hasherPool.Put(h)
	h.Reset()

	var b [32]byte

	h.Write(data)
	h.Sum(b[:0])

	return b
}

// PublicKeyFromBytes creates a BLS public key from a  BigEndian byte slice.
func PublicKeyFromBytes(pubKey []byte) (*blst.P1Affine, error) {
	if len(pubKey) != 48 {
		return nil, fmt.Errorf("public key must be 48 bytes")
	}

	// Subgroup check NOT done when decompressing pubkey.
	p := new(blst.P1Affine).Uncompress(pubKey)
	if p == nil {
		return nil, errors.New("could not unmarshal bytes into public key")
	}
	// Subgroup and infinity check
	if !p.KeyValidate() {
		return nil, fmt.Errorf("infinite key")
	}

	return p, nil
}

// SignatureFromBytes creates a BLS signature from a LittleEndian byte slice.
func SignatureFromBytes(sig []byte) (*blst.P2Affine, error) {
	if len(sig) != 96 {
		return nil, fmt.Errorf("signature must be 96 bytes")
	}
	signature := new(blst.P2Affine).Uncompress(sig)
	if signature == nil {
		return nil, errors.New("could not unmarshal bytes into signature")
	}
	// Group check signature. Do not check for infinity since an aggregated signature
	// could be infinite.
	if !signature.SigValidate(false) {
		return nil, errors.New("signature not in group")
	}
	return signature, nil
}
