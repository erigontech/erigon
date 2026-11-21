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

package main

import (
	"encoding/json"
	"log"
	"os"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/commitment/trie"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func main() {
	var block struct {
		StateRoot common.Hash                                 `json:"stateRoot"`
		Proofs    map[common.Address]*accounts.AccProofResult `json:"proofs"`
	}
	if err := json.NewDecoder(os.Stdin).Decode(&block); err != nil {
		log.Fatal(err)
	}
	if len(block.Proofs) == 0 {
		log.Fatal("no account proofs")
	}
	for address, proof := range block.Proofs {
		if proof == nil || proof.Balance == nil || proof.Address != address {
			log.Fatalf("missing or mismatched account proof for %s", address)
		}
		if err := trie.VerifyAccountProof(block.StateRoot, proof); err != nil {
			log.Fatalf("account %s: %v", address, err)
		}
	}
}
