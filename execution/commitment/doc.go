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

// Package commitment implements Ethereum's hexary Patricia state-commitment trie:
// the hashing that produces the state root and the branch records emitted per trie
// prefix.
//
// A parallel leaf fold task computes its subtree cell one of two ways. The default
// mounts a transient HexPatricia row grid and replays each touched key through it
// (mount+replay). Behind TrieConfig.TruthtreeFold — the --experimental.truthtree-fold
// flag, parallel regime only, default off — a provably-fresh leaf subtree instead
// folds directly over its touched prefixTrie with reused per-goroutine scratch
// (truthtree_fold.go), avoiding that double materialization. Both paths emit
// byte-identical roots and branch records.
package commitment
