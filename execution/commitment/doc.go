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
//
// A commit against provably-empty on-disk state (genesis, initial sync) routes to a
// whole-fresh account-plane fork-join (dispatchWholeFresh, fold_pool.go): each
// top-nibble subtree folds per prefix against an empty wall through the direct
// recursion, crossing every depth-64 seam into the account's fresh storage. The
// top-nibble subtrees and the interior splits inside them dispatch through the same
// slot-bounded fork-join contract (forkJoinEach, truthtree_fold.go) sharing one
// worker-count semaphore: a subtree forks onto its own goroutine when a slot is
// free and folds inline when saturated, so independent subtrees overlap instead of
// draining back-to-back. The route is internal to the parallel/streaming regimes —
// no flag — and any commit with on-disk state keeps the ordinary frontier fold
// unchanged.
//
// With DeferBranchUpdates the fold emits DeferredBranchUpdate records instead of
// writing branches to state; their prefix/raw/prev bytes live in a per-block arena
// owned by BranchEncoder rather than per-branch clones. The parallel/streaming
// regimes additionally merge each update against its previous branch bytes on the
// fold workers as subtrees complete, so the deferred list handed to the caller is
// pre-merged and the caller's flush is a pure write — the durable DB write happens
// at the caller's Commit. The serial engine applies its deferred updates inline at
// the end of Process, merging and writing at that barrier, unchanged.
package commitment
