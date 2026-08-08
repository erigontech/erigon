# EIP-8297 corpus gap: erigon Tasks 6-10 against the reference corpus

Every case erigon's spec-catch-up added for delegation (Tasks 6, 7), code
reclamation (Task 9) and the adversarial shapes (Task 10), placed beside its
nearest equivalent in `ethereum/execution-specs@projects/binary-trie`. The point
is to propose upstream only what the corpus does not already pin.

Compared against `projects/binary-trie` at `9a3b64e38` (base; the local
`~/org/wrk/espr` checkout also carries the unmerged Task 15 commit). Four
sources make up the corpus:

| Source | Size | What it pins |
| --- | --- | --- |
| `tests/binary_trie/vectors/binary_trie_vectors.json` | 18 `pbt_state` cases | flat state → root |
| `tests/binary_trie/test_embedding.py` | key/value derivation, chunkify, `embed_account`/`remove_account` | one state, leaf-level |
| `tests/binary_trie/test_state_pbt.py` | `BlockDiff` application over a provider | pre-state → post-state, leaf-level |
| `tests/binary_tree/eip8297_partitioned_binary_tree/` | 48 fixture tests (49 with Task 15's) | on-chain execution + committed root |

The plan's Task 16 named only the fixture suite as the minimum to check. The two
Python suites turned out to carry the closest equivalents by a wide margin, so
they decide most verdicts below.

## Case-by-case

| erigon case | nearest existing | verdict |
| --- | --- | --- |
| `TestPBinIsDelegationClassifiesByBytes` (`pbin_delegation_test.go`) | `test_embedding.py::test_delegation_is_classified_by_code_never_by_hash` (7 params) | covered — spec is a superset; it also pins a wrong marker byte (`0xef0101`), which erigon does not. Also `test_code_sharing.py::test_contract_hashing_to_the_delegation_marker_executes_as_code` and vector `code_hash_starting_with_the_delegation_marker` |
| `TestPBinEncodeDelegationPadsToThirtyTwo` | `test_embedding.py::test_delegation_leaf_value_layout` + `test_chunkify_designator_shaped_code_still_chunks` | covered — spec is a superset: it pins each target byte's position, not just the nine-zero tail |
| `TestPBinDelegationLeafIsExclusive/fresh EOA delegates` | `test_state_pbt.py::test_every_account_holds_exactly_one_of_the_two_leaves[delegated_eoa]`; vector `delegation_designator` | covered |
| `…/delegation replaces contract code` | `test_state_pbt.py::test_delegating_replaces_the_code_hash_leaf` + `test_delegating_an_account_reclaims_nothing` | covered — the second pins that the replaced code's chunks stay |
| `…/delegation cleared to empty code` | `test_state_pbt.py::test_undelegating_restores_the_empty_code_hash_leaf`; the `b""` arm of `test_delegation_change_replaces_the_header_leaf` | covered |
| `…/two authorities one target` | `test_state_pbt.py::test_authorities_to_one_target_hold_separate_delegation_leaves`; vector `two_authorities_one_target`; `test_code_sharing.py::test_shared_designator_survives_peer_redelegation` | covered |
| `TestPBinReclaimDropsCodeWithNoSurvivor` (`pbin_reclaim_test.go`) | `test_state_pbt.py::test_deleting_the_last_holder_removes_its_code`, `test_deleting_a_sole_holder_removes_its_short_code`, `test_deleting_the_last_holder_drops_every_group` | covered — see the divergence below |
| `TestPBinReclaimKeepsCodeForBatchSibling` | `test_state_pbt.py::test_deleting_a_holder_keeps_chunks_a_survivor_still_holds[diff_first,diff_last]` | covered — that test's diff arm is exactly the batch sibling, and it orders the survivor both ways |
| `TestPBinReclaimKeepsCodeForPreexistingHolder` | `test_state_pbt.py::test_deleting_one_holder_keeps_shared_code`, the pre-state arm of `…keeps_chunks_a_survivor_still_holds`; `test_code_sharing.py::test_shared_code_survives_sibling_same_tx_selfdestruct` | covered |
| `TestPBinDelegationSetAndClearedInOneBatch` (`pbin_adversarial_test.go`) | `test_state_pbt.py::test_undelegating_restores_the_empty_code_hash_leaf` | not applicable upstream — see below |
| `TestPBinDelegationRepointedInOneBatch` | `test_state_pbt.py::test_delegation_change_replaces_the_header_leaf`; step 1 of `…authorities_to_one_target…` | not applicable upstream — see below |
| `TestPBinZeroChunkAloneInItsGroup` | `test_state_pbt.py::test_absent_chunk_in_a_later_group_does_not_stall_removal`, `test_group_exact_code_fills_group_zero_and_nothing_more` | **absent** |
| `TestPBinSharedCodeOutlivesOneHolder` | `test_state_pbt.py::test_shared_code_survives_until_the_last_holder_is_gone`, `test_deleting_one_holder_keeps_shared_code`; vector `shared_bytecode_two_accounts` | covered |

## Shortlist

One case, not the two the plan expected.

**A zero chunk alone in its `tree_index` group.** A code of 257 chunks whose last
chunk is all-zero: group 1 exists in the code and holds no leaf at all, so its
stem is absent from the tree. Every existing group-1 case has at least one
present leaf there, or no chunk in group 1 at all:

- `test_absent_chunk_in_a_later_group_does_not_stall_removal` — 258 chunks with
  chunk 256 zeroed, so group 1 keeps chunk 257's leaf beside the hole.
- `test_group_exact_code_fills_group_zero_and_nothing_more` — 256 chunks, so
  group 1 has no chunk to place, zero or otherwise.
- `test_deleting_the_last_holder_drops_every_group` — 257 chunks from
  `_distinct_chunk_code`, whose only hole falls at chunk 223 (checked by running
  the helper, not by reading the cycle); chunk 256 is present.
- vector `code_across_the_group_boundary` — `TWO_GROUP_CODE` is 257 nonzero
  chunks.
- vector `code_chunks_of_zero_bytes` — 62 zero bytes, both chunks in group 0.
- execution-specs#3305's reframed `zero_chunk_across_the_group_boundary`
  (unmerged, Task 14) — 300 chunks with 5, 255 and 256 zeroed, so group 1 still
  holds 43 leaves. Adding this case would not close the gap.

The distinction it pins: whether a group's stem is derived from the chunk *ids*
the code reaches or from the leaves it actually places. An implementation that
materializes a stem per group it computes still commits the empty group; one
that derives keys per placed leaf does not.

It is expressible as a flat `pbt_state` vector case — one account, one code —
so it needs no transaction and belongs in the vector corpus. Its theme is
absent chunks at a group boundary, which is execution-specs#3305's theme, so
folding it into that branch is the coherent packaging rather than a second PR.

**Reclamation with no surviving holder is not a gap.** The plan expected it on
the shortlist; the reference corpus pins it three times over
(`test_deleting_a_sole_holder_removes_its_short_code`,
`test_deleting_the_last_holder_removes_its_code`,
`test_deleting_the_last_holder_drops_every_group`), plus the sequenced
`test_shared_code_survives_until_the_last_holder_is_gone` and the both-in-one-block
`test_two_holders_deleted_in_one_block_drop_the_code_once`.

## Two findings worth carrying

**Erigon diverges on the last-holder drop.** The reference drops a code's chunk
leaves when no account in the post-state holds the hash, resolved against the
whole flat state (`code_hash_survives` reads the diff's own values *and* the
untouched pre-state). Erigon keeps them unconditionally — Task 9 established the
drop branch is unreachable for a commitment batch, which cannot enumerate
holders outside itself. On-chain the two agree, because EIP-6780 deletes only
accounts created in the same transaction, whose chunks were never inserted. They
disagree on a handcrafted diff that deletes a pre-existing sole holder, which is
what the three reference tests above construct. Recorded, not reopened:
contributing the case upstream is pointless (it is already there) and erigon's
side is a known, argued deviation.

**The intra-batch delegation cases have no upstream form.** A reference
`BlockDiff` carries one post-state account per address, already merged, so
"delegate then clear in one batch" and "delegate then repoint twice in one
batch" cannot be stated against it — the model has nowhere to put the
intermediate write. They pin erigon's own update-merge path (`ModeUpdate` /
`ModeParallel`) rather than the embedding, so they stay erigon-internal.
