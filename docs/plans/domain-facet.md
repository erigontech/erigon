```text
│
└─ A genuinely new PROJECTION of (key, ts, value), needed by several
   entities, requiring its own file class + merge/prune rules?
   → new FACET (new column). Expect this ~never.
```

"How many facets do I need?" — three, and the count is essentially frozen. Here's the argument for why: facets are
projections of a 3-column relation, and there are only a handful of useful ones. The candidates people usually propose
all fall out:

- "ts → keys changed in [t1,t2)" (changesets) — that's HistoryRange, already served by the History facet because .v
  files are ts-ordered. Composition, not a new column.
- "exists (key)?" — existence filters: accessor on Latest.
- "hash → blockNum" — accessor on Headers' Latest.
- append-only values by number (Headers, Caplin's beacon blocks) — degenerate Latest where key = ts, not a new facet.

This asymmetry is exactly what makes the design scale in the direction you actually grow: entities (rows) grow with
product features — new receipt flavors, block entities, future BAL-related streams — costing one const + one Cfg row
each. Facets (columns) grow with query algebra, which is bounded and stable. Compare with today, where adding
LogTopic-style data required a whole parallel type (InvertedIdx) with its own enum, its own RoTx array, its own *
Progress method.

★ Insight ─────────────────────────────────────

- The entity/accessor/facet distinction is the same one B-tree databases settled on decades ago: table (entity) / index
  (accessor) / access method (facet). Postgres has had the same ~5 access methods (heap, btree, hash, GIN, GiST) for 25
  years while tables and indexes multiplied without bound. If your "new facet" wouldn't justify its own
  merge+prune+visibility implementation, it was an accessor or an entity all along.                             
  ─────────────────────────────────────────────────

-------

Are there existing databases with a similar design? (domain+facets)

Yes — this pattern recurs under different names.

The closest matches, and what each one validates about the design:

```text
┌───────────────────┬───────────┬───────────────────────────────────────┬────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ System │ "Entity"  │               "Facets"                │ What it validates │
├───────────────────┼───────────┼───────────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ Lucene / │ │ postings (≈II), doc-values (≈Latest │ The facet mask: Lucene's FieldType flags (indexed?, docValues?,
stored?) are literally Cfg[e].facets — per-field choice of which projections to │ │ Elasticsearch │ field │ columnar),
stored fields, points │ materialize from one document stream. Its immutable segments + background merge + refcounted
per-segment readers are also uncannily close to │ │ │ │ │ visibleFiles/RoTx. │
├───────────────────┼───────────┼───────────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ Datomic │ attribute │ EAVT / AEVT / AVET / VAET indexes │ The frozen column count: one append-only log of (entity,
attr, value, tx) facts, projected into exactly four fixed covering indexes — fixed for │ │ │ │ │ 15+ years while schemas
grew unbounded. Also ships db.asOf (t) — your GetAsOf as a first-class API. │
├───────────────────┼───────────┼───────────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ ClickHouse │ table │ MergeTree projections (alt sort │ The facet vs accessor distinction: a PROJECTION gets its own
immutable parts maintained through insert/merge (facet — own lifecycle); a skip-index │ │ │ │ orders, own parts) vs
skip-indexes │ is derived metadata inside an existing part (accessor). Same fork as .v files vs .kvi. │
├───────────────────┼───────────┼───────────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ InfluxDB │ series │ TSM ((key, time) → value ≈ History) + │ Two fixed file classes with independent compaction over
one write stream — facet planes with per-facet merge rules. │ │ │ │ TSI (tag → series ≈ II)              │ │
├───────────────────┼───────────┼───────────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ Event sourcing / │ stream │ read-model projections │ The founding principle: the update stream is the truth; every
queryable structure is a derivable, rebuildable projection. Your "facets carry zero │ │ CQRS │ │ │ new information" rule
is this, verbatim. │
└───────────────────┴───────────┴───────────────────────────────────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

None of them call it "entity + facet", but the architecture — one identity axis that grows with the product, one small
frozen set of projection kinds, a per-identity mask saying which projections exist — is the standard endpoint for
log-structured storage engines. Erigon would not be inventing a shape; it would be converging on one.

★ Insight ─────────────────────────────────────

- The strongest single precedent is Lucene, because it also solved your other problem the same way: an IndexReader
  doesn't allocate an object per field — it holds per-facet planes (terms, docValues, storedFields) that you address by
  field name/number. Field = row index, structure = column. Twenty years of production says the addressing model holds
  up. ─────────────────────────────────────────────────

------- 

OMG! `EAVT` is it an append-only stream of facts - which can be Specified and Distributed over Blockchain clients
(immutable append-only files). And all other files will be a "Derivable, Rebuildable projection"??

----

.kv must auto-split to 64mb shards. It will reduce write-amplification of Merge (can merge limited amount of shards at a
time). bloom-filter can be per-shard.

The logical stream is a table with three columns:

(key, txNum, value)

Erigon stores it decomposed:

- .ef = the (key, {txNum}) columns — keys stored once, txNum lists Elias-Fano-compressed (near the information-theoretic
  minimum for monotone sequences)
- .v = the value column, in enumeration order
- .kv = is also part of stream. `.kv` storing Latest Values which do not exist in `.v`

Important notice:

- .kv - immutable. new files shadow old (by keys). merge (like in LSM)
- .ev and .v - immutable. no-shadow. Concatenation - not really a `merge`
- mutable part stored in `mdbx` (can be any db, or in-mem)

What we don't have:

- .kv files are not sharded and not equal (Bloatnet produced huge files)
- .kv files don't have `LSM-like compaction` for reproducibility of files and for content-addressable-hashes-stability
- .kv files are sharded by `txNum` - maybe need shard by `gas` for better equality (Bloatnet produced huge
  transactions - which required manually reconfiguring Erigon to throw data from DB to files earlier). Maybe it means that
  Ethereum `FullImmutabilityThreshold` must be measured in `Gas` instead of Blocks/Slots
-

## !Alex's IDEA!

Add LatestValues into .ef file. So format: `(key, latestValue, txNums)`.

- Solves next problems:
    - Problem 1: Can have `LSM-like compaction` as we don't need content-addressable-hashes-stability for `.kv` files.
      "no-compaction" - it's not only about "size" - it's also an "attack vector"
    - Problem 2: It's safe to remove old .v + .ef files. It's safe to remove recent .kv + .v + .ef files. But .kv merge
      is unbounded means - maybe we will have only 1 .kv file to store all 8 years of Ethereum - and nothing to remove
      then (for example: if recent data has bug and need re-create it).
    - Problem 3: old `.kv` are big (`>> RAM`) and cold. And data in `.kv` colocated by "update_time" (old files store
      keys which was updated that time) + "alphabet" (inside file). Theoretically `.kv` can be lazy-built driven by
      `read-amplification` (or AI).
    - Problem 4: "Latest State" is most sensitive part of any Erigon Client - agree on common format here will be
      impossible. Removing "Latest State" from picture - make it derivable from History. Increasing the "State Spec" project's chance of success.
    - Problem 5: (AI suggested) "How to verify files?" - Chunk N's latestValue (K) must equal chunk N+1's first
      pre-value of K in .v. The duplication you're paying for is a cross-chunk consistency check a verifier gets for
      free — and it chains with BAL post-values per block.

- Cons:
    - duplication values in .ef - but we already have duplication-keys in .ef and duplication values in .kv (no
      compaction)
    - .ef getting bigger: can create new `.lv` file (stands for "latest values") LatestValues there from .ef file -
      preserve Order of .ef file - it will allow us keep .ef small

