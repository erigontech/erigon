DupSort feature explanation
===========================

If KV database has no concept of "Buckets/Tables/Collections" then all keys must have "Prefix". For example to store
Block bodies and headers need use `b` and `h` prefixes:

```
b1->encoded_block1
b2->encoded_block2
b3->encoded_block3
...
h1->encoded_header1
h2->encoded_header2
h3->encoded_header3
...
```

Of course this is 1 byte per key overhead is not very big. But if DB provide concept of named "
Buckets/Tables/Collections" then need create 2 tables `b` and `h` and store there key without prefixes. Physically table
names will stored only once (not 1 per key).

But if do 1 step forward - and introduce concept of named "Sub-Buckets/Sub-Tables/Sub-Collections". Then in will allow
to store physically once longer prefixes.

Let's look at ChangeSets. If block N changed account A from value X to Y:  
`ChangeSet -> bigEndian(N) -> A -> X`

- `ChangeSet` - name of Table
- `bigEndian(N)` - name of Sub-Table
- `A` - key inside Sub-Table
- `X` - value inside Sub-Table

MDBX supports
-------------

MDBX supports "tables" (it uses name DBI) and supports "sub-tables" (DupSort DBI).

```
#MDBX_DUPSORT
    Duplicate keys may be used in the database. (Or, from another perspective,
    keys may have multiple data items, stored in sorted order.) By default
    keys must be unique and may have only a single data item.
``` 

MDBX stores keys in Tree(B+Tree), and keys of sub-tables in sub-Tree (which is linked to Tree of table).

Find value of 1 key, still can be done by single method:

```
subTableName, keyInSubTable, value := db.Get(tableName, subTableName, keyInSubTable)
```

Common pattern to iterate over whole 'normal' table (without sub-table) in a pseudocode:

```
cursor := transaction.OpenCursor(tableName)
for k, v := cursor.Seek(key); k != nil; k, v = cursor.Next() {
    // logic works with 'k' and 'v' variables
} 
```

Iterate over table with sub-table:

```
cursor := transaction.OpenCursor(tableName)
for k, _ := cursor.SeekDup(subTableName, keyInSubTable); k != nil; k, _ = cursor.Next() {
    // logic works with 'k1', 'k' and 'v' variables
} 
```

Enough straight forward. No performance penalty (only profit from smaller database size).

MDBX in-depth
-------------

Max key size: 2022byte (same for key of sub-Table)
Let's look at ChangeSets. If block N changed account A from value X to Y:  
`ChangeSet -> bigEndian(N) -> A -> X`

- `ChangeSet` - name of Table
- `bigEndian(N)` - name of Sub-Table
- `A` - key inside Sub-Table
- `X` - value inside Sub-Table

```
------------------------------------------------------------------------------------------
    table        | sub-table-name    |      keyAndValueJoinedTogether (no 'value' column)
------------------------------------------------------------------------------------------
  'ChangeSets'   | 
                 | {1}                | {A}+{X}   
                 |                    | {A2}+{X2}
                 | {2}                | {A3}+{X3}   
                 |                    | {A4}+{X4}
                 | ...                | ...               
```

It's a bit unexpected, but doesn't change much. All operations are still work:

```
subTableName, keyAndValueJoinedTogether := cursor.Get(subTableName, keyInSubTable)
{N}, {A}+{X} := cursor.Seek({N}, {A})
```

You need manually separate 'A' and 'X'. But, it unleash bunch of new features!
Can iterate in sortet manner all changes in block N. Can read only 1 exact change - even if Block changed many megabytes
of state.

And format of StorageChangeSetBucket:
Loc - location hash (key of storage)

```
------------------------------------------------------------------------------------------
    table        | sub-table-name    |      keyAndValueJoinedTogether (no 'value' column)
------------------------------------------------------------------------------------------
'StorageChanges' | 
                 | {1}+{A}+{inc1}     | {Loc1}+{X}
                 |                    | {Loc2}+{X2}
                 |                    | {Loc3}+{X3}
                 | {2}+{A}+{inc1}     | {Loc4}+{X4}
                 |                    | {Loc5}+{X5}
                 |                    | {Loc6}+{X6}
                 |                    | ...             
 ```

Because column "keyAndValueJoinedTogether" is stored as key - it has same size limit: 551byte

MDBX, can you do better?
------------------------

By default, for each key MDBX does store small metadata (size of data). Indices by nature - store much-much keys.

If all keys in sub-table (DupSort DBI) have same size - MDBX can store much less metadata.  
(Remember! that "keys in sub-table" it's "keyAndValueJoinedTogether" - this thing must have same size). MDBX called this
feature DupFixed (can add this flag to table configuration).

```
#MDB_DUPFIXED
	 This flag may only be used in combination with #MDB_DUPSORT. This option
	 tells the library that the data items for this database are all the same
	 size, which allows further optimizations in storage and retrieval. When
	 all data items are the same size, the #MDB_GET_MULTIPLE, #MDB_NEXT_MULTIPLE
	 and #MDB_PREV_MULTIPLE cursor operations may be used to retrieve multiple
	 items at once.
```

It means in 1 db call you can Get/Put up to 4Kb of sub-table keys.

[see mdbx.h](https://github.com/erigontech/libmdbx/blob/master/mdbx.h)

Erigon
---------

This article target is to show tricky concepts on examples. Future
reading [here](./db_walkthrough.MD#table-history-of-accounts)

Erigon supports multiple typed cursors, see the [KV
Readme.md](https://github.com/ledgerwatch/erigon-lib/tree/main/kv)



