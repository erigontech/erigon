Indices implementation in Turbo-Geth
====================================

Indices (inverted indices) - allow search data by multiple filters. 
Here is an example: "In which blocks account X was updated? (account can be created/updated/deleted)"
2 types of data "accounts value" and "accounts history" need to store in 1 key-value database.
To avoid keys collision between data types - used `account` and `history` prefixes.
To encode `created/updated/deleted` operations - used `C`, `U`, `D` markers. 

```
// Picture 1  
----------------------------------------------------
       key                      |       value
----------------------------------------------------
'account'{account1_address}     | {account1_value}
'account'{account2_address}     | {account2_value}
...                             | ...
'account'{accountN_address}     | {accountN_value}
'history'{account1_address}'C'  | {block_number1}
'history'{account1_address}'U'  | {block_number2}
'history'{account1_address}'U'  | {block_number3}
'history'{account1_address}'D'  | {block_number4}
'history'{account2_address}'C'  | {block_number5}
'history'{account2_address}'U'  | {block_number6}
...                             | ...
'history'{accountN_address}'U'  | {block_numberM}
```

**Observation 1**: `account` and `history` prefixes repeated over and over again - wasting disk space.
Complete solutions is: database supports "named buckets" - independent sub-databases - between buckets collisions are impossible.

```
// Picture 2
--------------------------------------------------------------
   bucket    |        key                |       value
--------------------------------------------------------------
  'account'  |
             | {account1_address}        | {account1_value}
             | {account2_address}        | {account2_value}
             | ...                       | ...
             | {accountN_address}        | {accountN_value}
  'history'  | 
             | {account1_address}'C'     | {block_number1}
             | {account1_address}'U'     | {block_number2}
             | {account1_address}'U'     | {block_number3}
             | {account1_address}'D'     | {block_number4}
             | {account2_address}'C'     | {block_number5}
             | {account2_address}'U'     | {block_number6}
             | ...                       | ...
             | {accountN_address}'U'     | {block_numberM}
```
Most of key-value databases (LevelDB, BadgerDB) do not provide such feature, but some do (BoltDB, LMDB)

**Observation 2**: Bucket 'history' again has much repeated prefixes: `{account1_address}` prefix will repeat every time account1 changed
This is same problem as in "Observation 1" - can we use same solution for the same problem?
Database supports "named sub-buckets" - independent sub-sub-databases - between sub-buckets collisions are impossible.

```
// Picture 3
---------------------------------------------------------------------------
    bucket   |   sub-bucket-name  |        key          |       value
---------------------------------------------------------------------------
  'account'  |
             | {account1_address} |                     | {account1_value}
             | {account2_address} |                     | {account2_value}
             | ...                |                     | ...
             | {accountN_address} |                     | {accountN_value}
  'history'  | 
             | {account1_address} |
             |                    | 'C'                 | {block_number1}
             |                    | 'U'                 | {block_number2}
             |                    | 'U'                 | {block_number3}
             |                    | 'D'                 | {block_number4}
             | {account2_address} |
             |                    | 'C'                 | {block_number5}
             |                    | 'U'                 | {block_number6}
             |                    | ...                 | ...
             | {accountn_address} |                
             |                    | 'U'                 | {block_numberM}
```

Keys don't have repetitive data anymore (markers 'C','U','D' can be part of sub-bucket name if need).

All this tricks must keep data accessible: search/iterate/insert operations must be easy.    

LMDB supports 
-------------
 
LMDB supports "buckets" (it uses name DBI) and supports "sub-buckets" (DupSort DBI).
```
#MDB_DUPSORT
    Duplicate keys may be used in the database. (Or, from another perspective,
    keys may have multiple data items, stored in sorted order.) By default
    keys must be unique and may have only a single data item.
``` 

LMDB stores keys in Tree(B+Tree), and keys of sub-buckets in sub-Tree (which is linked to Tree of bucket).

Find value of 1 key, still can be done by single method:  
```
subBucketName, keyInSubBucket, value := cursor.Get(subBucketName, keyInSubBucket)
```

Common pattern to iterate over whole 'normal' bucket (without sub-buckets) in a pseudocode:
```
cursor := transaction.OpenCursor(bucketName)
for k, v := cursor.Seek(key); k != nil; k, v = cursor.Next() {
    // logic works with 'k' and 'v' variables
} 
```

Iterate over bucket with sub-buckets: 
```
cursor := transaction.OpenCursor(bucketName)
for k, _ := cursor.SeekDup(subBucketName, keyInSubBucket); k != nil; k, _ = cursor.Next() {
    // logic works with 'k1', 'k' and 'v' variables
} 
```

Enough strait forward. No performance penalty (only profit from smaller database size).

LMDB in-depth
-------------
 
Max key size: 551byte (same for key of sub-bucket)

Please take a look on 'Picture 3' again - it illustrates the high-level idea, but LMDB stores it different way. 
'Picture 4' shows - sub-bucket (DupSort DBI) has no "value", it does join bytes of key and value and store it as 'key': 

```
// Picture 4
--------------------------------------------------------------------------------------
    bucket   | sub-bucket-name    |      keyAndValueJoinedTogether (no 'value' column)
--------------------------------------------------------------------------------------
  'account'  |
             | {account1_address} | {account1_value}   
             | {account2_address} | {account2_value}
             | ...                | ...               
             | {accountN_address} | {accountN_value}
  'history'  | 
             | {account1_address} |
             |                    | 'C'{block_number1}
             |                    | 'U'{block_number2}
             |                    | 'U'{block_number3}
             |                    | 'D'{block_number4}
             | {account2_address} |
             |                    | 'C'{block_number5}
             |                    | 'U'{block_number6}
             |                    | ...
             | {accountn_address} |                
             |                    | 'U'{block_numberM}
```

It's a bit unexpected, but doesn't change much. All operations are still work:
```
subBucketName, keyAndValueJoinedTogether := cursor.Get(subBucketName, keyInSubBucket)
```

You may need manually separate 'key' and 'value'. But, it unleash bunch of new features!

Because column "keyAndValueJoinedTogether" is sorted and stored as key in same Tree (as normal keys). 
"value" can be used as part your query. In 1 db command we can answer more complex question:
"Dear DB, Give me block number where account X was update and which is greater or equal than N".
```
{account1_address}, 'U'{block_number2} := cursor.Seek({account1_address}, 'U'{block_number1})
// notice that in parameter we used 'block_numger1' 
// but DB had no 'U' records for this block and this account
// then db returned value which is greater than what we requested 
// it returned 'block_number2' 
```

Because column "keyAndValueJoinedTogether" is stored as key - it has same size limit: 551byte 

LMDB, can you do better?
------------------------

By default, for each key LMDB does store small metadata (size of data). 
Indices by nature - store much-much keys.

If all keys in sub-bucket (DupSort DBI) have same size - LMDB can store much less metadata.  
(Remember! that "keys in sub-bucket" it's "keyAndValueJoinedTogether" - this thing must have same size).
LMDB called this feature DupFixed (can add this flag to bucket configuration).

```
#MDB_DUPFIXED
	 This flag may only be used in combination with #MDB_DUPSORT. This option
	 tells the library that the data items for this database are all the same
	 size, which allows further optimizations in storage and retrieval. When
	 all data items are the same size, the #MDB_GET_MULTIPLE, #MDB_NEXT_MULTIPLE
	 and #MDB_PREV_MULTIPLE cursor operations may be used to retrieve multiple
	 items at once.
```

It means in 1 db call you can Get/Put up to 4Kb of sub-bucket keys. 

[lmdb docs](https://github.com/ledgerwatch/lmdb-go/blob/master/lmdb/lmdb.h)

TurboGeth
---------

This article target is to show tricky concepts on simple examples. 
Real way how TurboGeth stores accounts value and accounts history is a bit different and described [here](./db_walkthrough.MD#bucket-history-of-accounts)    
 
TurboGeth supports multiple typed cursors, see [AbstractKV.md](./../../ethdb/AbstractKV.md)



