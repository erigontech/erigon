#### `Ethdb` package hold's bouquet of objects to access DB

Words "KV" and "DB" have special meaning here: 
- KV - key-value-style API to access data: let developer manage transactions, stateful cursors. 
- DB - object-oriented-style API to access data: Get/Put/Delete/WalkOverTable/MultiPut, managing transactions internally.

So, DB abstraction fits 95% times and leads to more maintainable code - because it looks stateless. 

About "key-value-style": Modern key-value databases don't provide Get/Put/Delete methods, 
  because it's very hard-drive-unfriendly - it pushes developers do random-disk-access which is [order of magnitude slower than sequential read](https://www.seagate.com/sg/en/tech-insights/lies-damn-lies-and-ssd-benchmark-master-ti/).
  To enforce sequential-reads - introduced stateful cursors/iterators - they intentionally look as file-api: open_cursor/seek/write_data_from_current_position/move_to_end/step_back/step_forward/delete_key_on_current_position/append.

## Class diagram: 

```asciiflow.com
// This is not call graph, just show classes from low-level to high-level. 
// And show which classes satisfy which interfaces.

                    +-----------------------------------+   +-----------------------------------+ 
                    |  github.com/torquem-ch/mdbx-go    |   | google.golang.org/grpc.ClientConn |                    
                    |  (app-agnostic MDBX go bindings)  |   | (app-agnostic RPC and streaming)  |
                    +-----------------------------------+   +-----------------------------------+
                                      |                                      |
                                      |                                      |
                                      v                                      v
                    +-----------------------------------+   +-----------------------------------+
                    |       ethdb/kv_mdbx.go            |   |       ethdb/kv_remote.go          |                
                    |  (tg-specific MDBX implementaion) |   |   (tg-specific remote DB access)  |              
                    +-----------------------------------+   +-----------------------------------+
                                      |                                      |
                                      |                                      |
                                      v                                      v
            +----------------------------------------------------------------------------------------------+
            |                                       ethdb/kv_abstract.go                                   |  
            |         (Common KV interface. DB-friendly, disk-friendly, cpu-cache-friendly.                |
            |           Same app code can work with local or remote database.                              |
            |           Allows experiment with another database implementations.                           |
            |          Supports context.Context for cancelation. Any operation can return error)           |
            +----------------------------------------------------------------------------------------------+
                 |                                        |                                      |
                 |                                        |                                      |
                 v                                        v                                      v
+-----------------------------------+   +-----------------------------------+   +-----------------------------------+
|       ethdb/object_db.go          |   |          ethdb/tx_db.go           |   |    ethdb/remote/remotedbserver    |                
|     (thread-safe, stateless,      |   | (non-thread-safe, more performant |   | (grpc server, using kv_abstract,  |  
|   opens/close short transactions  |   |   than object_db, method Begin    |   |   kv_remote call this server, 1   |
|      internally when need)        |   |  DOESN'T create new TxDb object)  |   | transaction maps on 1 grpc stream |
+-----------------------------------+   +-----------------------------------+   +-----------------------------------+
                |                                          |                                     
                |                                          |                                     
                v                                          v                                     
            +-----------------------------------------------------------------------------------------------+
            |                                    ethdb/interface.go                                         |  
            |     (Common DB interfaces. ethdb.Database and ethdb.DbWithPendingMutations are widely used)   |
            +-----------------------------------------------------------------------------------------------+
                |                      
                |                      
                v                      
+--------------------------------------------------+ 
|             ethdb/mutation.go                    |                 
| (also known as "batch", recording all writes and |  
|   them flush to DB in sorted way only when call  | 
|     .Commit(), use it to avoid random-writes.    | 
|   It use and satisfy ethdb.Database in same time |
+--------------------------------------------------+ 

```

## ethdb.AbstractKV design:

- InMemory, ReadOnly: `NewMDBX().Flags(mdbx.ReadOnly).InMem().Open()`
- MultipleDatabases, Customization: `NewMDBX().Path(path).WithBucketsConfig(config).Open()`


- 1 Transaction object can be used only within 1 goroutine.
- Only 1 write transaction can be active at a time (other will wait).
- Unlimited read transactions can be active concurrently (not blocked by write transaction).


- Methods db.Update, db.View - can be used to open and close short transaction.
- Methods Begin/Commit/Rollback - for long transaction.
- it's safe to call .Rollback() after .Commit(), multiple rollbacks are also safe. Common transaction pattern:

```
tx, err := db.Begin(true, ethdb.RW)
if err != nil {
    return err
}
defer tx.Rollback() // important to avoid transactions leak at panic or early return

// ... code which uses database in transaction
 
err := tx.Commit()
if err != nil {
    return err
}
```


- No internal copies/allocations. It means: 1. app must copy keys/values before put to database. 2. Data after read from db - valid only during current transaction - copy it if plan use data after transaction Commit/Rollback.
- Methods .Bucket() and .Cursor(), can’t return nil, can't return error.
- Bucket and Cursor - are interfaces - means different classes can satisfy it: for example `MdbxCursor` and `MdbxDupSortCursor` classes satisfy it. 
  If your are not familiar with "DupSort" concept, please read [dupsort.md](../docs/programmers_guide/dupsort.md) first.


- If Cursor returns err!=nil then key SHOULD be != nil (can be []byte{} for example). 
Then traversal code look as: 
```go
for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
    if err != nil {
        return err
    }
    // logic
}
``` 
- Move cursor: `cursor.Seek(key)`



## ethdb.Database design:

- Allows pass multiple implementations 
- Allows traversal tables by `db.Walk` 

## ethdb.TxDb design:
- holds inside 1 long-running transaction and 1 cursor per table
- method Begin DOESN'T create new TxDb object, it means this object can be passed into other objects by pointer,
  and high-level app code can start/commit transactions when it needs without re-creating all objects which holds
  TxDb pointer.
- This is the reason why txDb.CommitAndBegin() method works: inside it creating new transaction object, pinter to TxDb stays valid.

## How to dump/load table

Install all database tools: `make db-tools`

```
./build/bin/mdbx_dump -a <datadir>/erigon/chaindata | lz4 > dump.lz4
lz4 -d < dump.lz4 | ./build/bin/mdbx_load -an <datadir>/erigon/chaindata
```

## How to get table checksum

```
./build/bin/mdbx_dump -s table_name <datadir>/erigon/chaindata | tail -n +4 | sha256sum # tail here is for excluding header 

Header example:
VERSION=3
geometry=l268435456,c268435456,u25769803776,s268435456,g268435456
mapsize=756375552
maxreaders=120
format=bytevalue
database=TBL0001
type=btree
db_pagesize=4096
duplicates=1
dupsort=1
HEADER=END
```
