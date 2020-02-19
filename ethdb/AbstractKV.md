## Vision: 

Ethereum gives users a powerful resource (which is hard to give) which is not explicitely priced - 
transaction atomicity and "serialisable" isolation (the highest level of isolation you can get in the databases). 
Which means that transaction does not even need to declare in advance what it wants to lock, the entire 
state is deemed "locked" for its execution. I wonder if the weaker isolation models would make sense. 
For example, ["read committed"](https://en.wikipedia.org/wiki/Isolation_(database_systems)#Read_committed)

with the weaker isolation, you might be able to split any transaction into smaller parts, each of which 
does not perform any Dynamic State Access (I have no proof of that though).

It is similar to Tendermint strategy, but even more granular. You can view it as a support for "continuations". 
Transactions starts, and whenever it hits dynamic access, its execution stops, gas is charged, and the continuation 
is added to the state. Then, transaction can be resumed, because by committing to some continuation, it makes its 
next dynamic state access static.

## Design principles:
- No internal copies/allocations - all must be delegated to user. 
Make it part of contract - written clearly in docs, because it's unsafe (unsafe to put slice to DB and then change it). 
Known problems: mutation.Put does copy internally. 
- Low-level API: as close to original Bolt/Badger as possible.
- Expose concept of transaction - app-level code can .Rollback() or .Commit() at once. 
  


## Abstraction to support: 

#### Buckets concept:
- Bucket’s can’t be null - abstraction will create bucket automatically on app start (if need)

#### InMemory and ReadOnly modes 

#### Transactions: db.Update, db.Batch, db.View transactions

#### Context:
- For transactions - yes
- For .First() and .Next() methods - no


#### Cursor/Iterator: 
- Badger iterator require i.Close() call - abstraction can hide it, not user. 
- i.Prefix - Badger using this option to understand which disk table need to touch… i.ValidForPrefix - is for 
termination of iterator loop, not for filtering. Probably abstraction must expose i.Prefix settings - because it’s 
also useful for RemoteDb. 
- Badger iterator has AllVersions=true by default - why?
- i.PrefetchSize - expose
- For Badger - auto-remove bucket prefix from key
- Bucket and Cursor can't be nil - means they must not be pointers???

#### Concept of Item:
- i.PrefetchValues - expose, default=true. 
- No Lazy values. 
- Badger has: `.Value = func ( func(val []byte) error) error` and `ValueCopy(dst []byte) ([]byte, error)`. Such 
signature doesn't allow to have next common go pattern: 
```go
buf := make([]byte, 32)
if buf, err := it.ValueCopy(buf); err != nil {
    return err
}
```
- Abstraction will provide `ValueCopy(dst *[]byte) error` version. 
It will force user to make allocation by himself (I think it's good practice and json.Unmarshal follow this way). 
Does nothing if i.PrefetchValues = false.
- Badger metadata, ttl and version - don’t expose

#### Managed/un-managed transactions  
#### i.SeekTo vs i.Rewind: TBD
#### in-memory LRU cache: TBD
#### Badger’s streaming:
-  Need more research: why it’s based on callback instead of  “for channel”? Is it ordered? Is it stoppable? 
- Is it equal to Bolt’s ForEach?
#### Errors:
- Badger and RemoteDB can return error on any operation: .Get,.Put,.Next. Abstraction will expose this error. 
-  Lib-Errors must be properly wrapped to project: for example ethdb.ErrKeyNotFound
#### Yeld: abstraction leak from RemoteDb, but need investigate how Badger Streams working here

#### Copy of data: 
- Bolt does copy keys inside .Put(), but doesn't copy values. How behave Badger here? 

## Not covered by Abstractions:
- DB stats, bucket.Stats(), item.EstimatedSize()
- buckets stats, buckets list
- Merge operator of Badger 
- TTL of keys
- Reverse Iterator
- Fetch AllVersions of Badger
- Monotonic int DB.GetSequence 
- Nested Buckets
- Backups, tx.WriteTo