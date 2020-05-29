## Target: 

To build 1 key-value abstraction on top of Bolt, Badger and RemoteDB (our own read-only TCP protocol for key-value databases).

## Design principles:
- No internal copies/allocations - all must be delegated to user. It means app must copy keys/values before put to database.  
Make it part of contract - written clearly in docs, because it's unsafe (unsafe to put slice to DB and then change it). 
Known problems: mutation.Put does copy internally. 
- Low-level API: as close to original Bolt/Badger as possible.
- Expose concept of transaction - app-level code can .Rollback() or .Commit() at once. 

## Result interface:

```
type KV interface {
	View(ctx context.Context, f func(tx Tx) error) (err error)
	Update(ctx context.Context, f func(tx Tx) error) (err error)
	Close() error

	Begin(ctx context.Context, writable bool) (Tx, error)
}

type Tx interface {
	Bucket(name []byte) Bucket

	Commit(ctx context.Context) error
	Rollback() error
}

type Bucket interface {
	Get(key []byte) (val []byte, err error)
	Put(key []byte, value []byte) error
	Delete(key []byte) error
	Cursor() Cursor
}

type Cursor interface {
	Prefix(v []byte) Cursor
	MatchBits(uint) Cursor
	Prefetch(v uint) Cursor
	NoValues() NoValuesCursor

	First() ([]byte, []byte, error)
	Seek(seek []byte) ([]byte, []byte, error)
	Next() ([]byte, []byte, error)
	Walk(walker func(k, v []byte) (bool, error)) error
}

type NoValuesCursor interface {
	First() ([]byte, uint32, error)
	Seek(seek []byte) ([]byte, uint32, error)
	Next() ([]byte, uint32, error)
	Walk(walker func(k []byte, vSize uint32) (bool, error)) error
}
```

## Rationale and Features list: 

#### Buckets concept:
- Bucket is an interface, can’t be nil, can't return error
- For Badger - auto-remove bucket from key prefix

#### InMemory and ReadOnly modes: 
- `NewBadger().InMem().ReadOnly().Open(ctx)` 

#### Context:
- For transactions - yes
- For .First() and .Next() methods - no

#### Cursor/Iterator: 
- Cursor is an interface, can’t be nil, can't return error
- `cursor.Prefix(prefix)` filtering keys by given prefix. Badger using i.Prefix. RemoteDb - to support server side filtering.
- `cursor.Prefetch(1000)` - useful for Badger and Remote
- Badger iterator require i.Close() call - abstraction automated it.
- Badger iterator has AllVersions=true by default - why?
- Methods .First, .Next, .Seek - can return error. If err!=nil then key SHOULD be !=nil (can be []byte{} for example). Then looping code will look as: 
```go
for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
    if err != nil {
        return err
    }
    // logic
}
``` 

#### Concept of Item:
- Badger's concept of Item adding complexity, need hide it: `k,v,err := curor.First()`
- No Lazy values, but can disable fetching values by: `.Cursor().PrefetchValues(false).FirstKey()`
- Badger's metadata, ttl and version - don’t expose

#### Managed/un-managed transactions
- Tx is an interface
- db.Update, db.View - yes
- db.Batch - no
  
#### Errors: 
- Lib-Errors must be properly wrapped to project: for example ethdb.ErrKeyNotFound

#### Badger’s streaming:
- Need more research: why it’s based on callback instead of  “for channel”? Is it ordered? Is it stoppable? 
- Is it equal to Bolt’s ForEach?

#### Yeld: abstraction leak from RemoteDb, but need investigate how Badger Streams working here
#### i.SeekTo vs i.Rewind: TBD
#### in-memory LRU cache: TBD
- Reverse Iterator

## Not covered by Abstractions:
- DB stats, bucket.Stats(), item.EstimatedSize()
- buckets stats, buckets list
- Merge operator of Badger - no
- TTL of keys
- Fetch AllVersions of Badger
- Monotonic int DB.GetSequence 
- Nested Buckets
- Backups, tx.WriteTo
