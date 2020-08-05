## Target: 

To build 1 key-value abstraction on top of Bolt, LMDB and RemoteDB (our own read-only TCP protocol for key-value databases).

## Design principles:
- No internal copies/allocations - all must be delegated to user. It means app must copy keys/values before put to database.  
Make it part of contract - written clearly in docs, because it's unsafe (unsafe to put slice to DB and then change it). 
Known problems: mutation.Put does copy internally. 
- Low-level API: as close to original LMDB as possible.
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
	Rollback() // doesn't return err to be defer friendly
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

#### InMemory and ReadOnly modes: 
- `NewLMDB().InMem().ReadOnly().Open(ctx)` 

#### Context:
- For transactions - yes
- For .First() and .Next() methods - no

#### Cursor/Iterator: 
- Cursor is an interface, can’t be nil, can't return error
- `cursor.Prefix(prefix)` filtering keys by given prefix. RemoteDb - to support server side filtering.
- `cursor.Prefetch(1000)` - useful for Remote
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
- No Lazy values, but can disable fetching values by: `.Cursor().PrefetchValues(false).FirstKey()`

#### Managed/un-managed transactions
- Tx is an interface
- db.Update, db.View - yes
- db.Batch - no
- all keys and values returned by all method are valid until end of transaction
- transaction object can be used only withing 1 goroutine
  
#### Errors: 
- Lib-Errors must be properly wrapped to project: for example ethdb.ErrKeyNotFound

#### i.SeekTo vs i.Rewind: TBD
#### in-memory LRU cache: TBD
- Reverse Iterator

## Not covered by Abstractions:
- DB stats, bucket.Stats(), item.EstimatedSize()
- buckets stats, buckets list
- TTL of keys
- Monotonic int DB.GetSequence 
- Nested Buckets
- Backups, tx.WriteTo
