## Target: 

To build 1 key-value abstraction on top of LMDB and RemoteDB (our own read-only TCP protocol for key-value databases).

## Design principles:
- No internal copies/allocations. It means app must copy keys/values before put to database.  
Known problems: mutation.Put does copy internally. 
- Low-level API: as close to original LMDB as possible.
- Expose concept of transaction - app-level code can .Rollback() or .Commit() 

## Result interface:

```

type KV interface {
	View(ctx context.Context, f func(tx Tx) error) error
	Update(ctx context.Context, f func(tx Tx) error) error
	Close()

	Begin(ctx context.Context, parent Tx, writable bool) (Tx, error)
	AllBuckets() dbutils.BucketsCfg
}

type Tx interface {
	Cursor(bucket string) Cursor
	CursorDupSort(bucket string) CursorDupSort
	CursorDupFixed(bucket string) CursorDupFixed
	Get(bucket string, key []byte) (val []byte, err error)

	Commit(ctx context.Context) error
	Rollback()
	BucketSize(name string) (uint64, error)
}

// Interface used for buckets migration, don't use it in usual app code
type BucketMigrator interface {
	DropBucket(string) error
	CreateBucket(string) error
	ExistsBucket(string) bool
	ClearBucket(string) error
	ExistingBuckets() ([]string, error)
}

type Cursor interface {
	Prefix(v []byte) Cursor
	Prefetch(v uint) Cursor

	First() ([]byte, []byte, error)
	Seek(seek []byte) ([]byte, []byte, error)
	SeekExact(key []byte) ([]byte, error)
	Next() ([]byte, []byte, error) // Next - returns next key/value (can iterate over DupSort key/values automatically)
	Prev() ([]byte, []byte, error)
	Last() ([]byte, []byte, error)

	Put(key, value []byte) error
	// PutNoOverwrite(key, value []byte) error
	// Reserve()

	// PutCurrent - replace the item at the current cursor position.
	//	The key parameter must still be provided, and must match it.
	//	If using sorted duplicates (#MDB_DUPSORT) the data item must still
	//	sort into the same place. This is intended to be used when the
	//	new data is the same size as the old. Otherwise it will simply
	//	perform a delete of the old record followed by an insert.
	PutCurrent(key, value []byte) error
	// Current - return key/data at current cursor position
	Current() ([]byte, []byte, error)

	// DeleteCurrent This function deletes the key/data pair to which the cursor refers.
	// This does not invalidate the cursor, so operations such as MDB_NEXT
	// can still be used on it.
	// Both MDB_NEXT and MDB_GET_CURRENT will return the same record after
	// this operation.
	DeleteCurrent() error
	Delete(key []byte) error
	Append(key []byte, value []byte) error // Returns error if provided data not sorted or has duplicates
}

type CursorDupSort interface {
	Cursor

	SeekBothExact(key, value []byte) ([]byte, []byte, error)
	SeekBothRange(key, value []byte) ([]byte, []byte, error)
	FirstDup() ([]byte, error)
	NextDup() ([]byte, []byte, error)   // NextDup - iterate only over duplicates of current key
	NextNoDup() ([]byte, []byte, error) // NextNoDup - iterate with skipping all duplicates
	LastDup() ([]byte, error)

	CountDuplicates() (uint64, error)  // Count returns the number of duplicates for the current key. See mdb_cursor_count
	DeleteCurrentDuplicates() error    // Delete all of the data items for the current key
	AppendDup(key, value []byte) error // Returns error if provided data not sorted or has duplicates

	//PutIfNoDup()      // Store the key-value pair only if key is not present
}

type CursorDupFixed interface {
	CursorDupSort

	// GetMulti - return up to a page of duplicate data items from current cursor position
	// After return - move cursor to prepare for #MDB_NEXT_MULTIPLE
	// See also lmdb.WrapMulti
	GetMulti() ([]byte, error)
	// NextMulti - return up to a page of duplicate data items from next cursor position
	// After return - move cursor to prepare for #MDB_NEXT_MULTIPLE
	// See also lmdb.WrapMulti
	NextMulti() ([]byte, []byte, error)
	// PutMulti store multiple contiguous data elements in a single request.
	// Panics if len(page) is not a multiple of stride.
	// The cursor's bucket must be DupFixed and DupSort.
	PutMulti(key []byte, page []byte, stride int) error
	// ReserveMulti()
}

type HasStats interface {
	DiskSize(context.Context) (uint64, error) // db size
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
