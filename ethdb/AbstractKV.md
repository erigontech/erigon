## Target: 

To build 1 key-value abstraction on top of LMDB and RemoteKV (our own read-only TCP protocol for key-value databases).

## Design principles:
- No internal copies/allocations. It means app must copy keys/values before put to database.  
- Low-level API: as close to original LMDB as possible.
- Expose concept of transaction - app-level code can Begin/Commit/Rollback 
- Concept of DupSort see [/index.md] 

## Result interface:

```
// ethdb/kv_abstract.go

// KV low-level database interface - main target is - to provide common abstraction over top of LMDB and RemoteKV.
//
// Common pattern for short-living transactions:
//
//  if err := db.View(ctx, func(tx ethdb.Tx) error {
//     ... code which uses database in transaction
//  }); err != nil {
//		return err
// }
//
// Common pattern for long-living transactions:
//	tx, err := db.Begin(true)
//	if err != nil {
//		return err
//	}
//	defer tx.Rollback()
//
//	... code which uses database in transaction
//
//	err := tx.Commit()
//	if err != nil {
//		return err
//	}
//
type KV interface {
	View(ctx context.Context, f func(tx Tx) error) error
	Update(ctx context.Context, f func(tx Tx) error) error
	Close()

	// Begin - creates transaction
	// 	tx may be discarded by .Rollback() method
	//
	// A transaction and its cursors must only be used by a single
	// 	thread (not goroutine), and a thread may only have a single transaction at a time.
	//  It happen automatically by - because this method calls runtime.LockOSThread() inside (Rollback/Commit releases it)
	//  By this reason application code can't call runtime.UnlockOSThread() - it leads to undefined behavior.
	//
	// If this `parent` is non-NULL, the new transaction
	//	will be a nested transaction, with the transaction indicated by parent
	//	as its parent. Transactions may be nested to any level. A parent
	//	transaction and its cursors may not issue any other operations than
	//	Commit and Rollback while it has active child transactions.
	Begin(ctx context.Context, parent Tx, writable bool) (Tx, error)
	AllBuckets() dbutils.BucketsCfg
}

type Tx interface {
	// Cursor - creates cursor object on top of given bucket. Type of cursor - depends on bucket configuration.
	// If bucket was created with lmdb.DupSort flag, then cursor with interface CursorDupSort created
	// If bucket was created with lmdb.DupFixed flag, then cursor with interface CursorDupFixed created
	// Otherwise - object of interface Cursor created
	Cursor(bucket string) Cursor
	CursorDupSort(bucket string) CursorDupSort   // CursorDupSort - can be used if bucket has lmdb.DupSort flag
	CursorDupFixed(bucket string) CursorDupFixed // CursorDupSort - can be used if bucket has lmdb.DupFixed flag
	Get(bucket string, key []byte) (val []byte, err error)

	Commit(ctx context.Context) error // Commit all the operations of a transaction into the database.
	Rollback()                        // Rollback - abandon all the operations of the transaction instead of saving them.

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

// Cursor - class for navigating through a database
// CursorDupSort and CursorDupFixed are inherit this class
//
// If methods (like First/Next/Seek) return error, then returned key SHOULD not be nil (can be []byte{} for example).
// Then looping code will look as:
// c := kv.Cursor(bucketName)
// for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
//    if err != nil {
//        return err
//    }
//    ... logic
// }
type Cursor interface {
	Prefix(v []byte) Cursor // Prefix returns only keys with given prefix, useful RemoteKV - because filtering done by server
	Prefetch(v uint) Cursor // Prefetch enables data streaming - used only by RemoteKV

	First() ([]byte, []byte, error)           // First - position at first key/data item
	Seek(seek []byte) ([]byte, []byte, error) // Seek - position at first key greater than or equal to specified key
	SeekExact(key []byte) ([]byte, error)     // SeekExact - position at first key greater than or equal to specified key
	Next() ([]byte, []byte, error)            // Next - position at next key/value (can iterate over DupSort key/values automatically)
	Prev() ([]byte, []byte, error)            // Prev - position at previous key
	Last() ([]byte, []byte, error)            // Last - position at last key and last possible value
	Current() ([]byte, []byte, error)         // Current - return key/data at current cursor position

	Put(k, v []byte) error           // Put - based on order
	Append(k []byte, v []byte) error // Append - append the given key/data pair to the end of the database. This option allows fast bulk loading when keys are already known to be in the correct order.
	Delete(key []byte) error

	// DeleteCurrent This function deletes the key/data pair to which the cursor refers.
	// This does not invalidate the cursor, so operations such as MDB_NEXT
	// can still be used on it.
	// Both MDB_NEXT and MDB_GET_CURRENT will return the same record after
	// this operation.
	DeleteCurrent() error

	// PutNoOverwrite(key, value []byte) error
	// Reserve()

	// PutCurrent - replace the item at the current cursor position.
	//	The key parameter must still be provided, and must match it.
	//	If using sorted duplicates (#MDB_DUPSORT) the data item must still
	//	sort into the same place. This is intended to be used when the
	//	new data is the same size as the old. Otherwise it will simply
	//	perform a delete of the old record followed by an insert.
	PutCurrent(key, value []byte) error
}

type CursorDupSort interface {
	Cursor

	SeekBothExact(key, value []byte) ([]byte, []byte, error)
	SeekBothRange(key, value []byte) ([]byte, []byte, error)
	FirstDup() ([]byte, error)          // FirstDup - position at first data item of current key
	NextDup() ([]byte, []byte, error)   // NextDup - position at next data item of current key
	NextNoDup() ([]byte, []byte, error) // NextNoDup - position at first data item of next key
	LastDup() ([]byte, error)           // LastDup - position at last data item of current key

	CountDuplicates() (uint64, error)  // CountDuplicates - number of duplicates for the current key
	DeleteCurrentDuplicates() error    // DeleteCurrentDuplicates - deletes all of the data items for the current key
	AppendDup(key, value []byte) error // AppendDup - same as Append, but for sorted dup data

	//PutIfNoDup()      // Store the key-value pair only if key is not present
}

// CursorDupFixed - has methods valid for buckets with lmdb.DupFixed flag
// See also lmdb.WrapMulti
type CursorDupFixed interface {
	CursorDupSort

	// GetMulti - return up to a page of duplicate data items from current cursor position
	// After return - move cursor to prepare for #MDB_NEXT_MULTIPLE
	GetMulti() ([]byte, error)
	// NextMulti - return up to a page of duplicate data items from next cursor position
	// After return - move cursor to prepare for #MDB_NEXT_MULTIPLE
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

#### InMemory, ReadOnly, MultipleDatabases, Customization: 
- `NewLMDB().InMem().ReadOnly().Open()` 
- `NewLMDB().Path(path).WithBucketsConfig(config).Open()` 

#### Context:
- For transactions - yes
- For .First() and .Next() methods - no

#### Cursor/Iterator: 
- Cursor is an interface, can’t be nil. `db.Cursor()` can't return error
- `cursor.Prefix(prefix)` filtering keys by given prefix. RemoteKV - to support server side filtering.
- `cursor.Prefetch(1000)` - useful for Remote
- No Lazy values
- Methods .First, .Next, .Seek - can return error. 
If err!=nil then key SHOULD be !=nil (can be []byte{} for example). 
Then looping code will look as: 
```go
for k, v, err := c.First(); k != nil; k, v, err = c.Next() {
    if err != nil {
        return err
    }
    // logic
}
``` 

#### Managed/un-managed transactions
- Tx is an interface
- db.Update, db.View - yes
- db.Batch - no
- all keys and values returned by all method are valid until end of transaction
- transaction object can be used only withing 1 goroutine
- it's safe to call .Rollback() after .Commit(), multiple rollbacks are also safe. Common transaction patter: 
```
tx, err := db.Begin(true)
if err != nil {
    return err
}
defer tx.Rollback()

// ... code which uses database in transaction
 
err := tx.Commit()
if err != nil {
    return err
}
```
  
## Not covered by Abstractions:
- TTL of keys
- Nested Buckets
- Backups
