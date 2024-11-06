package ethdb

import "io"

// KeyValueReader wraps the Has and Get method of a backing data store.
type KeyValueReader interface {
	// Has retrieves if a key is present in the key-value data store.
	Has(key []byte) (bool, error)

	// Get retrieves the given key if it's present in the key-value data store.
	Get(key []byte) ([]byte, error)
}

// KeyValueWriter wraps the Put method of a backing data store.
type KeyValueWriter interface {
	// Put inserts the given value into the key-value data store.
	Put(key []byte, value []byte) error

	// Delete removes the key from the key-value data store.
	Delete(key []byte) error
}

// KeyValueStater wraps the Stat method of a backing data store.
type KeyValueStater interface {
	// Stat returns a particular internal stat of the database.
	Stat(property string) (string, error)
}

// Compacter wraps the Compact method of a backing data store.
type Compacter interface {
	// Compact flattens the underlying data store for the given key range. In essence,
	// deleted and overwritten versions are discarded, and the data is rearranged to
	// reduce the cost of operations needed to access them.
	//
	// A nil start is treated as a key before all keys in the data store; a nil limit
	// is treated as a key after all keys in the data store. If both is nil then it
	// will compact entire data store.
	Compact(start []byte, limit []byte) error
}

// KeyValueStore contains all the methods required to allow handling different
// key-value data stores backing the high level database.
type KeyValueStore interface {
	KeyValueReader
	KeyValueWriter
	KeyValueStater
	Batcher
	Iteratee
	Compacter
	Snapshotter
	io.Closer
}

// AncientReaderOp contains the methods required to read from immutable ancient data.
type AncientReaderOp interface {
	// HasAncient returns an indicator whether the specified data exists in the
	// ancient store.
	HasAncient(kind string, number uint64) (bool, error)

	// Ancient retrieves an ancient binary blob from the append-only immutable files.
	Ancient(kind string, number uint64) ([]byte, error)

	// AncientRange retrieves multiple items in sequence, starting from the index 'start'.
	// It will return
	//   - at most 'count' items,
	//   - if maxBytes is specified: at least 1 item (even if exceeding the maxByteSize),
	//     but will otherwise return as many items as fit into maxByteSize.
	//   - if maxBytes is not specified, 'count' items will be returned if they are present
	AncientRange(kind string, start, count, maxBytes uint64) ([][]byte, error)

	// Ancients returns the ancient item numbers in the ancient store.
	Ancients() (uint64, error)

	// Tail returns the number of first stored item in the freezer.
	// This number can also be interpreted as the total deleted item numbers.
	Tail() (uint64, error)

	// AncientSize returns the ancient size of the specified category.
	AncientSize(kind string) (uint64, error)
}

// AncientReader is the extended ancient reader interface including 'batched' or 'atomic' reading.
type AncientReader interface {
	AncientReaderOp

	// ReadAncients runs the given read operation while ensuring that no writes take place
	// on the underlying freezer.
	ReadAncients(fn func(AncientReaderOp) error) (err error)
}

// AncientWriter contains the methods required to write to immutable ancient data.
type AncientWriter interface {
	// ModifyAncients runs a write operation on the ancient store.
	// If the function returns an error, any changes to the underlying store are reverted.
	// The integer return value is the total size of the written data.
	ModifyAncients(func(AncientWriteOp) error) (int64, error)

	// TruncateHead discards all but the first n ancient data from the ancient store.
	// After the truncation, the latest item can be accessed it item_n-1(start from 0).
	TruncateHead(n uint64) (uint64, error)

	// TruncateTail discards the first n ancient data from the ancient store. The already
	// deleted items are ignored. After the truncation, the earliest item can be accessed
	// is item_n(start from 0). The deleted items may not be removed from the ancient store
	// immediately, but only when the accumulated deleted data reach the threshold then
	// will be removed all together.
	TruncateTail(n uint64) (uint64, error)

	// Sync flushes all in-memory ancient store data to disk.
	Sync() error

	// MigrateTable processes and migrates entries of a given table to a new format.
	// The second argument is a function that takes a raw entry and returns it
	// in the newest format.
	MigrateTable(string, func([]byte) ([]byte, error)) error
}

// AncientWriteOp is given to the function argument of ModifyAncients.
type AncientWriteOp interface {
	// Append adds an RLP-encoded item.
	Append(kind string, number uint64, item interface{}) error

	// AppendRaw adds an item without RLP-encoding it.
	AppendRaw(kind string, number uint64, item []byte) error
}

// AncientStater wraps the Stat method of a backing data store.
type AncientStater interface {
	// AncientDatadir returns the path of root ancient directory. Empty string
	// will be returned if ancient store is not enabled at all. The returned
	// path can be used to construct the path of other freezers.
	AncientDatadir() (string, error)
}

// Reader contains the methods required to read data from both key-value as well as
// immutable ancient data.
type Reader interface {
	KeyValueReader
	AncientReader
}

// Writer contains the methods required to write data to both key-value as well as
// immutable ancient data.
type Writer interface {
	KeyValueWriter
	AncientWriter
}

// Stater contains the methods required to retrieve states from both key-value as well as
// immutable ancient data.
type Stater interface {
	KeyValueStater
	AncientStater
}

// AncientStore contains all the methods required to allow handling different
// ancient data stores backing immutable chain data store.
type AncientStore interface {
	AncientReader
	AncientWriter
	io.Closer
}

type WasmTarget string
type WasmDataBaseRetriever interface {
	WasmDataBase() (KeyValueStore, uint32)
	WasmTargets() []WasmTarget
}

// Database contains all the methods required by the high level database to not
// only access the key-value data store but also the chain freezer.
type Database interface {
	Reader
	Writer
	Batcher
	Iteratee
	Stater
	Compacter
	Snapshotter
	io.Closer
	WasmDataBaseRetriever
}

// IdealBatchSize defines the size of the data batches should ideally add in one
// write.
const IdealBatchSize = 100 * 1024

// Batch is a write-only database that commits changes to its host database
// when Write is called. A batch cannot be used concurrently.
type Batch interface {
	KeyValueWriter

	// ValueSize retrieves the amount of data queued up for writing.
	ValueSize() int

	// Write flushes any accumulated data to disk.
	Write() error

	// Reset resets the batch for reuse.
	Reset()

	// Replay replays the batch contents.
	Replay(w KeyValueWriter) error
}

// Batcher wraps the NewBatch method of a backing data store.
type Batcher interface {
	// NewBatch creates a write-only database that buffers changes to its host db
	// until a final write is called.
	NewBatch() Batch

	// NewBatchWithSize creates a write-only database batch with pre-allocated buffer.
	NewBatchWithSize(size int) Batch
}

// Iterator iterates over a database's key/value pairs in ascending key order.
//
// When it encounters an error any seek will return false and will yield no key/
// value pairs. The error can be queried by calling the Error method. Calling
// Release is still necessary.
//
// An iterator must be released after use, but it is not necessary to read an
// iterator until exhaustion. An iterator is not safe for concurrent use, but it
// is safe to use multiple iterators concurrently.
type Iterator interface {
	// Next moves the iterator to the next key/value pair. It returns whether the
	// iterator is exhausted.
	Next() bool

	// Error returns any accumulated error. Exhausting all the key/value pairs
	// is not considered to be an error.
	Error() error

	// Key returns the key of the current key/value pair, or nil if done. The caller
	// should not modify the contents of the returned slice, and its contents may
	// change on the next call to Next.
	Key() []byte

	// Value returns the value of the current key/value pair, or nil if done. The
	// caller should not modify the contents of the returned slice, and its contents
	// may change on the next call to Next.
	Value() []byte

	// Release releases associated resources. Release should always succeed and can
	// be called multiple times without causing error.
	Release()
}

// Iteratee wraps the NewIterator methods of a backing data store.
type Iteratee interface {
	// NewIterator creates a binary-alphabetical iterator over a subset
	// of database content with a particular key prefix, starting at a particular
	// initial key (or after, if it does not exist).
	//
	// Note: This method assumes that the prefix is NOT part of the start, so there's
	// no need for the caller to prepend the prefix to the start
	NewIterator(prefix []byte, start []byte) Iterator
}

type Snapshot interface {
	// Has retrieves if a key is present in the snapshot backing by a key-value
	// data store.
	Has(key []byte) (bool, error)

	// Get retrieves the given key if it's present in the snapshot backing by
	// key-value data store.
	Get(key []byte) ([]byte, error)

	// Release releases associated resources. Release should always succeed and can
	// be called multiple times without causing error.
	Release()
}

// Snapshotter wraps the Snapshot method of a backing data store.
type Snapshotter interface {
	// NewSnapshot creates a database snapshot based on the current state.
	// The created snapshot will not be affected by all following mutations
	// happened on the database.
	// Note don't forget to release the snapshot once it's used up, otherwise
	// the stale data will never be cleaned up by the underlying compactor.
	NewSnapshot() (Snapshot, error)
}
