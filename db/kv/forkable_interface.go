package kv

import (
	"context"
)

// forkables

type ForkableTxCommons interface {
	GetFromFiles(entityNum Num) (v []byte, found bool, fileIdx int, err error) // snapshot only
	Close()
	VisibleFilesMaxRootNum() RootNum
	VisibleFilesMaxNum() Num

	VisibleFiles() VisibleFiles
	GetFromFile(entityNum Num, idx int) (v []byte, found bool, err error)

	HasRootNumUpto(ctx context.Context, to RootNum) (bool, error)
	Progress() (Num, error)
	StepSize() uint64
}

// unmarked
type UnmarkedDbTx interface {
	GetDb(num Num) ([]byte, error)
}

type UnmarkedDbRwTx interface {
	UnmarkedDbTx
	Prune(ctx context.Context, to RootNum, limit uint64) (uint64, error)
	Unwind(ctx context.Context, from RootNum) error
}

type UnmarkedTx interface {
	Get(num Num) ([]byte, error)

	Debug() ForkableTxCommons
	RoDbDebug() UnmarkedDbTx
	BufferedWriter() BufferedWriter
}

type UnmarkedRwTx interface {
	UnmarkedTx
	RwDbDebug() UnmarkedDbRwTx
	Append(num Num, value []byte) error
}

// equivalent of TemporalPutDel
type UnmarkedPutter interface {
	Put(num Num, value []byte) error
}

// marked
type MarkedDbTx interface {
	GetDb(num Num, hash []byte) ([]byte, error) // db only (hash==nil => canonical value)
}

type MarkedDbRwTx interface {
	MarkedDbTx
	Prune(ctx context.Context, to RootNum, limit uint64) (uint64, error)
	Unwind(ctx context.Context, from RootNum) error
}

type MarkedTx interface {
	Get(num Num) ([]byte, error)

	Debug() ForkableTxCommons
	RoDbDebug() MarkedDbTx
}

type MarkedRwTx interface {
	MarkedTx
	Put(num Num, hash []byte, value []byte) error

	RwDbDebug() MarkedDbRwTx
}

//

type BufferedWriter interface {
	Put(n Num, v []byte) error
	Flush(ctx context.Context, tx RwTx) error
	Close()
}
