package kv

import (
	"context"
)

// forkables

type ForkablesRoTx interface {
	Marked() MarkedRoTx
	Unmarked() UnmarkedRoTx
}

type ForkablesRwTx interface {
	Marked() MarkedRwTx
	Unmarked() UnmarkedRwTx
}

type ForkableRoTxCommons interface {
	GetFromFiles(entityNum Num) (v []byte, found bool, fileIdx int, err error) // snapshot only
	Close()
	VisibleFilesMaxRootNum() RootNum
	VisibleFilesMaxNum() Num

	VisibleFiles() VisibleFiles
	GetFromFile(entityNum Num, idx int) (v []byte, found bool, err error)

	HasRootNumUpto(ctx context.Context, to RootNum) (bool, error)
	Type() CanonicityStrategy
}

// marked
type MarkedDbRoTx interface {
	GetDb(num Num, hash []byte) ([]byte, error) // db only (hash==nil => canonical value)
}

type MarkedDbRwTx interface {
	MarkedDbRoTx
	Prune(ctx context.Context, to RootNum, limit uint64) (uint64, error)
	Unwind(ctx context.Context, from RootNum) error
}

type MarkedRoTx interface {
	Get(num Num) ([]byte, error)

	Debug() ForkableRoTxCommons
	RoDbDebug() MarkedDbRoTx
}

type MarkedRwTx interface {
	MarkedRoTx
	Put(num Num, hash []byte, value []byte) error

	RwDbDebug() MarkedDbRwTx
}

// unmarked
type UnmarkedDbRoTx interface {
	GetDb(num Num) ([]byte, error)
}

type UnmarkedDbRwTx interface {
	UnmarkedDbRoTx
	Prune(ctx context.Context, to RootNum, limit uint64) (uint64, error)
	Unwind(ctx context.Context, from RootNum) error
}

type UnmarkedRoTx interface {
	Get(num Num) ([]byte, error)

	Debug() ForkableRoTxCommons
	RoDbDebug() UnmarkedDbRoTx
}

type UnmarkedRwTx interface {
	UnmarkedRoTx
	RwDbDebug() UnmarkedDbRoTx
	Append(num Num, value []byte) error
}

// buffer: TODO
