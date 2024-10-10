package verkletrie

import (
	"context"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/turbo/trie/vtree"
)

type regeneratePedersenAccountsJob struct {
	address  libcommon.Address
	account  accounts.Account
	codeSize uint64
}

type regeneratePedersenAccountsOut struct {
	versionHash libcommon.Hash
	address     libcommon.Address
	account     accounts.Account
	codeSize    uint64
}

type regeneratePedersenStorageJob struct {
	storageVerkleKey libcommon.Hash
	storageKey       *uint256.Int
	storageValue     []byte
	address          libcommon.Address
}

type regeneratePedersenCodeJob struct {
	address libcommon.Address
	code    []byte
}

type regeneratePedersenCodeOut struct {
	chunks     [][]byte
	address    libcommon.Address
	chunksKeys [][]byte
	codeSize   int
}

type regenerateIncrementalPedersenAccountsJob struct {
	// Update
	address       libcommon.Address
	account       accounts.Account
	code          []byte // New code
	isContract    bool
	absentInState bool
}

type regenerateIncrementalPedersenAccountsOut struct {
	address       libcommon.Address
	versionHash   []byte
	account       accounts.Account
	codeSize      uint64
	codeChunks    [][]byte
	codeKeys      [][]byte
	isContract    bool
	absentInState bool
	badKeys       [][]byte
}

const batchSize = 10000

func pedersenAccountWorker(ctx context.Context, logPrefix string, in chan *regeneratePedersenAccountsJob, out chan *regeneratePedersenAccountsOut) {
	var job *regeneratePedersenAccountsJob
	var ok bool
	for {
		select {
		case job, ok = <-in:
			if !ok {
				return
			}
			if job == nil {
				return
			}
		case <-ctx.Done():
			return
		}

		// prevent sending to close channel
		out <- &regeneratePedersenAccountsOut{
			versionHash: libcommon.BytesToHash(vtree.GetTreeKeyVersion(job.address[:])),
			account:     job.account,
			address:     job.address,
			codeSize:    job.codeSize,
		}
	}
}

func pedersenStorageWorker(ctx context.Context, logPrefix string, in, out chan *regeneratePedersenStorageJob) {
	var job *regeneratePedersenStorageJob
	var ok bool
	for {
		select {
		case job, ok = <-in:
			if !ok {
				return
			}
			if job == nil {
				return
			}
		case <-ctx.Done():
			return
		}
		out <- &regeneratePedersenStorageJob{
			storageVerkleKey: libcommon.BytesToHash(vtree.GetTreeKeyStorageSlot(job.address[:], job.storageKey)),
			storageKey:       job.storageKey,
			address:          job.address,
			storageValue:     job.storageValue,
		}
	}
}

func pedersenCodeWorker(ctx context.Context, logPrefix string, in chan *regeneratePedersenCodeJob, out chan *regeneratePedersenCodeOut) {
	var job *regeneratePedersenCodeJob
	var ok bool
	for {
		select {
		case job, ok = <-in:
			if !ok {
				return
			}
			if job == nil {
				return
			}
		case <-ctx.Done():
			return
		}

		var chunks [][]byte
		var chunkKeys [][]byte
		if job.code == nil || len(job.code) == 0 {
			out <- &regeneratePedersenCodeOut{
				chunks:     chunks,
				chunksKeys: chunkKeys,
				codeSize:   0,
				address:    job.address,
			}
		}
		// Chunkify contract code and build keys for each chunks and insert them in the tree
		chunkedCode := vtree.ChunkifyCode(job.code)
		offset := byte(0)
		offsetOverflow := false
		currentKey := vtree.GetTreeKeyCodeChunk(job.address[:], uint256.NewInt(0))
		// Write code chunks
		for i := 0; i < len(chunkedCode); i += 32 {
			chunks = append(chunks, libcommon.CopyBytes(chunkedCode[i:i+32]))
			if currentKey[31]+offset < currentKey[31] || offsetOverflow {
				currentKey = vtree.GetTreeKeyCodeChunk(job.address[:], uint256.NewInt(uint64(i)/32))
				chunkKeys = append(chunkKeys, libcommon.CopyBytes(currentKey))
				offset = 1
				offsetOverflow = false
			} else {
				codeKey := libcommon.CopyBytes(currentKey)
				codeKey[31] += offset
				chunkKeys = append(chunkKeys, libcommon.CopyBytes(codeKey))
				offset += 1
				// If offset overflows, handle it.
				offsetOverflow = offset == 0
			}
		}
		out <- &regeneratePedersenCodeOut{
			chunks:     chunks,
			chunksKeys: chunkKeys,
			codeSize:   len(job.code),
			address:    job.address,
		}
	}
}

func incrementalAccountWorker(ctx context.Context, logPrefix string, in chan *regenerateIncrementalPedersenAccountsJob, out chan *regenerateIncrementalPedersenAccountsOut) {
	var job *regenerateIncrementalPedersenAccountsJob
	var ok bool
	for {
		select {
		case job, ok = <-in:
			if !ok {
				return
			}
			if job == nil {
				return
			}
		case <-ctx.Done():
			return
		}
		versionKey := libcommon.BytesToHash(vtree.GetTreeKeyVersion(job.address[:]))
		if job.absentInState {
			out <- &regenerateIncrementalPedersenAccountsOut{
				versionHash:   versionKey[:],
				isContract:    job.isContract,
				absentInState: job.absentInState,
			}
			continue
		}

		var chunks [][]byte
		var chunkKeys [][]byte
		// Chunkify contract code and build keys for each chunks and insert them in the tree
		chunkedCode := vtree.ChunkifyCode(job.code)
		offset := byte(0)
		offsetOverflow := false
		currentKey := vtree.GetTreeKeyCodeChunk(job.address[:], uint256.NewInt(0))
		// Write code chunks
		for i := 0; i < len(chunkedCode); i += 32 {
			chunks = append(chunks, libcommon.CopyBytes(chunkedCode[i:i+32]))
			codeKey := libcommon.CopyBytes(currentKey)
			if currentKey[31]+offset < currentKey[31] || offsetOverflow {
				currentKey = vtree.GetTreeKeyCodeChunk(job.address[:], uint256.NewInt(uint64(i)/32))
				chunkKeys = append(chunkKeys, codeKey)
				offset = 1
				offsetOverflow = false
			} else {
				codeKey[31] += offset
				chunkKeys = append(chunkKeys, codeKey)
				offset += 1
				// If offset overflows, handle it.
				offsetOverflow = offset == 0
			}
		}
		out <- &regenerateIncrementalPedersenAccountsOut{
			versionHash:   versionKey[:],
			account:       job.account,
			codeSize:      uint64(len(job.code)),
			codeChunks:    chunks,
			codeKeys:      chunkKeys,
			absentInState: job.absentInState,
			isContract:    job.isContract,
			address:       job.address,
		}
	}
}
