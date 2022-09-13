package main

import (
	"context"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/turbo/trie/vtree"
)

type regeneratePedersenAccountsJob struct {
	address  common.Address
	account  accounts.Account
	codeSize uint64
}

type regeneratePedersenAccountsOut struct {
	versionHash common.Hash
	address     common.Address
	account     accounts.Account
	codeSize    uint64
}

type regeneratePedersenStorageJob struct {
	storageVerkleKey common.Hash
	storageKey       *uint256.Int
	storageValue     []byte
	address          common.Address
}

type regeneratePedersenCodeJob struct {
	address common.Address
	code    []byte
}

type regeneratePedersenCodeOut struct {
	chunks     [][]byte
	address    common.Address
	chunksKeys []common.Hash
	codeSize   int
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
			versionHash: common.BytesToHash(vtree.GetTreeKeyVersion(job.address[:])),
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
			storageVerkleKey: common.BytesToHash(vtree.GetTreeKeyStorageSlot(job.address[:], job.storageKey)),
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
		var chunkKeys []common.Hash
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
			chunks = append(chunks, common.CopyBytes(chunkedCode[i:i+32]))
			if currentKey[31]+offset < currentKey[31] || offsetOverflow {
				currentKey = vtree.GetTreeKeyCodeChunk(job.address[:], uint256.NewInt(uint64(i)/32))
				chunkKeys = append(chunkKeys, common.BytesToHash(currentKey))
				offset = 1
				offsetOverflow = false
			} else {
				codeKey := common.CopyBytes(currentKey)
				codeKey[31] += offset
				chunkKeys = append(chunkKeys, common.BytesToHash(codeKey))
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
