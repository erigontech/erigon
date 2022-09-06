package main

import (
	"context"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/turbo/trie/vtree"
)

type regeneratePedersenAccountsJob struct {
	address common.Address
	account accounts.Account
}

type regeneratePedersenAccountsOut struct {
	versionHash    common.Hash
	address        common.Address
	encodedAccount []byte
}

type regeneratePedersenStorageJob struct {
	storageVerkleKey common.Hash
	storageKey       *uint256.Int
	storageValue     []byte
	address          common.Address
}

type regeneratePedersenCodeJob struct {
	address      common.Address
	codeSizeHash common.Hash
	code         []byte
}

type regeneratePedersenCodeOut struct {
	chunks       [][]byte
	address      common.Address
	chunksKeys   []common.Hash
	codeSizeHash common.Hash
	codeSize     int
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

		vAcc := vtree.AccountToVerkleAccount(job.account)
		encoded := make([]byte, vAcc.GetVerkleAccountSizeForStorage())
		vAcc.EncodeVerkleAccountForStorage(encoded)
		// prevent sending to close channel
		out <- &regeneratePedersenAccountsOut{
			versionHash:    common.BytesToHash(vtree.GetTreeKeyVersion(job.address[:])),
			encodedAccount: encoded,
			address:        job.address,
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
				chunks:       chunks,
				chunksKeys:   chunkKeys,
				codeSizeHash: job.codeSizeHash,
				codeSize:     0,
				address:      job.address,
			}
		}
		// Chunkify contract code and build keys for each chunks and insert them in the tree
		chunkedCode := vtree.ChunkifyCode(job.code)
		// Write code chunks
		for i := 0; i < len(chunkedCode); i += 32 {
			chunks = append(chunks, common.CopyBytes(chunkedCode[i:i+32]))
			chunkKeys = append(chunkKeys, common.BytesToHash(vtree.GetTreeKeyCodeChunk(job.address[:], uint256.NewInt(uint64(i)/32))))
		}
		out <- &regeneratePedersenCodeOut{
			chunks:       chunks,
			chunksKeys:   chunkKeys,
			codeSizeHash: job.codeSizeHash,
			codeSize:     len(job.code),
			address:      job.address,
		}
	}
}
