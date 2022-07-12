/*
   Copyright 2022 Erigon contributors
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package engineapi

import (
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/log/v3"
)

// the maximum point from the current head, past which side forks are not validated anymore.
const maxForkDepth = 32 // 32 slots is the duration of an epoch thus there cannot be side forks in PoS deeper than 32 blocks from head.

type validatePayloadFunc func(kv.RwTx, *types.Header, *types.RawBody, uint64, []*types.Header, []*types.RawBody) error

// Fork segment is a side fork segment and repressent a full side fork block.
type forkSegment struct {
	header *types.Header
	body   *types.RawBody
}

type ForkValidator struct {
	// Hash => side fork block, any block saved into this map is considered valid.
	// blocks saved are required to have at most distance maxForkDepth from the head.
	// if we miss a segment, we only accept the block and give up on full validation.
	sideForksBlock map[common.Hash]forkSegment
	// current memory batch containing chain head that extend canonical fork.
	extendingFork *memdb.MemoryMutation
	// hash of chain head that extend canonical fork.
	extendingForkHeadHash common.Hash
	// this is the function we use to perform payload validation.
	validatePayload validatePayloadFunc
	// this is the current point where we processed the chain so far.
	currentHeight uint64
}

// abs64 is a utility method that given an int64, it returns its absolute value in uint64.
func abs64(n int64) uint64 {
	if n < 0 {
		return uint64(-n)
	}
	return uint64(n)
}

func NewForkValidatorMock(currentHeight uint64) *ForkValidator {
	return &ForkValidator{
		sideForksBlock: make(map[common.Hash]forkSegment),
		currentHeight:  currentHeight,
	}
}

func NewForkValidator(currentHeight uint64, validatePayload validatePayloadFunc) *ForkValidator {
	return &ForkValidator{
		sideForksBlock:  make(map[common.Hash]forkSegment),
		validatePayload: validatePayload,
		currentHeight:   currentHeight,
	}
}

// ExtendingForkHeadHash return the fork head hash of the fork that extends the canonical chain.
func (fv *ForkValidator) ExtendingForkHeadHash() common.Hash {
	return fv.extendingForkHeadHash
}

// NotifyCurrentHeight is to be called at the end of the stage cycle and repressent the last processed block.
func (fv *ForkValidator) NotifyCurrentHeight(currentHeight uint64) {
	fv.currentHeight = currentHeight
}

// FlushExtendingFork flush the current extending fork if fcu chooses its head hash as the its forkchoice.
func (fv *ForkValidator) FlushExtendingFork(tx kv.RwTx) error {
	// Flush changes to db.
	if err := fv.extendingFork.Flush(tx); err != nil {
		return err
	}
	// Clean extending fork data
	fv.extendingFork.Rollback()
	fv.extendingForkHeadHash = common.Hash{}
	fv.extendingFork = nil
	return nil
}

// ValidatePayload returns whether a payload is valid or invalid, or if cannot be determined, it will be accepted.
// if the payload extend the canonical chain, then we stack it in extendingFork without any unwind.
// if the payload is a fork then we unwind to the point where the fork meet the canonical chain and we check if it is valid or not from there.
// if for any reasons none of the action above can be performed due to lack of information, we accept the payload and avoid validation.
func (fv *ForkValidator) ValidatePayload(tx kv.RwTx, header *types.Header, body *types.RawBody, extendCanonical bool) (status remote.EngineStatus, latestValidHash common.Hash, validationError error, criticalError error) {
	if fv.validatePayload == nil {
		status = remote.EngineStatus_ACCEPTED
		return
	}
	defer fv.clean()

	if extendCanonical {
		// If the new block extends the canonical chain we update extendingFork.
		if fv.extendingFork == nil {
			fv.extendingFork = memdb.NewMemoryBatch(tx)
		} else {
			fv.extendingFork.UpdateTxn(tx)
		}
		// Update fork head hash.
		fv.extendingForkHeadHash = header.Hash()
		// Let's assemble the side fork chain if we have others building.
		validationError = fv.validatePayload(fv.extendingFork, header, body, 0, nil, nil)
		if validationError != nil {
			status = remote.EngineStatus_INVALID
			latestValidHash = header.ParentHash
			return
		}
		status = remote.EngineStatus_VALID
		latestValidHash = header.Hash()
		fv.sideForksBlock[latestValidHash] = forkSegment{header, body}
		return
	}
	// If the block is stored within the side fork it means it was already validated.
	if _, ok := fv.sideForksBlock[header.Hash()]; ok {
		status = remote.EngineStatus_VALID
		latestValidHash = header.Hash()
		return
	}

	// if the block is not in range of maxForkDepth from head then we do not validate it.
	if abs64(int64(fv.currentHeight)-header.Number.Int64()) > maxForkDepth {
		status = remote.EngineStatus_ACCEPTED
		return
	}
	// Let's assemble the side fork backwards
	var foundCanonical bool
	currentHash := header.ParentHash
	foundCanonical, criticalError = rawdb.IsCanonicalHash(tx, currentHash)
	if criticalError != nil {
		return
	}

	var bodiesChain []*types.RawBody
	var headersChain []*types.Header
	unwindPoint := header.Number.Uint64() - 1
	for !foundCanonical {
		var sb forkSegment
		var ok bool
		if sb, ok = fv.sideForksBlock[currentHash]; !ok {
			// We miss some components so we did not check validity.
			status = remote.EngineStatus_ACCEPTED
			return
		}
		headersChain = append(headersChain, sb.header)
		bodiesChain = append(bodiesChain, sb.body)
		currentHash = sb.header.ParentHash
		foundCanonical, criticalError = rawdb.IsCanonicalHash(tx, currentHash)
		if criticalError != nil {
			return
		}
		unwindPoint = sb.header.Number.Uint64() - 1
	}
	status = remote.EngineStatus_VALID
	// if it is not canonical we validate it as a side fork.
	batch := memdb.NewMemoryBatch(tx)
	defer batch.Close()
	validationError = fv.validatePayload(batch, header, body, unwindPoint, headersChain, bodiesChain)
	latestValidHash = header.Hash()
	if validationError != nil {
		latestValidHash = header.ParentHash
		status = remote.EngineStatus_INVALID
		return
	}
	fv.sideForksBlock[header.Hash()] = forkSegment{header, body}
	return
}

// Clear wipes out current extending fork data, this method is called after fcu is called,
// because fcu decides what the head is and after the call is done all the non-chosed forks are
// to be considered obsolete.
func (fv *ForkValidator) Clear(tx kv.RwTx) {
	sb, ok := fv.sideForksBlock[fv.extendingForkHeadHash]
	// If we did not flush the fork state, then we need to notify the txpool through unwind.
	if fv.extendingFork != nil && fv.extendingForkHeadHash != (common.Hash{}) && ok {
		fv.extendingFork.UpdateTxn(tx)
		// this will call unwind of extending fork to notify txpool of reverting transactions.
		if err := fv.validatePayload(fv.extendingFork, nil, nil, sb.header.Number.Uint64()-1, nil, nil); err != nil {
			log.Warn("Could not clean payload", "err", err)
		}
		fv.extendingFork.Rollback()
	}
	// Clean all data relative to txpool
	fv.extendingForkHeadHash = common.Hash{}
	fv.extendingFork = nil
}

// clean wipes out all outdated sideforks whose distance exceed the height of the head.
func (fv *ForkValidator) clean() {
	for hash, sb := range fv.sideForksBlock {
		if abs64(int64(fv.currentHeight)-sb.header.Number.Int64()) > maxForkDepth {
			delete(fv.sideForksBlock, hash)
		}
	}
}
