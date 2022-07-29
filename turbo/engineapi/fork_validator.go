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
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/changeset"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/shards"
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

func (fv *ForkValidator) rewindAccumulator(to uint64, accumulator *shards.Accumulator, c shards.StateChangeConsumer) error {
	hash, err := rawdb.ReadCanonicalHash(fv.extendingFork, to)
	if err != nil {
		return fmt.Errorf("read canonical hash of unwind point: %w", err)
	}
	header := rawdb.ReadHeader(fv.extendingFork, hash, to)
	if header == nil {
		return fmt.Errorf("could not find header for block: %d", to)
	}

	txs, err := rawdb.RawTransactionsRange(fv.extendingFork, to, to+1)
	if err != nil {
		return err
	}
	// Start the changes
	accumulator.Reset(0)
	accumulator.StartChange(to, hash, txs, true)
	accChangesCursor, err := fv.extendingFork.CursorDupSort(kv.AccountChangeSet)
	if err != nil {
		return err
	}
	defer accChangesCursor.Close()

	storageChangesCursor, err := fv.extendingFork.CursorDupSort(kv.StorageChangeSet)
	if err != nil {
		return err
	}
	defer storageChangesCursor.Close()

	startingKey := dbutils.EncodeBlockNumber(to)
	// Unwind notifications on accounts
	for k, v, err := accChangesCursor.Seek(startingKey); k != nil; k, v, err = accChangesCursor.Next() {
		if err != nil {
			return err
		}
		_, dbKey, dbValue, err := changeset.FromDBFormat(k, v)
		if err != nil {
			return err
		}
		if len(dbValue) > 0 {
			var acc accounts.Account
			if err := acc.DecodeForStorage(dbValue); err != nil {
				return err
			}
			// Fetch the code hash
			var address common.Address
			copy(address[:], dbKey)
			if acc.Incarnation > 0 && acc.IsEmptyCodeHash() {
				if codeHash, err2 := fv.extendingFork.GetOne(kv.PlainContractCode, dbutils.PlainGenerateStoragePrefix(address[:], acc.Incarnation)); err2 == nil {
					copy(acc.CodeHash[:], codeHash)
				}
			}

			newV := make([]byte, acc.EncodingLengthForStorage())
			acc.EncodeForStorage(newV)
			accumulator.ChangeAccount(address, acc.Incarnation, newV)
		} else {
			var address common.Address
			copy(address[:], dbKey)
			accumulator.DeleteAccount(address)
		}
	}
	// Unwind notifications on storage
	for k, v, err := storageChangesCursor.Seek(startingKey); k != nil; k, v, err = accChangesCursor.Next() {
		if err != nil {
			return err
		}
		_, dbKey, dbValue, err := changeset.FromDBFormat(k, v)
		if err != nil {
			return err
		}
		var address common.Address
		var incarnation uint64
		var location common.Hash
		copy(address[:], dbKey[:length.Addr])
		incarnation = binary.BigEndian.Uint64(dbKey[length.Addr:])
		copy(location[:], dbKey[length.Addr+length.Incarnation:])
		accumulator.ChangeStorage(address, incarnation, location, common.CopyBytes(dbValue))
	}
	accumulator.SendAndReset(context.Background(), c, header.BaseFee.Uint64(), header.GasLimit)
	log.Info("Transaction pool notified of discard side fork.")
	return nil
}

// NotifyCurrentHeight is to be called at the end of the stage cycle and repressent the last processed block.
func (fv *ForkValidator) NotifyCurrentHeight(currentHeight uint64) {
	fv.currentHeight = currentHeight
	// If the head changed,e previous assumptions on head are incorrect now.
	if fv.extendingFork != nil {
		fv.extendingFork.Rollback()
	}
	fv.extendingFork = nil
	fv.extendingForkHeadHash = common.Hash{}
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
		return fv.validateAndStorePayload(fv.extendingFork, header, body, 0, nil, nil)
	}
	// If the block is stored within the side fork it means it was already validated.
	if _, ok := fv.sideForksBlock[header.Hash()]; ok {
		status = remote.EngineStatus_VALID
		latestValidHash = header.Hash()
		return
	}

	// if the block is not in range of maxForkDepth from head then we do not validate it.
	if math.AbsoluteDifference(fv.currentHeight, header.Number.Uint64()) > maxForkDepth {
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
	// Do not set an unwind point if we are already there.
	if unwindPoint == fv.currentHeight {
		unwindPoint = 0
	}
	// Use the current tx to simulate validation and then Reset it, we are not using MemoryMutation due to its instability with deletions.
	// TODO(Giulio2002): fix memory mutation.
	status, latestValidHash, validationError, criticalError = fv.validateAndStorePayload(tx, header, body, unwindPoint, headersChain, bodiesChain)
	if criticalError != nil {
		return
	}

	if criticalError = tx.Reset(); criticalError != nil {
		return
	}
	// We do this, in case we previously saved blocks and deleted them.
	for i := len(headersChain) - 1; i >= 0; i-- {
		var parentTd *big.Int
		currentHeader := headersChain[i]
		parentTd, criticalError = rawdb.ReadTd(tx, currentHeader.ParentHash, currentHeader.Number.Uint64()-1)
		if criticalError != nil || parentTd == nil {
			criticalError = fmt.Errorf("parent's total difficulty not found with error: %v", criticalError)
			return
		}
		td := new(big.Int).Add(parentTd, currentHeader.Difficulty)
		if criticalError = rawdb.WriteTd(tx, currentHeader.Hash(), currentHeader.Number.Uint64(), td); criticalError != nil {
			return
		}
		rawdb.WriteHeader(tx, currentHeader)
	}
	return
}

// Clear wipes out current extending fork data, this method is called after fcu is called,
// because fcu decides what the head is and after the call is done all the non-chosed forks are
// to be considered obsolete.
func (fv *ForkValidator) Clear() {
	if fv.extendingFork != nil {
		fv.extendingFork.Rollback()
	}
	fv.extendingForkHeadHash = common.Hash{}
	fv.extendingFork = nil
}

// Clear wipes out current extending fork data and notify txpool.
func (fv *ForkValidator) ClearWithUnwind(tx kv.RwTx, accumulator *shards.Accumulator, c shards.StateChangeConsumer) {
	sb, ok := fv.sideForksBlock[fv.extendingForkHeadHash]
	// If we did not flush the fork state, then we need to notify the txpool through unwind.
	if fv.extendingFork != nil && accumulator != nil && fv.extendingForkHeadHash != (common.Hash{}) && ok {
		fv.extendingFork.UpdateTxn(tx)
		// this will call unwind of extending fork to notify txpool of reverting transactions.
		if err := fv.rewindAccumulator(sb.header.Number.Uint64()-1, accumulator, c); err != nil {
			log.Warn("could not notify txpool of invalid side fork", "err", err)
		}
		fv.extendingFork.Rollback()
	}
	fv.Clear()
}

// validateAndStorePayload validate and store a payload fork chain if such chain results valid.
func (fv *ForkValidator) validateAndStorePayload(tx kv.RwTx, header *types.Header, body *types.RawBody, unwindPoint uint64, headersChain []*types.Header, bodiesChain []*types.RawBody) (status remote.EngineStatus, latestValidHash common.Hash, validationError error, criticalError error) {
	validationError = fv.validatePayload(tx, header, body, unwindPoint, headersChain, bodiesChain)
	latestValidHash = header.Hash()
	if validationError != nil {
		latestValidHash = header.ParentHash
		status = remote.EngineStatus_INVALID
		if fv.extendingFork != nil {
			fv.extendingFork.Rollback()
			fv.extendingFork = nil
		}
		fv.extendingForkHeadHash = common.Hash{}
		return
	}
	// If we do not have the body we can recover it from the batch.
	if body == nil {
		var bodyWithTxs *types.Body
		bodyWithTxs, criticalError = rawdb.ReadBodyWithTransactions(tx, header.Hash(), header.Number.Uint64())
		if criticalError != nil {
			return
		}
		var encodedTxs [][]byte
		buf := bytes.NewBuffer(nil)
		for _, tx := range bodyWithTxs.Transactions {
			buf.Reset()
			if criticalError = rlp.Encode(buf, tx); criticalError != nil {
				return
			}
			encodedTxs = append(encodedTxs, common.CopyBytes(buf.Bytes()))
		}
		fv.sideForksBlock[header.Hash()] = forkSegment{header, &types.RawBody{
			Transactions: encodedTxs,
		}}
	} else {
		fv.sideForksBlock[header.Hash()] = forkSegment{header, body}
	}
	status = remote.EngineStatus_VALID
	return
}

// clean wipes out all outdated sideforks whose distance exceed the height of the head.
func (fv *ForkValidator) clean() {
	for hash, sb := range fv.sideForksBlock {
		if math.AbsoluteDifference(fv.currentHeight, sb.header.Number.Uint64()) > maxForkDepth {
			delete(fv.sideForksBlock, hash)
		}
	}
}
