// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package clique

import (
	"bytes"
	"fmt"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/types"
)

func (c *Clique) verifyHeader(chain rules.ChainHeaderReader, header *types.Header, parents []*types.Header) error {
	number := header.Number.Uint64()

	now := time.Now()
	nowUnix := now.Unix()

	if header.Time > uint64(nowUnix) {
		return rules.ErrFutureBlock
	}

	checkpoint := (number % c.config.Epoch) == 0
	if checkpoint && header.Coinbase != (common.Address{}) {
		return errInvalidCheckpointBeneficiary
	}

	if !bytes.Equal(header.Nonce[:], NonceAuthVote) && !bytes.Equal(header.Nonce[:], nonceDropVote) {
		return errInvalidVote
	}

	if checkpoint && !bytes.Equal(header.Nonce[:], nonceDropVote) {
		return errInvalidCheckpointVote
	}

	if len(header.Extra) < ExtraVanity {
		return errMissingVanity
	}
	if len(header.Extra) < ExtraVanity+ExtraSeal {
		return errMissingSignature
	}
	signersBytes := len(header.Extra) - ExtraVanity - ExtraSeal
	if !checkpoint && signersBytes != 0 {
		return errExtraSigners
	}
	if checkpoint && signersBytes%length.Addr != 0 {
		return errInvalidCheckpointSigners
	}
	if header.MixDigest != (common.Hash{}) {
		return errInvalidMixDigest
	}
	if header.UncleHash != empty.UncleHash {
		return errInvalidUncleHash
	}
	if number > 0 {
		if header.Difficulty.CmpUint64(DiffInTurn) != 0 && header.Difficulty.CmpUint64(diffNoTurn) != 0 {
			return errInvalidDifficulty
		}
	}

	if header.WithdrawalsHash != nil {
		return rules.ErrUnexpectedWithdrawals
	}

	if header.RequestsHash != nil {
		return rules.ErrUnexpectedRequests
	}

	if header.SlotNumber != nil {
		return rules.ErrUnexpectedSlotNumber
	}

	if header.BlockAccessListHash != nil {
		return rules.ErrUnexpectedBlockAccessListHash
	}

	return c.verifyCascadingFields(chain, header, parents)
}

func (c *Clique) verifyCascadingFields(chain rules.ChainHeaderReader, header *types.Header, parents []*types.Header) error {
	number := header.Number.Uint64()
	if number == 0 {
		return nil
	}

	var parent *types.Header
	if len(parents) > 0 {
		parent = parents[len(parents)-1]
	} else {
		parent = chain.GetHeader(header.ParentHash, number-1)
	}
	if parent == nil || parent.Number.Uint64() != number-1 || parent.Hash() != header.ParentHash {
		return rules.ErrUnknownAncestor
	}

	if parent.Time+c.config.Period > header.Time {
		return errInvalidTimestamp
	}
	if !chain.Config().IsLondon(header.Number.Uint64()) {
		if header.BaseFee != nil {
			return fmt.Errorf("invalid baseFee before fork: have %d, want <nil>", header.BaseFee)
		}
		if err := misc.VerifyGaslimit(parent.GasLimit, header.GasLimit); err != nil {
			return err
		}
	} else if err := misc.VerifyEip1559Header(chain.Config(), parent, header); err != nil {
		return err
	}

	if err := misc.VerifyAbsenceOfCancunHeaderFields(header); err != nil {
		return err
	}

	snap, err := c.Snapshot(chain, number-1, header.ParentHash, parents)
	if err != nil {
		return err
	}

	if number%c.config.Epoch == 0 {
		signers := make([]byte, len(snap.Signers)*length.Addr)
		for i, signer := range snap.GetSigners() {
			copy(signers[i*length.Addr:], signer[:])
		}

		extraSuffix := len(header.Extra) - ExtraSeal
		if !bytes.Equal(header.Extra[ExtraVanity:extraSuffix], signers) {
			return errMismatchingCheckpointSigners
		}
	}

	return c.verifySeal(chain, header, snap)
}

func (c *Clique) Snapshot(chain rules.ChainHeaderReader, number uint64, hash common.Hash, parents []*types.Header) (*Snapshot, error) {
	var (
		headers []*types.Header
		snap    *Snapshot
	)
	for snap == nil { //nolint:govet
		if s, ok := c.recents.Get(hash); ok {
			snap = s
			break
		}
		if number%c.snapshotConfig.CheckpointInterval == 0 {
			if s, err := loadSnapshot(c.config, c.DB, number, hash); err == nil {
				c.logger.Trace("Loaded voting snapshot from disk", "number", number, "hash", hash)
				snap = s
				break
			}
		}
		if number == 0 || (number%c.config.Epoch == 0 && (len(headers) > config3.FullImmutabilityThreshold || chain.GetHeaderByNumber(number-1) == nil)) {
			checkpoint := chain.GetHeaderByNumber(number)
			if checkpoint != nil {
				hash := checkpoint.Hash()

				signers := make([]common.Address, (len(checkpoint.Extra)-ExtraVanity-ExtraSeal)/length.Addr)
				for i := range signers {
					copy(signers[i][:], checkpoint.Extra[ExtraVanity+i*length.Addr:])
				}
				snap = newSnapshot(c.config, number, hash, signers)
				if err := snap.store(c.DB); err != nil {
					return nil, err
				}
				c.logger.Info("[Clique] Stored checkpoint snapshot to disk", "number", number, "hash", hash)
				break
			}
		}
		var header *types.Header
		if len(parents) > 0 {
			header = parents[len(parents)-1]
			if header.Hash() != hash || header.Number.Uint64() != number {
				return nil, rules.ErrUnknownAncestor
			}
			parents = parents[:len(parents)-1]
		} else {
			header = chain.GetHeader(hash, number)
			if header == nil {
				return nil, rules.ErrUnknownAncestor
			}
		}
		headers = append(headers, header)
		number, hash = number-1, header.ParentHash
	}
	for i := 0; i < len(headers)/2; i++ {
		headers[i], headers[len(headers)-1-i] = headers[len(headers)-1-i], headers[i]
	}
	snap, err := snap.apply(c.signatures, c.logger, headers...)
	if err != nil {
		return nil, err
	}
	c.recents.Add(snap.Hash, snap)

	if snap.Number%c.snapshotConfig.CheckpointInterval == 0 && len(headers) > 0 {
		if err := snap.store(c.DB); err != nil {
			return nil, err
		}
		c.logger.Trace("Stored voting snapshot to disk", "number", snap.Number, "hash", snap.Hash)
	}
	return snap, err
}

func (c *Clique) verifySeal(chain rules.ChainHeaderReader, header *types.Header, snap *Snapshot) error {
	number := header.Number.Uint64()
	if number == 0 {
		return errUnknownBlock
	}

	signer, err := ecrecover(header, c.signatures)
	if err != nil {
		return err
	}

	if _, ok := snap.Signers[signer]; !ok {
		return ErrUnauthorizedSigner
	}

	for seen, recent := range snap.Recents {
		if recent == signer {
			if limit := uint64(len(snap.Signers)/2 + 1); seen > number-limit {
				return ErrRecentlySigned
			}
		}
	}

	if !c.FakeDiff {
		inturn := snap.inturn(header.Number.Uint64(), signer.Value())
		if inturn && header.Difficulty.CmpUint64(DiffInTurn) != 0 {
			return errWrongDifficulty
		}
		if !inturn && header.Difficulty.CmpUint64(diffNoTurn) != 0 {
			return errWrongDifficulty
		}
	}

	return nil
}
