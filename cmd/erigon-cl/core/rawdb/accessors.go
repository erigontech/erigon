package rawdb

import (
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus/serenity"
	rawdb2 "github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	ssz "github.com/prysmaticlabs/fastssz"
)

func EncodeNumber(n uint64) []byte {
	ret := make([]byte, 4)
	binary.BigEndian.PutUint32(ret, uint32(n))
	return ret
}

// WriteBeaconState writes beacon state for specific block to database.
func WriteBeaconState(tx kv.Putter, state *state.BeaconState) error {
	data, err := utils.EncodeSSZSnappy(state)
	if err != nil {
		return err
	}

	return tx.Put(kv.BeaconState, EncodeNumber(state.Slot()), data)
}

// ReadBeaconState reads beacon state for specific block from database.
func ReadBeaconState(tx kv.Getter, slot uint64) (*state.BeaconState, error) {
	data, err := tx.GetOne(kv.BeaconState, EncodeNumber(slot))
	if err != nil {
		return nil, err
	}
	bellatrixState := &cltypes.BeaconStateBellatrix{}

	if len(data) == 0 {
		return nil, nil
	}

	if err := utils.DecodeSSZSnappy(bellatrixState, data); err != nil {
		return nil, err
	}

	return state.FromBellatrixState(bellatrixState), nil
}

func WriteLightClientUpdate(tx kv.RwTx, update *cltypes.LightClientUpdate) error {
	key := make([]byte, 4)
	binary.BigEndian.PutUint32(key, uint32(update.SignatureSlot/8192))

	encoded, err := update.MarshalSSZ()
	if err != nil {
		return err
	}
	return tx.Put(kv.LightClientUpdates, key, encoded)
}

func WriteLightClientFinalityUpdate(tx kv.RwTx, update *cltypes.LightClientFinalityUpdate) error {
	encoded, err := update.MarshalSSZ()
	if err != nil {
		return err
	}
	return tx.Put(kv.LightClient, kv.LightClientFinalityUpdate, encoded)
}

func WriteLightClientOptimisticUpdate(tx kv.RwTx, update *cltypes.LightClientOptimisticUpdate) error {
	encoded, err := update.MarshalSSZ()
	if err != nil {
		return err
	}
	return tx.Put(kv.LightClient, kv.LightClientOptimisticUpdate, encoded)
}

func ReadLightClientUpdate(tx kv.RwTx, period uint32) (*cltypes.LightClientUpdate, error) {
	key := make([]byte, 4)
	binary.BigEndian.PutUint32(key, period)

	encoded, err := tx.GetOne(kv.LightClientUpdates, key)
	if err != nil {
		return nil, err
	}
	update := &cltypes.LightClientUpdate{}
	if err = update.UnmarshalSSZ(encoded); err != nil {
		return nil, err
	}
	return update, nil
}

func ReadLightClientFinalityUpdate(tx kv.Tx) (*cltypes.LightClientFinalityUpdate, error) {
	encoded, err := tx.GetOne(kv.LightClient, kv.LightClientFinalityUpdate)
	if err != nil {
		return nil, err
	}
	if len(encoded) == 0 {
		return nil, nil
	}
	update := &cltypes.LightClientFinalityUpdate{}
	if err = update.UnmarshalSSZ(encoded); err != nil {
		return nil, err
	}
	return update, nil
}

func ReadLightClientOptimisticUpdate(tx kv.Tx) (*cltypes.LightClientOptimisticUpdate, error) {
	encoded, err := tx.GetOne(kv.LightClient, kv.LightClientOptimisticUpdate)
	if err != nil {
		return nil, err
	}
	if len(encoded) == 0 {
		return nil, nil
	}
	update := &cltypes.LightClientOptimisticUpdate{}
	if err = update.UnmarshalSSZ(encoded); err != nil {
		return nil, err
	}
	return update, nil
}

func EncodeSSZ(prefix []byte, object ssz.Marshaler) ([]byte, error) {
	enc, err := object.MarshalSSZ()
	if err != nil {
		return nil, err
	}
	return append(prefix, enc...), nil
}

// Bytes2FromLength convert length to 2 bytes repressentation
func Bytes2FromLength(size int) []byte {
	return []byte{
		byte(size>>8) & 0xFF,
		byte(size>>0) & 0xFF,
	}
}

// LengthBytes2 convert length to 2 bytes repressentation
func LengthFromBytes2(buf []byte) int {
	return int(buf[0])*0x100 + int(buf[1])
}

func WriteAttestations(tx kv.RwTx, slot uint64, attestations []*cltypes.Attestation) error {
	return tx.Put(kv.Attestetations, EncodeNumber(slot), cltypes.EncodeAttestationsForStorage(attestations))
}

func ReadAttestations(tx kv.RwTx, slot uint64) ([]*cltypes.Attestation, error) {
	attestationsEncoded, err := tx.GetOne(kv.Attestetations, EncodeNumber(slot))
	if err != nil {
		return nil, err
	}
	return cltypes.DecodeAttestationsForStorage(attestationsEncoded)
}

func WriteBeaconBlock(tx kv.RwTx, signedBlock *cltypes.SignedBeaconBlock) error {
	var (
		block     = signedBlock.Block
		blockBody = block.Body
		//payload   = blockBody.ExecutionPayload
	)

	// database key is is [slot + body root]
	key := EncodeNumber(block.Slot)
	value, err := signedBlock.EncodeForStorage()
	if err != nil {
		return err
	}
	/*if err := WriteExecutionPayload(tx, payload); err != nil {
		return err
	}*/

	if err := WriteAttestations(tx, block.Slot, blockBody.Attestations); err != nil {
		return err
	}
	// Finally write the beacon block
	return tx.Put(kv.BeaconBlocks, key, value)
}

func ReadBeaconBlock(tx kv.RwTx, slot uint64) (*cltypes.SignedBeaconBlock, error) {
	signedBlock, _, _, _, err := ReadBeaconBlockForStorage(tx, slot)
	if err != nil {
		return nil, err
	}
	if signedBlock == nil {
		return nil, nil
	}

	attestations, err := ReadAttestations(tx, slot)
	if err != nil {
		return nil, err
	}
	signedBlock.Block.Body.Attestations = attestations
	return signedBlock, err
	/*
		// Process payload
		header := rawdb2.ReadHeader(tx, hash, blockNumber)
		if header == nil {
			beaconBlock.Body = beaconBody
			signedBeaconBlock.Block = beaconBlock
			return signedBeaconBlock, nil // Header is empty so avoid writing EL data.
		}
		// Pack basic
		payload := &cltypes.ExecutionPayload{
			ParentHash:   header.ParentHash,
			FeeRecipient: header.Coinbase,
			StateRoot:    header.Root,
			ReceiptsRoot: header.ReceiptHash,
			LogsBloom:    header.Bloom[:],
			PrevRandao:   header.MixDigest,
			BlockNumber:  header.Number.Uint64(),
			GasLimit:     header.GasLimit,
			GasUsed:      header.GasUsed,
			Timestamp:    header.Time,
			ExtraData:    header.Extra,
			BlockHash:    hash,
		}
		// TODO: pack back the baseFee
		if header.BaseFee != nil {
			baseFeeBytes := header.BaseFee.Bytes()
			payload.BaseFeePerGas = make([]byte, 32)
			for i, j := 0, len(baseFeeBytes)-1; i < j; i, j = i+1, j-1 {
				baseFeeBytes[i], baseFeeBytes[j] = baseFeeBytes[j], baseFeeBytes[i]
			}
			copy(payload.BaseFeePerGas, baseFeeBytes)
		}

		body, err := rawdb2.ReadStorageBody(tx, hash, blockNumber)
		if err != nil {
			return nil, err
		}
		if err := tx.ForAmount(kv.EthTx, common2.EncodeTs(body.BaseTxId+1), body.TxAmount-2, func(k, v []byte) error {
			payload.Transactions = append(payload.Transactions, v)
			return nil
		}); err != nil {
			return nil, err
		}
		//
		// Pack types
		beaconBody.ExecutionPayload = payload
		beaconBlock.Body = beaconBody
		signedBeaconBlock.Block = beaconBlock
		return signedBeaconBlock, nil*/
}

func ReadBeaconBlockForStorage(tx kv.Getter, slot uint64) (block *cltypes.SignedBeaconBlock, eth1Number uint64, eth1Hash common.Hash, eth2Hash common.Hash, err error) {
	encodedBeaconBlock, err := tx.GetOne(kv.BeaconBlocks, EncodeNumber(slot))
	if err != nil {
		return nil, 0, common.Hash{}, common.Hash{}, err
	}
	if len(encodedBeaconBlock) == 0 {
		return nil, 0, common.Hash{}, common.Hash{}, nil
	}
	if len(encodedBeaconBlock) == 0 {
		return nil, 0, common.Hash{}, common.Hash{}, nil
	}
	return cltypes.DecodeBeaconBlockForStorage(encodedBeaconBlock)
}

// WriteExecutionPayload Writes Execution Payload in EL format.. [Will be removed soonish]
func WriteExecutionPayload(tx kv.RwTx, payload *cltypes.ExecutionPayload) error {
	header := &types.Header{
		ParentHash:  common.BytesToHash(payload.ParentHash[:]),
		UncleHash:   types.EmptyUncleHash,
		Coinbase:    common.BytesToAddress(payload.FeeRecipient[:]),
		Root:        common.BytesToHash(payload.StateRoot[:]),
		TxHash:      types.DeriveSha(types.BinaryTransactions(payload.Transactions)),
		ReceiptHash: common.BytesToHash(payload.ReceiptsRoot[:]),
		Bloom:       types.BytesToBloom(payload.LogsBloom),
		Difficulty:  serenity.SerenityDifficulty,
		Number:      big.NewInt(int64(payload.BlockNumber)),
		GasLimit:    payload.GasLimit,
		GasUsed:     payload.GasUsed,
		Time:        payload.Timestamp,
		Extra:       payload.ExtraData,
		MixDigest:   common.BytesToHash(payload.PrevRandao[:]),
		Nonce:       serenity.SerenityNonce,
	}

	if len(payload.BaseFeePerGas) > 0 {
		baseFeeBytes := common.CopyBytes(payload.BaseFeePerGas)
		for len(baseFeeBytes) > 0 && baseFeeBytes[len(baseFeeBytes)-1] == 0 {
			baseFeeBytes = baseFeeBytes[:len(baseFeeBytes)-1]
		}
		for i, j := 0, len(baseFeeBytes)-1; i < j; i, j = i+1, j-1 {
			baseFeeBytes[i], baseFeeBytes[j] = baseFeeBytes[j], baseFeeBytes[i]
		}
		header.BaseFee = new(big.Int).SetBytes(baseFeeBytes)
	}
	hash := header.Hash()
	// Sanity check to see if we decoded the header correctly
	if payload.BlockHash != hash {
		return fmt.Errorf("mismatching header hashes %x != %x", payload.BlockHash, hash)
	}

	// Write header and body in ETH1 storage
	rawdb2.WriteHeader(tx, header)
	_, _, err := rawdb2.WriteRawBodyIfNotExists(tx, hash, header.Number.Uint64(), &types.RawBody{
		Transactions: payload.Transactions,
	})

	return err
}
