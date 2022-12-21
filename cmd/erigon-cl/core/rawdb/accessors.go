package rawdb

import (
	"encoding/binary"
	"fmt"
	"math/big"

	common2 "github.com/ledgerwatch/erigon-lib/common"
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

func WriteBeaconBlock(tx kv.RwTx, signedBlock *cltypes.SignedBeaconBlockBellatrix) error {
	var (
		block     = signedBlock.Block
		blockBody = block.Body
		payload   = blockBody.ExecutionPayload
	)

	// database key is is [slot + body root]
	key := EncodeNumber(block.Slot)
	// 34068 is the biggest it can ever be.
	value := make([]byte, 0, 33553)                    // Pre-allocate some data for faster appending
	value = append(value, signedBlock.Signature[:]...) // First 96 bytes is the signature
	// Block level encoding
	value = append(value, EncodeNumber(block.ProposerIndex)...) // Proposer index is next
	value = append(value, block.ParentRoot[:]...)               // Encode ParentRoot
	value = append(value, block.StateRoot[:]...)                // Encode State Root
	// Body level encoding
	value = append(value, blockBody.RandaoReveal[:]...) // Encode Randao Reveal
	value = append(value, blockBody.Graffiti...)        // Encode Graffiti
	var err error
	// Encode eth1Data
	if value, err = EncodeSSZ(value, blockBody.Eth1Data); err != nil {
		return err
	}
	// Encode Sync Aggregate
	if value, err = EncodeSSZ(value, blockBody.SyncAggregate); err != nil {
		return err
	}
	// Encode Attestantions
	value = append(value, byte(len(blockBody.Attestations)))
	for _, entry := range blockBody.Attestations {
		value = append(value, Bytes2FromLength(entry.SizeSSZ())...)
		if value, err = EncodeSSZ(value, entry); err != nil {
			return err
		}
	}
	// Encode Proposer Slashings
	value = append(value, byte(len(blockBody.ProposerSlashings)))
	for _, entry := range blockBody.ProposerSlashings {
		if value, err = EncodeSSZ(value, entry); err != nil {
			return err
		}
	}
	// Encode Attester Slashings
	value = append(value, byte(len(blockBody.AttesterSlashings)))
	for _, entry := range blockBody.AttesterSlashings {
		value = append(value, Bytes2FromLength(entry.SizeSSZ())...)
		if value, err = EncodeSSZ(value, entry); err != nil {
			return err
		}
	}
	// Encode Voluntary Exits
	value = append(value, byte(len(blockBody.VoluntaryExits)))
	for _, entry := range blockBody.VoluntaryExits {
		if value, err = EncodeSSZ(value, entry); err != nil {
			return err
		}
	}
	// Encode Deposits
	value = append(value, byte(len(blockBody.Deposits)))
	for _, entry := range blockBody.Deposits {
		if value, err = EncodeSSZ(value, entry); err != nil {
			return err
		}
	}
	value = append(value, EncodeNumber(payload.BlockNumber)...) // Proposer index is next
	// Execution Payload is stored separately in EL format
	value = append(value, payload.BlockHash[:]...)
	if err := WriteExecutionPayload(tx, payload); err != nil {
		return err
	}
	value = utils.CompressSnappy(value)
	// Finally write the beacon block
	return tx.Put(kv.BeaconBlocks, key, value)
}

func ReadBeaconBlock(tx kv.RwTx, slot uint64) (*cltypes.SignedBeaconBlockBellatrix, error) {
	key := EncodeNumber(slot)
	beaconBlockBytesCompressed, err := tx.GetOne(kv.BeaconBlocks, key)
	if err != nil {
		return nil, err
	}
	if len(beaconBlockBytesCompressed) == 0 {
		return nil, nil
	}
	beaconBlockBytes, err := utils.DecompressSnappy(beaconBlockBytesCompressed)
	if err != nil {
		return nil, err
	}
	signedBeaconBlock := &cltypes.SignedBeaconBlockBellatrix{}
	beaconBlock := &cltypes.BeaconBlockBellatrix{}
	beaconBody := &cltypes.BeaconBodyBellatrix{}

	pos := 96
	// Signature level
	copy(signedBeaconBlock.Signature[:], beaconBlockBytes)
	// Block level
	beaconBlock.Slot = slot
	beaconBlock.ProposerIndex = uint64(binary.BigEndian.Uint32(beaconBlockBytes[pos:]))
	pos += 4
	// Encode roots
	copy(beaconBlock.ParentRoot[:], beaconBlockBytes[pos:])
	copy(beaconBlock.StateRoot[:], beaconBlockBytes[pos+32:])
	pos += 64
	// Body level encoding
	copy(beaconBody.RandaoReveal[:], beaconBlockBytes[pos:])
	beaconBody.Graffiti = make([]byte, 32)
	copy(beaconBody.Graffiti, beaconBlockBytes[pos+96:])
	pos += 128
	// Eth1Data
	beaconBody.Eth1Data = &cltypes.Eth1Data{}
	if err := beaconBody.Eth1Data.UnmarshalSSZ(beaconBlockBytes[pos : pos+beaconBody.Eth1Data.SizeSSZ()]); err != nil {
		return nil, err
	}
	pos += beaconBody.Eth1Data.SizeSSZ()
	// Sync Aggreggate
	beaconBody.SyncAggregate = &cltypes.SyncAggregate{}
	if err := beaconBody.SyncAggregate.UnmarshalSSZ(beaconBlockBytes[pos : pos+beaconBody.SyncAggregate.SizeSSZ()]); err != nil {
		return nil, err
	}
	pos += beaconBody.SyncAggregate.SizeSSZ()
	// Attestantions
	payloadLength := int(beaconBlockBytes[pos])
	pos++
	beaconBody.Attestations = make([]*cltypes.Attestation, payloadLength)
	for i := 0; i < payloadLength; i++ {
		beaconBody.Attestations[i] = &cltypes.Attestation{}
		sizeSSZ := LengthFromBytes2(beaconBlockBytes[pos:])
		pos += 2
		if err := beaconBody.Attestations[i].UnmarshalSSZ(beaconBlockBytes[pos : pos+sizeSSZ]); err != nil {
			return nil, err
		}
		pos += sizeSSZ
	}
	// Proposer slashings
	payloadLength = int(beaconBlockBytes[pos])
	pos++
	beaconBody.ProposerSlashings = make([]*cltypes.ProposerSlashing, payloadLength)
	for i := 0; i < payloadLength; i++ {
		beaconBody.ProposerSlashings[i] = &cltypes.ProposerSlashing{}
		if err := beaconBody.ProposerSlashings[i].UnmarshalSSZ(beaconBlockBytes[pos : pos+beaconBody.ProposerSlashings[i].SizeSSZ()]); err != nil {
			return nil, err
		}
		pos += beaconBody.ProposerSlashings[i].SizeSSZ()
	}
	// Attester slashings
	payloadLength = int(beaconBlockBytes[pos])
	pos++
	beaconBody.AttesterSlashings = make([]*cltypes.AttesterSlashing, payloadLength)
	for i := 0; i < payloadLength; i++ {
		sizeSSZ := LengthFromBytes2(beaconBlockBytes[pos:])
		pos += 2
		beaconBody.AttesterSlashings[i] = &cltypes.AttesterSlashing{}
		if err := beaconBody.AttesterSlashings[i].UnmarshalSSZ(beaconBlockBytes[pos : pos+sizeSSZ]); err != nil {
			return nil, err
		}
		pos += sizeSSZ
	}
	// Voluntary Exits
	payloadLength = int(beaconBlockBytes[pos])
	pos++
	beaconBody.VoluntaryExits = make([]*cltypes.SignedVoluntaryExit, payloadLength)
	for i := 0; i < payloadLength; i++ {
		beaconBody.VoluntaryExits[i] = &cltypes.SignedVoluntaryExit{}
		if err := beaconBody.VoluntaryExits[i].UnmarshalSSZ(beaconBlockBytes[pos : pos+beaconBody.VoluntaryExits[i].SizeSSZ()]); err != nil {
			return nil, err
		}
		pos += beaconBody.VoluntaryExits[i].SizeSSZ()
	}
	// Deposits
	payloadLength = int(beaconBlockBytes[pos])
	pos++
	beaconBody.Deposits = make([]*cltypes.Deposit, payloadLength)
	for i := 0; i < payloadLength; i++ {
		beaconBody.Deposits[i] = &cltypes.Deposit{}
		if err := beaconBody.Deposits[i].UnmarshalSSZ(beaconBlockBytes[pos : pos+beaconBody.Deposits[i].SizeSSZ()]); err != nil {
			return nil, err
		}
		pos += beaconBody.Deposits[i].SizeSSZ()
	}
	// last bytes is the ETH1 block number and hash
	blockNumber := uint64(binary.BigEndian.Uint32(beaconBlockBytes[pos:]))
	pos += 4
	hash := common.BytesToHash(beaconBlockBytes[pos : pos+32])
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
	return signedBeaconBlock, nil
}

// WriteExecutionPayload Writes Execution Payload in EL format
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
