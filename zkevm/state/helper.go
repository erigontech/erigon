package state

import (
	"fmt"
	"math/big"
	"strconv"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/zkevm/hex"
	"github.com/ledgerwatch/erigon/zkevm/log"
)

// EncodeTransactions RLP encodes the given transactions
func EncodeTransactions(txs []types.Transaction) ([]byte, error) {
	var batchL2Data []byte

	for _, tx := range txs {
		v, r, s := tx.RawSignatureValues()
		sign := 1 - (v.Uint64() & 1)

		nonce, gasPrice, gas, to, value, data, chainID := tx.GetNonce(), tx.GetPrice(), tx.GetGas(), tx.GetTo(), tx.GetValue(), tx.GetData(), tx.GetChainID()
		log.Debug(nonce, " ", gasPrice, " ", gas, " ", to, " ", value, " ", len(data), " ", chainID)

		rlpFieldsToEncode := []interface{}{
			nonce,
			gasPrice,
			gas,
			to,
			value,
			data,
		}

		if tx.GetChainID().Uint64() > 0 {
			rlpFieldsToEncode = append(rlpFieldsToEncode, chainID)
			rlpFieldsToEncode = append(rlpFieldsToEncode, uint(0))
			rlpFieldsToEncode = append(rlpFieldsToEncode, uint(0))
		}

		txCodedRlp, err := rlp.EncodeToBytes(rlpFieldsToEncode)
		if err != nil {
			return nil, err
		}

		newV := new(big.Int).Add(big.NewInt(ether155V), big.NewInt(int64(sign)))
		newRPadded := fmt.Sprintf("%064x", r.String())
		newSPadded := fmt.Sprintf("%064x", s.String())
		newVPadded := fmt.Sprintf("%02x", newV.Text(hex.Base))
		txData, err := hex.DecodeString(hex.EncodeToString(txCodedRlp) + newRPadded + newSPadded + newVPadded)
		if err != nil {
			return nil, err
		}

		batchL2Data = append(batchL2Data, txData...)
	}

	return batchL2Data, nil
}

// EncodeTransaction RLP encodes the given transaction
func EncodeTransaction(tx types.Transaction) ([]byte, error) {
	transactions := []types.Transaction{tx}
	return EncodeTransactions(transactions)
}

// EncodeUnsignedTransaction RLP encodes the given unsigned transaction
func EncodeUnsignedTransaction(tx types.Transaction, chainID uint64, forcedNonce *uint64) ([]byte, error) {
	v, _ := new(big.Int).SetString("0x1c", 0)
	r, _ := new(big.Int).SetString("0xa54492cfacf71aef702421b7fbc70636537a7b2fbe5718c5ed970a001bb7756b", 0)
	s, _ := new(big.Int).SetString("0x2e9fb27acc75955b898f0b12ec52aa34bf08f01db654374484b80bf12f0d841e", 0)

	sign := 1 - (v.Uint64() & 1)

	nonce, gasPrice, gas, to, value, data, chainID := tx.GetNonce(), tx.GetPrice(), tx.GetGas(), tx.GetTo(), tx.GetValue(), tx.GetData(), chainID
	log.Debug(nonce, " ", gasPrice, " ", gas, " ", to, " ", value, " ", len(data), " ", chainID)

	if forcedNonce != nil {
		nonce = *forcedNonce
		log.Debug("Forced nonce: ", nonce)
	}

	txCodedRlp, err := rlp.EncodeToBytes([]interface{}{
		nonce,
		gasPrice,
		gas,
		to,
		value,
		data,
		big.NewInt(0).SetUint64(chainID), uint(0), uint(0),
	})

	if err != nil {
		return nil, err
	}

	newV := new(big.Int).Add(big.NewInt(ether155V), big.NewInt(int64(sign)))
	newRPadded := fmt.Sprintf("%064s", r.Text(hex.Base))
	newSPadded := fmt.Sprintf("%064s", s.Text(hex.Base))
	newVPadded := fmt.Sprintf("%02s", newV.Text(hex.Base))
	txData, err := hex.DecodeString(hex.EncodeToString(txCodedRlp) + newRPadded + newSPadded + newVPadded)
	if err != nil {
		return nil, err
	}

	return txData, nil
}

// DecodeTxs extracts Transactions for its encoded form
func DecodeTxsOld(txsData []byte) ([]types.Transaction, []byte, error) {
	// Process coded txs
	var pos uint64
	var txs []types.Transaction
	const (
		headerByteLength = 1
		sLength          = 32
		rLength          = 32
		vLength          = 1
		c0               = 192 // 192 is c0. This value is defined by the rlp protocol
		ff               = 255 // max value of rlp header
		shortRlp         = 55  // length of the short rlp codification
		f7               = 247 // 192 + 55 = c0 + shortRlp
	)
	txDataLength := len(txsData)
	if txDataLength == 0 {
		return txs, txsData, nil
	}
	for pos < uint64(txDataLength) {
		num, err := strconv.ParseUint(hex.EncodeToString(txsData[pos:pos+1]), hex.Base, hex.BitSize64)
		if err != nil {
			log.Debug("error parsing header length: ", err)
			return []types.Transaction{}, txsData, err
		}
		// First byte is the length and must be ignored
		len := num - c0
		if len > shortRlp { // If rlp is bigger than length 55
			// n is the length of the rlp data without the header (1 byte) for example "0xf7"
			if (pos + 1 + num - f7) > uint64(txDataLength) {
				log.Debug("error parsing length: ", err)
				return []types.Transaction{}, txsData, err
			}

			n, err := strconv.ParseUint(hex.EncodeToString(txsData[pos+1:pos+1+num-f7]), hex.Base, hex.BitSize64) // +1 is the header. For example 0xf7
			if err != nil {
				log.Debug("error parsing length: ", err)
				return []types.Transaction{}, txsData, err
			}
			len = n + num - f7 // num - f7 is the header. For example 0xf7
		}
		if len > uint64(txDataLength) || len < 0 {
			return []types.Transaction{}, txsData, ErrInvalidData
		}

		fullDataTx := txsData[pos : pos+len+rLength+sLength+vLength+headerByteLength]
		txInfo := txsData[pos : pos+len+headerByteLength]
		rData := txsData[pos+len+headerByteLength : pos+len+rLength+headerByteLength]
		sData := txsData[pos+len+rLength+headerByteLength : pos+len+rLength+sLength+headerByteLength]
		vData := txsData[pos+len+rLength+sLength+headerByteLength : pos+len+rLength+sLength+vLength+headerByteLength]

		pos = pos + len + rLength + sLength + vLength + headerByteLength

		// Decode rlpFields
		var rlpFields [][]byte
		err = rlp.DecodeBytes(txInfo, &rlpFields)
		if err != nil {
			log.Debug("error decoding tx bytes: ", err, ". fullDataTx: ", hex.EncodeToString(fullDataTx), "\n tx: ", hex.EncodeToString(txInfo), "\n Txs received: ", hex.EncodeToString(txsData))
			return []types.Transaction{}, txsData, err
		}

		legacyTx, err := RlpFieldsToLegacyTx(rlpFields, vData, rData, sData)
		if err != nil {
			log.Debug("error creating tx from rlp fields: ", err, ". fullDataTx: ", hex.EncodeToString(fullDataTx), "\n tx: ", hex.EncodeToString(txInfo), "\n Txs received: ", hex.EncodeToString(txsData))
			return []types.Transaction{}, txsData, err
		}

		txs = append(txs, legacyTx)
	}
	return txs, txsData, nil
}

const (
	forkID5      = 5
	double       = 2
	ether155V    = 27
	etherPre155V = 35
	// MaxEffectivePercentage is the maximum value that can be used as effective percentage
	MaxEffectivePercentage = uint8(255)
	// Decoding constants
	headerByteLength               uint64 = 1
	sLength                        uint64 = 32
	rLength                        uint64 = 32
	vLength                        uint64 = 1
	c0                             uint64 = 192 // 192 is c0. This value is defined by the rlp protocol
	ff                             uint64 = 255 // max value of rlp header
	shortRlp                       uint64 = 55  // length of the short rlp codification
	f7                             uint64 = 247 // 192 + 55 = c0 + shortRlp
	efficiencyPercentageByteLength uint64 = 1
)

// DecodeTxs extracts Transactions for its encoded form
func DecodeTxs(txsData []byte, forkID uint64) ([]types.Transaction, []byte, []uint8, error) {
	// Process coded txs
	var pos uint64
	var txs []types.Transaction
	var efficiencyPercentages []uint8
	txDataLength := uint64(len(txsData))
	if txDataLength == 0 {
		return txs, txsData, nil, nil
	}
	for pos < txDataLength {
		num, err := strconv.ParseUint(hex.EncodeToString(txsData[pos:pos+1]), hex.Base, hex.BitSize64)
		if err != nil {
			log.Debug("error parsing header length: ", err)
			return []types.Transaction{}, txsData, []uint8{}, err
		}
		// First byte is the length and must be ignored
		if num < c0 {
			log.Debugf("error num < c0 : %d, %d", num, c0)
			return []types.Transaction{}, txsData, []uint8{}, ErrInvalidData
		}
		length := uint64(num - c0)
		if length > shortRlp { // If rlp is bigger than length 55
			// n is the length of the rlp data without the header (1 byte) for example "0xf7"
			if (pos + 1 + num - f7) > txDataLength {
				log.Debug("error parsing length: ", err)
				return []types.Transaction{}, txsData, []uint8{}, err
			}
			n, err := strconv.ParseUint(hex.EncodeToString(txsData[pos+1:pos+1+num-f7]), hex.Base, hex.BitSize64) // +1 is the header. For example 0xf7
			if err != nil {
				log.Debug("error parsing length: ", err)
				return []types.Transaction{}, txsData, []uint8{}, err
			}
			if n+num < f7 {
				log.Debug("error n + num < f7: ", err)
				return []types.Transaction{}, txsData, []uint8{}, ErrInvalidData
			}
			length = n + num - f7 // num - f7 is the header. For example 0xf7
		}

		endPos := pos + length + rLength + sLength + vLength + headerByteLength

		if forkID >= forkID5 {
			endPos += efficiencyPercentageByteLength
		}

		if endPos > txDataLength {
			err := fmt.Errorf("endPos %d is bigger than txDataLength %d", endPos, txDataLength)
			log.Debug("error parsing header: ", err)
			return []types.Transaction{}, txsData, []uint8{}, ErrInvalidData
		}

		if endPos < pos {
			err := fmt.Errorf("endPos %d is smaller than pos %d", endPos, pos)
			log.Debug("error parsing header: ", err)
			return []types.Transaction{}, txsData, []uint8{}, ErrInvalidData
		}

		if endPos < pos {
			err := fmt.Errorf("endPos %d is smaller than pos %d", endPos, pos)
			log.Debug("error parsing header: ", err)
			return []types.Transaction{}, txsData, []uint8{}, ErrInvalidData
		}

		fullDataTx := txsData[pos:endPos]
		dataStart := pos + length + headerByteLength
		txInfo := txsData[pos:dataStart]
		rData := txsData[dataStart : dataStart+rLength]
		sData := txsData[dataStart+rLength : dataStart+rLength+sLength]
		vData := txsData[dataStart+rLength+sLength : dataStart+rLength+sLength+vLength]

		if forkID >= forkID5 {
			efficiencyPercentage := txsData[dataStart+rLength+sLength+vLength : endPos]
			efficiencyPercentages = append(efficiencyPercentages, uint8(efficiencyPercentage[0]))
		}

		pos = endPos

		// Decode rlpFields
		var rlpFields [][]byte
		err = rlp.DecodeBytes(txInfo, &rlpFields)
		if err != nil {
			log.Error("error decoding tx Bytes: ", err, ". fullDataTx: ", hex.EncodeToString(fullDataTx), "\n tx: ", hex.EncodeToString(txInfo), "\n Txs received: ", hex.EncodeToString(txsData))
			return []types.Transaction{}, txsData, []uint8{}, ErrInvalidData
		}

		legacyTx, err := RlpFieldsToLegacyTx(rlpFields, vData, rData, sData)
		if err != nil {
			log.Debug("error creating tx from rlp fields: ", err, ". fullDataTx: ", hex.EncodeToString(fullDataTx), "\n tx: ", hex.EncodeToString(txInfo), "\n Txs received: ", hex.EncodeToString(txsData))
			return []types.Transaction{}, txsData, []uint8{}, err
		}
		
		txs = append(txs, legacyTx)
	}
	return txs, txsData, efficiencyPercentages, nil
}

// DecodeTx decodes a string rlp tx representation into a types.Transaction instance
func DecodeTx(encodedTx string) (types.Transaction, error) {
	_, err := hex.DecodeHex(encodedTx)
	if err != nil {
		return nil, err
	}

	panic("III: use decodeRLP here if needed")

	/*
		tx := new(types.LegacyTx)
		if err := tx.UnmarshalBinary(b); err != nil {
			return nil, err
		}
		return tx, nil
	*/
}

func generateReceipt(blockNumber *big.Int, processedTx *ProcessTransactionResponse) *types.Receipt {
	receipt := &types.Receipt{
		Type:              uint8(processedTx.Type),
		PostState:         processedTx.StateRoot.Bytes(),
		CumulativeGasUsed: processedTx.GasUsed,
		BlockNumber:       blockNumber,
		GasUsed:           processedTx.GasUsed,
		TxHash:            processedTx.Tx.Hash(),
		TransactionIndex:  0,
		ContractAddress:   processedTx.CreateAddress,
		Logs:              processedTx.Logs,
	}

	// TODO: this fix is temporary while the Executor is returning a
	// different Tx hash for the TxHash, Log.TxHash and Tx.Hash().
	// At the moment, the processedTx.TxHash and Log[n].TxHash are
	// returning a different hash than the Hash of the transaction
	// sent to be processed by the Executor.
	// The processedTx.Tx.Hash() is correct.
	for i := 0; i < len(receipt.Logs); i++ {
		receipt.Logs[i].TxHash = processedTx.Tx.Hash()
	}
	if processedTx.RomError == nil {
		receipt.Status = types.ReceiptStatusSuccessful
	} else {
		receipt.Status = types.ReceiptStatusFailed
	}

	return receipt
}

func toPostgresInterval(duration string) (string, error) {
	unit := duration[len(duration)-1]
	var pgUnit string

	switch unit {
	case 's':
		pgUnit = "second"
	case 'm':
		pgUnit = "minute"
	case 'h':
		pgUnit = "hour"
	default:
		return "", ErrUnsupportedDuration
	}

	isMoreThanOne := duration[0] != '1' || len(duration) > 2 //nolint:gomnd
	if isMoreThanOne {
		pgUnit = pgUnit + "s"
	}

	return fmt.Sprintf("%s %s", duration[:len(duration)-1], pgUnit), nil
}
