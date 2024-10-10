package tx

import (
	"errors"
	"fmt"
	"math/big"
	"strconv"

	"bytes"

	"regexp"
	"strings"

	"encoding/binary"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/smt/pkg/utils"
	"github.com/ledgerwatch/erigon/zk/constants"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zkevm/hex"
	"github.com/ledgerwatch/log/v3"
)

const (
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

	changeL2BlockTxType = 11
	changeL2BlockLength = 9
)

var (
	ErrInvalidData = errors.New("invalid data")
)

type DecodedBatchL2Data struct {
	Transactions                 []types.Transaction
	EffectiveGasPricePercentages []uint8
	DeltaTimestamp               uint32
	L1InfoTreeIndex              uint32
}

func DecodeBatchL2Blocks(txsData []byte, forkID uint64) ([]DecodedBatchL2Data, error) {
	var result []DecodedBatchL2Data
	var pos uint64
	txDataLength := uint64(len(txsData))
	if txDataLength == 0 {
		return result, nil
	}

	currentData := DecodedBatchL2Data{}
	var currentDelta uint32
	var currentL1InfoTreeIndex uint32

	for pos < txDataLength {
		num, err := strconv.ParseUint(hex.EncodeToString(txsData[pos:pos+1]), hex.Base, hex.BitSize64)
		if err != nil {
			log.Debug("error parsing header length: ", err)
			return result, err
		}

		// if num is 11 then we are trying to parse a `changeL2Block` transaction
		if num == changeL2BlockTxType {
			hasStarted := pos > 0

			// if we aren't at the very start of the data then we know we're at a block boundary, so we want to capture
			// the data we have been working on so far (hasStarted handles this).  In some cases the data will not
			// contain any change l2 blocks, just pure transaction data, so the capture of currentData will be picked
			// up outside the loop over txData.
			//
			// We also want to check if the next byte is a change block transaction,
			// this is so that we can handle empty blocks with no transactions inside the data.
			if hasStarted || txsData[pos+1] == changeL2BlockTxType {
				currentData.DeltaTimestamp = currentDelta
				currentData.L1InfoTreeIndex = currentL1InfoTreeIndex
				result = append(result, currentData)
				currentData = DecodedBatchL2Data{}
			}

			currentDelta = binary.BigEndian.Uint32(txsData[pos+1 : pos+5])
			currentL1InfoTreeIndex = binary.BigEndian.Uint32(txsData[pos+5 : pos+9])

			pos += changeL2BlockLength

			continue
		}

		// First byte is the length and must be ignored
		if num < c0 {
			log.Debug("error num < c0 : %d, %d", num, c0)
			return result, ErrInvalidData
		}
		length := num - c0
		if length > shortRlp { // If rlp is bigger than length 55
			// n is the length of the rlp data without the header (1 byte) for example "0xf7"
			if (pos + 1 + num - f7) > txDataLength {
				log.Debug("error parsing length: ", err)
				return result, err
			}
			n, err := strconv.ParseUint(hex.EncodeToString(txsData[pos+1:pos+1+num-f7]), hex.Base, hex.BitSize64) // +1 is the header. For example 0xf7
			if err != nil {
				log.Debug("error parsing length: ", err)
				return result, err
			}
			if n+num < f7 {
				log.Debug("error n + num < f7: ", err)
				return result, ErrInvalidData
			}
			length = n + num - f7 // num - f7 is the header. For example 0xf7
		}

		endPos := pos + length + rLength + sLength + vLength + headerByteLength

		if forkID >= uint64(constants.ForkID5Dragonfruit) {
			endPos += efficiencyPercentageByteLength
		}

		if endPos > txDataLength {
			err := fmt.Errorf("endPos %d is bigger than txDataLength %d", endPos, txDataLength)
			log.Debug("error parsing header: ", err)
			return result, ErrInvalidData
		}

		if endPos < pos {
			err := fmt.Errorf("endPos %d is smaller than pos %d", endPos, pos)
			log.Debug("error parsing header: ", err)
			return result, ErrInvalidData
		}

		if endPos < pos {
			err := fmt.Errorf("endPos %d is smaller than pos %d", endPos, pos)
			log.Debug("error parsing header: ", err)
			return result, ErrInvalidData
		}

		fullDataTx := txsData[pos:endPos]
		dataStart := pos + length + headerByteLength
		txInfo := txsData[pos:dataStart]
		rData := txsData[dataStart : dataStart+rLength]
		sData := txsData[dataStart+rLength : dataStart+rLength+sLength]
		vData := txsData[dataStart+rLength+sLength : dataStart+rLength+sLength+vLength]

		if forkID >= uint64(constants.ForkID5Dragonfruit) {
			efficiencyPercentage := txsData[dataStart+rLength+sLength+vLength : endPos]
			currentData.EffectiveGasPricePercentages = append(currentData.EffectiveGasPricePercentages, efficiencyPercentage[0])
		}

		pos = endPos

		// Decode rlpFields
		var rlpFields [][]byte
		err = rlp.DecodeBytes(txInfo, &rlpFields)
		if err != nil {
			log.Error("error decoding tx Bytes: ", err, ". fullDataTx: ", hex.EncodeToString(fullDataTx), "\n tx: ", hex.EncodeToString(txInfo), "\n Transactions received: ", hex.EncodeToString(txsData))
			return result, ErrInvalidData
		}

		legacyTx, err := rlpFieldsToLegacyTx(rlpFields, vData, rData, sData)
		if err != nil {
			log.Debug("error creating tx from rlp fields: ", err, ". fullDataTx: ", hex.EncodeToString(fullDataTx), "\n tx: ", hex.EncodeToString(txInfo), "\n Transactions received: ", hex.EncodeToString(txsData))
			return result, err
		}

		currentData.Transactions = append(currentData.Transactions, legacyTx)
	}

	// always capture the last data as there won't have been a change l2 block to seal it off
	// or there are no change l2 blocks and the data only contains transaction info
	currentData.DeltaTimestamp = currentDelta
	currentData.L1InfoTreeIndex = currentL1InfoTreeIndex
	result = append(result, currentData)

	return result, nil
}

type TxDecoder func(encodedTx []byte, gasPricePercentage uint8, forkID uint64) (types.Transaction, uint8, error)

func DecodeTx(encodedTx []byte, efficiencyPercentage byte, forkId uint64) (types.Transaction, uint8, error) {
	// efficiencyPercentage := uint8(0)
	if forkId >= uint64(constants.ForkID5Dragonfruit) {
		encodedTx = append(encodedTx, efficiencyPercentage)
	}

	tx, err := types.DecodeTransaction(rlp.NewStream(bytes.NewReader(encodedTx), 0))
	if err != nil {
		return nil, 0, err
	}
	return tx, efficiencyPercentage, nil
}

// RlpFieldsToLegacyTx parses the rlp fields slice into a type.LegacyTx
// in this specific order:
//
// required fields:
// [0] Nonce    uint64
// [1] GasPrice *big.Int
// [2] Gas      uint64
// [3] To       *common.Address
// [4] Value    *big.Int
// [5] Data     []byte
// [6] Chain id *big.Int
//
// optional fields:
// [7] V        *big.Int
// [8] R        *big.Int
// [9] S        *big.Int
func rlpFieldsToLegacyTx(fields [][]byte, v, r, s []byte) (tx *types.LegacyTx, err error) {
	const (
		fieldsSizeWithoutChainID = 6
		fieldsSizeWithChainID    = 7
	)

	if len(fields) < fieldsSizeWithoutChainID {
		return nil, types.ErrTxTypeNotSupported
	}

	nonce := big.NewInt(0).SetBytes(fields[0]).Uint64()

	gasPriceI := &uint256.Int{}
	gasPriceI.SetBytes(fields[1])

	gas := big.NewInt(0).SetBytes(fields[2]).Uint64()
	var to *common.Address

	if fields[3] != nil && len(fields[3]) != 0 {
		tmp := common.BytesToAddress(fields[3])
		to = &tmp
	}
	value := big.NewInt(0).SetBytes(fields[4])
	data := fields[5]

	txV := big.NewInt(0).SetBytes(v)
	chainIDBig := big.NewInt(0)
	if len(fields) >= fieldsSizeWithChainID {
		chainIDBig = big.NewInt(0).SetBytes(fields[6])

		// a = chainId * 2
		// b = v - 27
		// c = a + 35
		// v = b + c
		//
		// same as:
		// v = v-27+chainId*2+35
		a := new(big.Int).Mul(chainIDBig, big.NewInt(double))
		b := new(big.Int).Sub(new(big.Int).SetBytes(v), big.NewInt(ether155V))
		c := new(big.Int).Add(a, big.NewInt(etherPre155V))
		txV = new(big.Int).Add(b, c)
	}
	chainID, _ := uint256.FromBig(chainIDBig)

	txVi := uint256.Int{}
	txVi.SetBytes(txV.Bytes())

	txRi := uint256.Int{}
	txRi.SetBytes(r)

	txSi := uint256.Int{}
	txSi.SetBytes(s)

	valueI := &uint256.Int{}
	valueI.SetBytes(value.Bytes())

	return &types.LegacyTx{
		CommonTx: types.CommonTx{
			Nonce:   nonce,
			Gas:     gas,
			To:      to,
			Value:   valueI,
			Data:    data,
			V:       txVi,
			R:       txRi,
			S:       txSi,
			ChainID: chainID,
		},
		GasPrice: gasPriceI,
	}, nil
}

/*
*
Copy of TransactionToL2Data with modifications:
- remove leading zeroes in individual byte arrays
- add two empty bytes at the end

Tne encoding is based on zkemv-commonjs/src/process-utils as of eb1ed1a1c05e2666cd32e3900beff5121bdeb4db
*/
func TransactionToL2Data(tx types.Transaction, forkId uint16, efficiencyPercentage uint8) ([]byte, error) {
	nonceBytes := hermez_db.Uint64ToBytes(tx.GetNonce())
	gasPriceBytes := tx.GetPrice().Bytes()
	gas := hermez_db.Uint64ToBytes(tx.GetGas())
	val := uint256.NewInt(0)
	if tx.GetValue() != nil {
		val = tx.GetValue()
	}
	valueBytes := val.Bytes()
	chainIdBytes := tx.GetChainID()

	var to []byte
	if tx.GetTo() != nil {
		to = tx.GetTo().Bytes()
	}

	v, r, s := tx.RawSignatureValues()

	toEncode := [][]byte{
		removeLeadingZeroesFromBytes(nonceBytes),
		removeLeadingZeroesFromBytes(gasPriceBytes),
		removeLeadingZeroesFromBytes(gas),
		to, // don't remove leading 0s from addr
		removeLeadingZeroesFromBytes(valueBytes),
		tx.GetData(),
	}

	if !tx.GetChainID().Eq(uint256.NewInt(0)) || !(v.Eq(uint256.NewInt(27)) || v.Eq(uint256.NewInt(28))) {
		toEncode = append(toEncode, removeLeadingZeroesFromBytes(chainIdBytes.Bytes()))
		toEncode = append(toEncode, []byte{})
		toEncode = append(toEncode, []byte{})
	}

	encoded, err := rlp.EncodeToBytes(toEncode)
	if err != nil {
		return nil, err
	}

	// reverse the eip-155 changes for the V value for transport
	v = GetDecodedV(tx, v)
	txV := new(big.Int).SetBytes(v.Bytes())

	vBytes := txV.Bytes()
	rBytes := r.Bytes32()
	sBytes := s.Bytes32()

	encoded = append(encoded, rBytes[:]...)
	encoded = append(encoded, sBytes[:]...)
	encoded = append(encoded, vBytes...)

	if forkId >= uint16(constants.ForkID5Dragonfruit) {
		ep := hermez_db.Uint8ToBytes(efficiencyPercentage)
		encoded = append(encoded, ep...)
	}

	return encoded, nil
}

func removeLeadingZeroesFromBytes(source []byte) []byte {
	size := len(source)
	leadingZeroes := 0
	for ; leadingZeroes < size; leadingZeroes++ {
		if source[leadingZeroes] != 0 {
			break
		}
	}

	return source[leadingZeroes:]
}

func GetDecodedV(tx types.Transaction, v *uint256.Int) *uint256.Int {
	if !tx.Protected() {
		return v
	}

	multiChain := new(big.Int).Mul(tx.GetChainID().ToBig(), big.NewInt(double))
	plus155V := new(big.Int).Add(new(big.Int).SetBytes(v.Bytes()), big.NewInt(ether155V))
	txV := new(big.Int).Sub(plus155V, multiChain)
	txV = txV.Sub(txV, big.NewInt(etherPre155V))

	result, _ := uint256.FromBig(txV)
	return result
}

type BatchTxData struct {
	Transaction                 types.Transaction
	EffectiveGasPricePercentage uint8
}

func GenerateBlockBatchL2Data(forkId uint16, deltaTimestamp uint32, l1InfoTreeIndex uint32, transactionsData []BatchTxData) ([]byte, error) {
	result := make([]byte, 0)
	// add in the changeL2Block transaction if after forkId 7
	if forkId >= uint16(constants.ForkID7Etrog) {
		result = GenerateStartBlockBatchL2Data(deltaTimestamp, l1InfoTreeIndex)
	}
	for _, transactionData := range transactionsData {
		encoded, err := TransactionToL2Data(transactionData.Transaction, forkId, transactionData.EffectiveGasPricePercentage)
		if err != nil {
			return nil, err
		}
		result = append(result, encoded...)
	}

	return result, nil
}

var (
	START_BLOCK_BATCH_L2_DATA_SIZE = uint64(65) // change this if GenerateStartBlockBatchL2Data changes
)

func GenerateStartBlockBatchL2Data(deltaTimestamp uint32, l1InfoTreeIndex uint32) []byte {
	var result []byte

	// add in the changeL2Block transaction
	result = append(result, changeL2BlockTxType)
	result = binary.BigEndian.AppendUint32(result, deltaTimestamp)
	result = binary.BigEndian.AppendUint32(result, l1InfoTreeIndex)

	return result
}

func ComputeL2TxHash(
	chainId *big.Int,
	value, gasPrice *uint256.Int,
	nonce, txGasLimit uint64,
	to, from *common.Address,
	data []byte,
) (common.Hash, error) {

	txType := "01"
	if chainId == nil || chainId.Cmp(big.NewInt(0)) == 0 {
		txType = "00"
	}

	// add txType, nonce, gasPrice and gasLimit
	noncePart, err := formatL2TxHashParam(nonce, 8)
	if err != nil {
		return common.Hash{}, err

	}
	gasPricePart, err := formatL2TxHashParam(gasPrice, 32)
	if err != nil {
		return common.Hash{}, err
	}
	gasLimitPart, err := formatL2TxHashParam(txGasLimit, 8)
	if err != nil {
		return common.Hash{}, err
	}
	hash := fmt.Sprintf("%s%s%s%s", txType, noncePart, gasPricePart, gasLimitPart)

	// check is deploy
	if to == nil {
		hash += "01"
	} else {
		toPart, err := formatL2TxHashParam(to.Hex(), 20)
		if err != nil {
			return common.Hash{}, err
		}
		hash += fmt.Sprintf("00%s", toPart)
	}
	// add value
	valuePart, err := formatL2TxHashParam(value, 32)
	if err != nil {
		return common.Hash{}, err
	}
	hash += valuePart

	// compute data length
	dataStr := hex.EncodeToHex(data)
	if len(dataStr) > 1 && dataStr[:2] == "0x" {
		dataStr = dataStr[2:]
	}

	//round to ceil
	dataLength := (len(dataStr) + 1) / 2
	dataLengthPart, err := formatL2TxHashParam(dataLength, 3)
	if err != nil {
		return common.Hash{}, err
	}
	hash += dataLengthPart

	if dataLength > 0 {
		dataPart, err := formatL2TxHashParam(dataStr, dataLength)
		if err != nil {
			return common.Hash{}, err
		}
		hash += dataPart
	}

	// add chainID
	if chainId != nil && chainId.Cmp(big.NewInt(0)) != 0 {
		chainIDPart, err := formatL2TxHashParam(chainId, 8)
		if err != nil {
			return common.Hash{}, err
		}
		hash += chainIDPart
	}

	// add from
	fromPart, err := formatL2TxHashParam(from.Hex(), 20)
	if err != nil {
		return common.Hash{}, err
	}
	hash += fromPart

	hashed := utils.HashContractBytecodeBigInt(hash)
	return common.BigToHash(hashed), nil
}

var re = regexp.MustCompile("^[0-9a-fA-F]*$")

func formatL2TxHashParam(param interface{}, paramLength int) (string, error) {
	var paramStr string

	switch v := param.(type) {
	case int:
		if v == 0 {
			paramStr = "0"
		} else {
			paramStr = strconv.FormatInt(int64(v), 16)
		}
	case int64:
		if v == 0 {
			paramStr = "0"
		} else {
			paramStr = strconv.FormatInt(v, 16)
		}
	case uint:
		if v == 0 {
			paramStr = "0"
		} else {
			paramStr = strconv.FormatUint(uint64(v), 16)
		}
	case uint64:
		if v == 0 {
			paramStr = "0"
		} else {
			paramStr = strconv.FormatUint(v, 16)
		}
	case *big.Int:
		if v.Sign() == 0 {
			paramStr = "0"
		} else {
			paramStr = v.Text(16)
		}
	case *uint256.Int:
		if v == nil {
			paramStr = "0"
		} else {
			paramStr = v.Hex()
		}
	case []uint8:
		paramStr = hex.EncodeToHex(v)
	case common.Address:
		paramStr = v.Hex()
	case string:
		paramStr = v
	default:
		return "", fmt.Errorf("unsupported parameter type")
	}

	paramStr = strings.TrimPrefix(paramStr, "0x")

	if len(paramStr)%2 == 1 {
		paramStr = "0" + paramStr
	}

	if !re.MatchString(paramStr) {
		return "", fmt.Errorf("invalid hex string")
	}

	paramStr = padStart(paramStr, paramLength*2, "0")

	return paramStr, nil
}

func padStart(str string, length int, pad string) string {
	if len(str) >= length {
		return str
	}
	padding := strings.Repeat(pad, length-len(str))
	return padding[:length-len(str)] + str
}
