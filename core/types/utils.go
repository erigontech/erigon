package types

import (
	"errors"
	"io"

	rlp2 "github.com/ledgerwatch/erigon-lib/rlp"
	"github.com/ledgerwatch/erigon/rlp"
)

// expects either [][]byte or []Transactions, but not both
func txnsPayloadSize(byteTxns [][]byte, txns []Transaction) (txsLen int) {
	if byteTxns != nil {
		for _, tx := range byteTxns {
			txsLen += len(tx)
		}
		return
	}

	for _, tx := range txns {
		txLen := tx.EncodingSize()
		txsLen += rlp2.ListPrefixLen(txLen) + txLen
	}

	return
}

func unclesPayloadSize(uncles []*Header) (unclesLen int) {
	for _, uncle := range uncles {
		uncleLen := uncle.EncodingSize()
		unclesLen += rlp2.ListPrefixLen(uncleLen) + uncleLen
	}
	return
}

func withdrawalsPayloadSize(withdrawals []*Withdrawal) (withdrawalsLen int) {
	for _, withdrawal := range withdrawals {
		withdrawalLen := withdrawal.EncodingSize()
		withdrawalsLen += rlp2.ListPrefixLen(withdrawalLen) + withdrawalLen
	}
	return
}

// expects either [][]byte or []Transactions, but not both
func encodeTxns(byteTxns [][]byte, txns []Transaction, txsLen int, w io.Writer, b []byte) error {
	if err := EncodeStructSizePrefix(txsLen, w, b[:]); err != nil {
		return err
	}

	if byteTxns != nil {
		for _, tx := range byteTxns {
			if _, err := w.Write(tx); err != nil {
				return err
			}
		}
		return nil
	}

	for _, tx := range txns {
		if err := tx.EncodeRLP(w); err != nil {
			return err
		}
	}

	return nil
}

func encodeUncles(uncles []*Header, unclesLen int, w io.Writer, b []byte) error {
	if err := EncodeStructSizePrefix(unclesLen, w, b[:]); err != nil {
		return err
	}
	for _, uncle := range uncles {
		if err := uncle.EncodeRLP(w); err != nil {
			return err
		}
	}
	return nil
}

func encodeWithdrawals(withdrawals []*Withdrawal, withdrawalsLen int, w io.Writer, b []byte) error {
	if err := EncodeStructSizePrefix(withdrawalsLen, w, b[:]); err != nil {
		return err
	}
	for _, withdrawal := range withdrawals {
		if err := withdrawal.EncodeRLP(w); err != nil {
			return err
		}
	}
	return nil
}

// expects either *[][]byte or *[]Transaction, but not both
func decodeTxns(s *rlp.Stream, bytesRecv *[][]byte, txnRecv *[]Transaction) (err error) {
	// decode Transactions
	if _, err = s.List(); err != nil {
		return err
	}

	if bytesRecv != nil {
		var tx []byte
		for tx, err = s.Raw(); err == nil; tx, err = s.Raw() {
			if tx == nil {
				return errors.New("RawBody.DecodeRLP tx nil")
			}
			*bytesRecv = append(*bytesRecv, tx)
		}
		if !errors.Is(err, rlp.EOL) {
			return err
		}
		// end of Transactions
		if err = s.ListEnd(); err != nil {
			return err
		}
		return nil
	}

	var tx Transaction
	blobTxnsAreWrappedWithBlobs := false
	for tx, err = DecodeRLPTransaction(s, blobTxnsAreWrappedWithBlobs); err == nil; tx, err = DecodeRLPTransaction(s, blobTxnsAreWrappedWithBlobs) {
		*txnRecv = append(*txnRecv, tx)
	}
	if !errors.Is(err, rlp.EOL) {
		return err
	}
	// end of Transactions
	if err = s.ListEnd(); err != nil {
		return err
	}
	return nil
}

func decodeUncles(s *rlp.Stream, recv *[]*Header) (err error) {
	// decode Uncles
	if _, err = s.List(); err != nil {
		return err
	}
	for err == nil {
		var uncle Header
		if err = uncle.DecodeRLP(s); err != nil {
			break
		}
		*recv = append(*recv, &uncle)
	}
	if !errors.Is(err, rlp.EOL) {
		return err
	}
	// end of Uncles
	if err = s.ListEnd(); err != nil {
		return err
	}
	return nil
}

func decodeWithdrawals(s *rlp.Stream, recv *[]*Withdrawal, couldEmpty bool) (err error) {
	for err == nil {
		var withdrawal Withdrawal
		if err = withdrawal.DecodeRLP(s); err != nil {
			if couldEmpty && len(*recv) == 0 {
				*recv = []*Withdrawal{}
			}
			break
		}
		*recv = append(*recv, &withdrawal)
	}
	if !errors.Is(err, rlp.EOL) {
		return err
	}
	// end of Withdrawals
	if err = s.ListEnd(); err != nil {
		return err
	}
	return nil
}
