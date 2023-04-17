package state

import (
	"errors"
	"fmt"

	"github.com/umbracle/ethgo/abi"
)

var (
	// ErrInvalidBatchHeader indicates the batch header is invalid
	ErrInvalidBatchHeader = errors.New("invalid batch header")
	// ErrUnexpectedBatch indicates that the batch is unexpected
	ErrUnexpectedBatch = errors.New("unexpected batch")
	// ErrStateNotSynchronized indicates the state database may be empty
	ErrStateNotSynchronized = errors.New("state not synchronized")
	// ErrNotFound indicates an object has not been found for the search criteria used
	ErrNotFound = errors.New("object not found")
	// ErrNilDBTransaction indicates the db transaction has not been properly initialized
	ErrNilDBTransaction = errors.New("database transaction not properly initialized")
	// ErrAlreadyInitializedDBTransaction indicates the db transaction was already initialized
	ErrAlreadyInitializedDBTransaction = errors.New("database transaction already initialized")
	// ErrNotEnoughIntrinsicGas indicates the gas is not enough to cover the intrinsic gas cost
	ErrNotEnoughIntrinsicGas = fmt.Errorf("not enough gas supplied for intrinsic gas costs")
	// ErrParsingExecutorTrace indicates an error occurred while parsing the executor trace
	ErrParsingExecutorTrace = fmt.Errorf("error while parsing executor trace")
	// ErrInvalidBatchNumber indicates the provided batch number is not the latest in db
	ErrInvalidBatchNumber = errors.New("provided batch number is not latest")
	// ErrLastBatchShouldBeClosed indicates that last batch needs to be closed before adding a new one
	ErrLastBatchShouldBeClosed = errors.New("last batch needs to be closed before adding a new one")
	// ErrBatchAlreadyClosed indicates that batch is already closed
	ErrBatchAlreadyClosed = errors.New("batch is already closed")
	// ErrClosingBatchWithoutTxs indicates that the batch attempted to close does not have txs.
	ErrClosingBatchWithoutTxs = errors.New("can not close a batch without transactions")
	// ErrTimestampGE indicates that timestamp needs to be greater or equal
	ErrTimestampGE = errors.New("timestamp needs to be greater or equal")
	// ErrDBTxNil indicates that the method requires a dbTx that is not nil
	ErrDBTxNil = errors.New("the method requires a dbTx that is not nil")
	// ErrExistingTxGreaterThanProcessedTx indicates that we have more txs stored
	// in db than the txs we want to process.
	ErrExistingTxGreaterThanProcessedTx = errors.New("there are more transactions in the database than in the processed transaction set")
	// ErrOutOfOrderProcessedTx indicates the the processed transactions of an
	// ongoing batch are not in the same order as the transactions stored in the
	// database for the same batch.
	ErrOutOfOrderProcessedTx = errors.New("the processed transactions are not in the same order as the stored transactions")
	// ErrInsufficientFunds is returned if the total cost of executing a transaction
	// is higher than the balance of the user's account.
	ErrInsufficientFunds = errors.New("insufficient funds for gas * price + value")
	// ErrExecutorNil indicates that the method requires an executor that is not nil
	ErrExecutorNil = errors.New("the method requires an executor that is not nil")
	// ErrStateTreeNil indicates that the method requires a state tree that is not nil
	ErrStateTreeNil = errors.New("the method requires a state tree that is not nil")
	// ErrUnsupportedDuration is returned if the provided unit for a time
	// interval is not supported by our conversion mechanism.
	ErrUnsupportedDuration = errors.New("unsupported time duration")
	// ErrInvalidData is the error when the raw txs is unexpected
	ErrInvalidData = errors.New("invalid data")

	zkCounterErrPrefix = "ZKCounter: "
)

func constructErrorFromRevert(err error, returnValue []byte) error {
	revertErrMsg, unpackErr := abi.UnpackRevertError(returnValue)
	if unpackErr != nil {
		return err
	}

	return fmt.Errorf("%w: %s", err, revertErrMsg)
}

// GetZKCounterError returns the error associated with the zkCounter
func GetZKCounterError(name string) error {
	return errors.New(zkCounterErrPrefix + name)
}
