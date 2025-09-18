package services

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/beacon/synced_data/mock_services"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/das"
	das_mock "github.com/erigontech/erigon/cl/das/mock_services"
	das_state_mock "github.com/erigontech/erigon/cl/das/state/mock_services"
	blob_storage_mock "github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	forkchoice_mock "github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
)

var (
	testSlot          = uint64(321)
	testEpoch         = uint64(10)
	testSlotsPerEpoch = uint64(32)
	testParentRoot    = common.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
)

type dataColumnSidecarTestSuite struct {
	suite.Suite
	gomockCtrl               *gomock.Controller
	mockForkChoice           *forkchoice_mock.ForkChoiceStorageMock
	mockSyncedData           *mock_services.MockSyncedData
	mockEthClock             *eth_clock.MockEthereumClock
	mockColumnSidecarStorage *blob_storage_mock.MockDataColumnStorage
	mockPeerDas              *das_mock.MockPeerDas
	mockPeerDasStateReader   *das_state_mock.MockPeerDasStateReader
	dataColumnSidecarService DataColumnSidecarService
	beaconConfig             *clparams.BeaconChainConfig
	mockFuncs                *mockFuncs
}

func (t *dataColumnSidecarTestSuite) SetupTest() {
	t.gomockCtrl = gomock.NewController(t.T())
	t.mockForkChoice = forkchoice_mock.NewForkChoiceStorageMock(t.T())
	t.mockEthClock = eth_clock.NewMockEthereumClock(t.gomockCtrl)
	t.mockColumnSidecarStorage = blob_storage_mock.NewMockDataColumnStorage(t.gomockCtrl)
	t.mockPeerDas = das_mock.NewMockPeerDas(t.gomockCtrl)
	t.mockPeerDasStateReader = das_state_mock.NewMockPeerDasStateReader(t.gomockCtrl)
	t.mockSyncedData = mock_services.NewMockSyncedData(t.gomockCtrl)
	t.mockForkChoice.MockPeerDas = t.mockPeerDas

	// Set up default mock behavior for PeerDas
	t.mockPeerDas.EXPECT().IsArchivedMode().Return(false).AnyTimes()
	t.mockPeerDas.EXPECT().StateReader().Return(t.mockPeerDasStateReader).AnyTimes()

	// Set up default mock behavior for PeerDasStateReader
	t.mockPeerDasStateReader.EXPECT().GetMyCustodyColumns().Return(map[uint64]bool{0: true, 1: true, 2: true, 3: true}, nil).AnyTimes()

	t.beaconConfig = &clparams.BeaconChainConfig{
		SlotsPerEpoch:    testSlotsPerEpoch,
		NumberOfColumns:  4,
		ElectraForkEpoch: 100000,
	}

	t.dataColumnSidecarService = NewDataColumnSidecarService(
		t.beaconConfig,
		t.mockEthClock,
		t.mockForkChoice,
		t.mockSyncedData,
		t.mockColumnSidecarStorage,
		beaconevents.NewEventEmitter(),
	)

	t.mockFuncs = &mockFuncs{
		ctrl: t.gomockCtrl,
	}
}

func (t *dataColumnSidecarTestSuite) TearDownTest() {
	t.gomockCtrl.Finish()
	// reset mock functions
	verifyDataColumnSidecar = das.VerifyDataColumnSidecar
	verifyDataColumnSidecarInclusionProof = das.VerifyDataColumnSidecarInclusionProof
	verifyDataColumnSidecarKZGProofs = das.VerifyDataColumnSidecarKZGProofs
	blsVerify = bls.Verify
	computeSubnetForDataColumnSidecar = das.ComputeSubnetForDataColumnSidecar
}

func createMockDataColumnSidecar(slot uint64, index uint64) *cltypes.DataColumnSidecar {
	// Create a minimal but valid data column sidecar
	sidecar := &cltypes.DataColumnSidecar{
		Index: index,
		SignedBlockHeader: &cltypes.SignedBeaconBlockHeader{
			Header: &cltypes.BeaconBlockHeader{
				Slot:          slot,
				ParentRoot:    testParentRoot,
				ProposerIndex: 1,
				BodyRoot:      common.Hash{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			},
			Signature: [96]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95},
		},
	}

	// Initialize the sidecar with proper data structures manually
	// Use reasonable defaults instead of calling GetBeaconConfig()
	sidecar.Column = solid.NewStaticListSSZ[*cltypes.Cell](4, 32)
	sidecar.KzgCommitments = solid.NewStaticListSSZ[*cltypes.KZGCommitment](4, 48)
	sidecar.KzgProofs = solid.NewStaticListSSZ[*cltypes.KZGProof](4, 48)
	sidecar.KzgCommitmentsInclusionProof = solid.NewHashVector(32)

	// Add some mock data to make it pass validation
	// Add a mock KZG commitment
	commitment := &cltypes.KZGCommitment{}
	copy(commitment[:], []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48})
	sidecar.KzgCommitments.Append(commitment)

	// Add a mock KZG proof
	proof := &cltypes.KZGProof{}
	copy(proof[:], []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48})
	sidecar.KzgProofs.Append(proof)

	// Add a mock cell
	cell := &cltypes.Cell{}
	copy(cell[:], []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32})
	sidecar.Column.Append(cell)

	return sidecar
}

func TestDataColumnSidecarService(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	suite.Run(t, &dataColumnSidecarTestSuite{})
}

// TestProcessMessage_WhenSyncing_ReturnsErrIgnore tests that the service returns ErrIgnore when syncing
func (t *dataColumnSidecarTestSuite) TestProcessMessage_WhenSyncing_ReturnsErrIgnore() {
	// Setup
	t.mockSyncedData.EXPECT().Syncing().Return(true)

	// Execute
	sidecar := createMockDataColumnSidecar(testSlot, 0)
	err := t.dataColumnSidecarService.ProcessMessage(context.Background(), nil, sidecar)

	// Assert
	t.Equal(ErrIgnore, err)
}

// TestProcessMessage_WhenAlreadySeen_ReturnsErrIgnore tests that the service returns ErrIgnore for duplicate sidecars
func (t *dataColumnSidecarTestSuite) TestProcessMessage_WhenAlreadySeen_ReturnsErrIgnore() {
	// Setup mock functions
	verifyDataColumnSidecar = t.mockFuncs.VerifyDataColumnSidecar
	verifyDataColumnSidecarInclusionProof = t.mockFuncs.VerifyDataColumnSidecarInclusionProof
	verifyDataColumnSidecarKZGProofs = t.mockFuncs.VerifyDataColumnSidecarKZGProofs
	blsVerify = t.mockFuncs.BlsVerify

	t.mockSyncedData.EXPECT().Syncing().Return(false).Times(2)
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecar", gomock.Any()).Return(true).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarInclusionProof", gomock.Any()).Return(true).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarKZGProofs", gomock.Any()).Return(true).AnyTimes()

	// Mock ViewHeadState for both calls
	t.mockSyncedData.EXPECT().ViewHeadState(gomock.Any()).DoAndReturn(func(fn synced_data.ViewHeadStateFn) error {
		return nil
	}).Return(nil).Times(1)
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "BlsVerify", gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).Times(1)

	// Mock storage for first call
	t.mockColumnSidecarStorage.EXPECT().WriteColumnSidecars(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)
	t.mockForkChoice.Headers[testParentRoot] = &cltypes.BeaconBlockHeader{}

	// First call should succeed
	sidecar := createMockDataColumnSidecar(testSlot, 0)
	err := t.dataColumnSidecarService.ProcessMessage(context.Background(), nil, sidecar)
	t.NoError(err)

	// Second call with same sidecar should return ErrIgnore
	err = t.dataColumnSidecarService.ProcessMessage(context.Background(), nil, sidecar)
	t.Equal(ErrIgnore, err)
}

// TestProcessMessage_WhenInvalidDataColumnSidecar_ReturnsError tests validation failure
func (t *dataColumnSidecarTestSuite) TestProcessMessage_WhenInvalidDataColumnSidecar_ReturnsError() {
	// Setup mock functions
	verifyDataColumnSidecar = t.mockFuncs.VerifyDataColumnSidecar

	// Setup
	t.mockSyncedData.EXPECT().Syncing().Return(false)
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecar", gomock.Any()).Return(false).AnyTimes()

	// Execute
	sidecar := createMockDataColumnSidecar(testSlot, 0)
	err := t.dataColumnSidecarService.ProcessMessage(context.Background(), nil, sidecar)

	// Assert
	t.Error(err)
	t.Contains(err.Error(), "invalid data column sidecar")
}

// TestProcessMessage_WhenIncorrectSubnet_ReturnsError tests subnet validation
func (t *dataColumnSidecarTestSuite) TestProcessMessage_WhenIncorrectSubnet_ReturnsError() {
	// Setup mock functions
	incorrectSubnet := uint64(987654321)
	computeSubnetForDataColumnSidecar = func(index uint64) uint64 { return 1234 } // Return different subnet
	verifyDataColumnSidecar = t.mockFuncs.VerifyDataColumnSidecar
	blsVerify = t.mockFuncs.BlsVerify

	// Setup
	t.mockSyncedData.EXPECT().Syncing().Return(false)
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecar", gomock.Any()).Return(true).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "BlsVerify", gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()

	// Mock fork choice methods to avoid panic
	t.mockForkChoice.Headers[testParentRoot] = &cltypes.BeaconBlockHeader{
		//Slot: testSlot - 1,
	}
	t.mockForkChoice.FinalizedCheckpointVal = solid.Checkpoint{
		//Epoch: (testSlot - 100) / 32,
		//Root:  [32]byte{1},
	}
	//t.mockForkChoice.Ancestors[(testSlot-100)/32*32] = [32]byte{1}

	// Mock ViewHeadState to avoid panic
	t.mockSyncedData.EXPECT().ViewHeadState(gomock.Any()).DoAndReturn(func(fn synced_data.ViewHeadStateFn) error {
		return nil
	}).Return(nil).AnyTimes()

	// Execute
	sidecar := createMockDataColumnSidecar(testSlot, 0)
	err := t.dataColumnSidecarService.ProcessMessage(context.Background(), &incorrectSubnet, sidecar)

	// Assert
	t.Error(err)
	t.Contains(err.Error(), "incorrect subnet")
}

// TestProcessMessage_WhenFutureSlot_ReturnsErrIgnore tests future slot handling
func (t *dataColumnSidecarTestSuite) TestProcessMessage_WhenFutureSlot_ReturnsErrIgnore() {
	// Setup mock functions
	verifyDataColumnSidecar = t.mockFuncs.VerifyDataColumnSidecar

	// Setup
	t.mockSyncedData.EXPECT().Syncing().Return(false)
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecar", gomock.Any()).Return(true).AnyTimes()
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot)
	t.mockEthClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(testSlot + 100).Return(false)

	// Execute
	sidecar := createMockDataColumnSidecar(testSlot+100, 0)
	err := t.dataColumnSidecarService.ProcessMessage(context.Background(), nil, sidecar)

	// Assert
	t.Equal(ErrIgnore, err)
}

// TestProcessMessage_WhenSlotTooOld_ReturnsErrIgnore tests finalized slot validation
func (t *dataColumnSidecarTestSuite) TestProcessMessage_WhenSlotTooOld_ReturnsErrIgnore() {
	// Setup mock functions
	verifyDataColumnSidecar = t.mockFuncs.VerifyDataColumnSidecar

	// Setup
	t.mockSyncedData.EXPECT().Syncing().Return(false)
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecar", gomock.Any()).Return(true).AnyTimes()
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()

	// Mock fork choice to return a finalized slot that makes the current slot too old
	t.mockForkChoice.FinalizedSlotVal = testSlot + 100

	// Mock GetHeader to return a valid parent header
	t.mockForkChoice.Headers[testParentRoot] = &cltypes.BeaconBlockHeader{
		//Slot: testSlot - 1,
	}

	// Mock FinalizedCheckpoint and Ancestor methods
	t.mockForkChoice.FinalizedCheckpointVal = solid.Checkpoint{
		//Epoch: (testSlot + 100) / 32,
		//Root:  [32]byte{1},
	}
	//t.mockForkChoice.Ancestors[(testSlot+100)/32*32] = [32]byte{1}

	// Execute
	sidecar := createMockDataColumnSidecar(testSlot, 0)
	err := t.dataColumnSidecarService.ProcessMessage(context.Background(), nil, sidecar)

	// Assert
	t.Equal(ErrIgnore, err)
}

// TestProcessMessage_WhenInvalidInclusionProof_ReturnsError tests inclusion proof validation
func (t *dataColumnSidecarTestSuite) TestProcessMessage_WhenInvalidInclusionProof_ReturnsError() {
	// Setup mock functions
	verifyDataColumnSidecar = t.mockFuncs.VerifyDataColumnSidecar
	verifyDataColumnSidecarInclusionProof = t.mockFuncs.VerifyDataColumnSidecarInclusionProof
	blsVerify = t.mockFuncs.BlsVerify

	// Setup
	t.mockSyncedData.EXPECT().Syncing().Return(false)
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecar", gomock.Any()).Return(true).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarInclusionProof", gomock.Any()).Return(false).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "BlsVerify", gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()

	// Mock fork choice methods to avoid panic
	t.mockForkChoice.Headers[testParentRoot] = &cltypes.BeaconBlockHeader{
		//Slot: testSlot - 1,
	}
	t.mockForkChoice.FinalizedCheckpointVal = solid.Checkpoint{
		//Epoch: (testSlot - 100) / 32,
		//Root:  [32]byte{1},
	}
	//t.mockForkChoice.Ancestors[(testSlot-100)/32*32] = [32]byte{1}

	// Mock ViewHeadState to avoid panic
	t.mockSyncedData.EXPECT().ViewHeadState(gomock.Any()).DoAndReturn(func(fn synced_data.ViewHeadStateFn) error {
		return nil
	}).Return(nil)

	// Execute
	sidecar := createMockDataColumnSidecar(testSlot, 0)
	err := t.dataColumnSidecarService.ProcessMessage(context.Background(), nil, sidecar)

	// Assert
	t.Error(err)
	t.Contains(err.Error(), "invalid inclusion proof")
}

// TestProcessMessage_WhenInvalidKZGProofs_ReturnsError tests KZG proof validation
func (t *dataColumnSidecarTestSuite) TestProcessMessage_WhenInvalidKZGProofs_ReturnsError() {
	// Setup mock functions
	verifyDataColumnSidecar = t.mockFuncs.VerifyDataColumnSidecar
	verifyDataColumnSidecarInclusionProof = t.mockFuncs.VerifyDataColumnSidecarInclusionProof
	verifyDataColumnSidecarKZGProofs = t.mockFuncs.VerifyDataColumnSidecarKZGProofs
	blsVerify = t.mockFuncs.BlsVerify

	// Setup
	t.mockSyncedData.EXPECT().Syncing().Return(false)
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecar", gomock.Any()).Return(true).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarInclusionProof", gomock.Any()).Return(true).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarKZGProofs", gomock.Any()).Return(false).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "BlsVerify", gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()

	// Mock fork choice methods to avoid panic
	t.mockForkChoice.Headers[testParentRoot] = &cltypes.BeaconBlockHeader{
		//Slot: testSlot - 1,
	}
	t.mockForkChoice.FinalizedCheckpointVal = solid.Checkpoint{
		//Epoch: (testSlot - 100) / 32,
		//Root:  [32]byte{1},
	}
	//t.mockForkChoice.Ancestors[(testSlot-100)/32*32] = [32]byte{1}

	// Mock ViewHeadState to avoid panic
	t.mockSyncedData.EXPECT().ViewHeadState(gomock.Any()).DoAndReturn(func(fn synced_data.ViewHeadStateFn) error {
		return nil
	}).Return(nil)

	// Execute
	sidecar := createMockDataColumnSidecar(testSlot, 0)
	err := t.dataColumnSidecarService.ProcessMessage(context.Background(), nil, sidecar)

	// Assert
	t.Error(err)
	t.Contains(err.Error(), "invalid kzg proofs")
}

// TestProcessMessage_WhenValidSidecar_StoresSuccessfully tests successful sidecar processing
func (t *dataColumnSidecarTestSuite) TestProcessMessage_WhenValidSidecar_StoresSuccessfully() {
	// Setup mock functions
	verifyDataColumnSidecar = t.mockFuncs.VerifyDataColumnSidecar
	verifyDataColumnSidecarInclusionProof = t.mockFuncs.VerifyDataColumnSidecarInclusionProof
	verifyDataColumnSidecarKZGProofs = t.mockFuncs.VerifyDataColumnSidecarKZGProofs
	blsVerify = t.mockFuncs.BlsVerify

	// Setup
	t.mockSyncedData.EXPECT().Syncing().Return(false)
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()
	t.mockEthClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(false).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecar", gomock.Any()).Return(true).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarInclusionProof", gomock.Any()).Return(true).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarKZGProofs", gomock.Any()).Return(true).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "BlsVerify", gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()
	t.mockForkChoice.Headers[testParentRoot] = &cltypes.BeaconBlockHeader{}

	// Mock synced data for proposer signature verification
	t.mockSyncedData.EXPECT().ViewHeadState(gomock.Any()).DoAndReturn(func(fn synced_data.ViewHeadStateFn) error {
		return nil
	}).Return(nil).Times(1)
	t.mockColumnSidecarStorage.EXPECT().WriteColumnSidecars(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)

	// Execute
	sidecar := createMockDataColumnSidecar(testSlot, 0)
	err := t.dataColumnSidecarService.ProcessMessage(context.Background(), nil, sidecar)

	// Assert
	t.NoError(err)
}

// TestProcessMessage_WhenStorageFails_ReturnsError tests storage failure handling
func (t *dataColumnSidecarTestSuite) TestProcessMessage_WhenStorageFails_ReturnsError() {
	// Setup mock functions
	verifyDataColumnSidecar = t.mockFuncs.VerifyDataColumnSidecar
	verifyDataColumnSidecarInclusionProof = t.mockFuncs.VerifyDataColumnSidecarInclusionProof
	verifyDataColumnSidecarKZGProofs = t.mockFuncs.VerifyDataColumnSidecarKZGProofs
	blsVerify = t.mockFuncs.BlsVerify

	// Setup
	t.mockSyncedData.EXPECT().Syncing().Return(false)
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecar", gomock.Any()).Return(true).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarInclusionProof", gomock.Any()).Return(true).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarKZGProofs", gomock.Any()).Return(true).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "BlsVerify", gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()
	t.mockForkChoice.Headers[testParentRoot] = &cltypes.BeaconBlockHeader{}

	// Mock ViewHeadState to avoid panic
	t.mockSyncedData.EXPECT().ViewHeadState(gomock.Any()).DoAndReturn(func(fn synced_data.ViewHeadStateFn) error {
		return nil
	}).Return(nil)

	t.mockColumnSidecarStorage.EXPECT().WriteColumnSidecars(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("storage error"))

	// Execute
	sidecar := createMockDataColumnSidecar(testSlot, 0)
	err := t.dataColumnSidecarService.ProcessMessage(context.Background(), nil, sidecar)

	// Assert
	t.Error(err)
	t.Contains(err.Error(), "failed to write data column sidecar")
}
