package services

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/beacon/synced_data/mock_services"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/das"
	das_mock "github.com/erigontech/erigon/cl/das/mock_services"
	das_state_mock "github.com/erigontech/erigon/cl/das/state/mock_services"
	"github.com/erigontech/erigon/cl/gossip"
	blob_storage_mock "github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	forkchoice_mock "github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	gossipmock "github.com/erigontech/erigon/cl/phase1/network/gossip/mock_services"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
)

var initOnce sync.Once

func init() {
	// Initialize global config for tests (only once across all test packages)
	initOnce.Do(func() {
		defer func() {
			// Recover from panic if config already initialized by another package
			_ = recover()
		}()
		cfg := &clparams.BeaconChainConfig{
			MaxBlobCommittmentsPerBlock: 6,
			NumberOfColumns:             128,
			SlotsPerEpoch:               32,
		}
		clparams.InitGlobalStaticConfig(cfg)
	})
}

var (
	testSlot          = uint64(321)
	testEpoch         = uint64(10)
	testSlotsPerEpoch = uint64(32)
	testParentRoot    = common.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
	testFinalizedRoot = common.Hash{0xf1}
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
	emitters                 *beaconevents.EventEmitter
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
	// Like ForkChoiceStore, fork choice starts from an anchor block that is the finalized checkpoint.
	t.mockForkChoice.FinalizedCheckpointVal = solid.Checkpoint{Root: testFinalizedRoot}
	t.mockForkChoice.Ancestors[0] = forkchoice.ForkChoiceNode{Root: testFinalizedRoot}

	// Set up default mock behavior for PeerDas
	t.mockPeerDas.EXPECT().IsArchivedMode().Return(false).AnyTimes()
	t.mockPeerDas.EXPECT().StateReader().Return(t.mockPeerDasStateReader).AnyTimes()

	// Set up default mock behavior for PeerDasStateReader
	t.mockPeerDasStateReader.EXPECT().GetMyCustodyColumns().Return(map[uint64]bool{0: true, 1: true, 2: true, 3: true}, nil).AnyTimes()

	t.beaconConfig = &clparams.BeaconChainConfig{
		SlotsPerEpoch:           testSlotsPerEpoch,
		NumberOfColumns:         4,
		ElectraForkEpoch:        0,
		FuluForkEpoch:           0,
		GloasForkEpoch:          100000,
		MaxBlobsPerBlockElectra: 9,
	}

	t.emitters = beaconevents.NewEventEmitter()
	t.dataColumnSidecarService = NewDataColumnSidecarService(
		canceledPendingQueueContext(t.T()),
		t.beaconConfig,
		t.mockEthClock,
		t.mockForkChoice,
		t.mockSyncedData,
		t.mockColumnSidecarStorage,
		t.emitters,
		nil,
	)
	// createMockDataColumnSidecar proposes every block with validator 1.
	t.setExpectedProposer(1, nil)

	t.mockFuncs = &mockFuncs{
		ctrl: t.gomockCtrl,
	}
}

// setExpectedProposer stubs the expected-proposer read for blocks of testSlot on testParentRoot, and checks that the
// service asks for that parent, its slot and the sidecar's slot.
func (t *dataColumnSidecarTestSuite) setExpectedProposer(proposer uint64, err error) {
	t.dataColumnSidecarService.(*dataColumnSidecarService).expectedProposer = func(_ context.Context, parentRoot common.Hash, parentSlot, slot uint64) (uint64, error) {
		t.Equal(testParentRoot, parentRoot)
		if parent, ok := t.mockForkChoice.Headers[testParentRoot]; ok {
			t.Equal(parent.Slot, parentSlot)
		}
		t.Equal(testSlot, slot)
		return proposer, err
	}
}

func (t *dataColumnSidecarTestSuite) TearDownTest() {
	t.gomockCtrl.Finish()
	// reset mock functions
	verifyDataColumnSidecar = das.VerifyDataColumnSidecar
	verifyDataColumnSidecarInclusionProof = das.VerifyDataColumnSidecarInclusionProof
	verifyDataColumnSidecarKZGProofs = das.VerifyDataColumnSidecarKZGProofs
	verifyDataColumnSidecarWithCommitments = das.VerifyDataColumnSidecarWithCommitments
	verifyDataColumnSidecarKZGProofsWithCommitments = das.VerifyDataColumnSidecarKZGProofsWithCommitments
	blsVerify = bls.Verify
	computeSubnetForDataColumnSidecar = das.ComputeSubnetForDataColumnSidecar
}

func (t *dataColumnSidecarTestSuite) activateGloas() {
	t.beaconConfig.ElectraForkEpoch = 0
	t.beaconConfig.FuluForkEpoch = 0
	t.beaconConfig.GloasForkEpoch = testEpoch
}

func createMockDataColumnSidecar(slot uint64, index uint64, version clparams.StateVersion) *cltypes.DataColumnSidecar {
	// Create a minimal but valid data column sidecar
	sidecar := cltypes.NewDataColumnSidecarWithVersion(version)
	sidecar.Index = index
	sidecar.SignedBlockHeader = &cltypes.SignedBeaconBlockHeader{
		Header: &cltypes.BeaconBlockHeader{
			Slot:          slot,
			ParentRoot:    testParentRoot,
			ProposerIndex: 1,
			BodyRoot:      common.Hash{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		},
		Signature: [96]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95},
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
	suite.Run(t, &dataColumnSidecarTestSuite{})
}

// TestProcessMessage_WhenSyncing_ReturnsErrIgnore tests that the service returns ErrIgnore when syncing
func (t *dataColumnSidecarTestSuite) TestProcessMessage_WhenSyncing_ReturnsErrIgnore() {
	// Setup
	t.mockSyncedData.EXPECT().Syncing().Return(true)

	// Execute
	sidecar := createMockDataColumnSidecar(testSlot, 0, clparams.FuluVersion)
	err := t.dataColumnSidecarService.ProcessMessage(context.Background(), nil, sidecar)

	// Assert
	t.Equal(ErrIgnore, err)
}

func (t *dataColumnSidecarTestSuite) TestProcessMessage_IgnoresPreGloasSchemaAtGloasSlotBeforeStorage() {
	t.activateGloas()
	sidecar := createMockDataColumnSidecar(testSlot, 0, clparams.FuluVersion)
	t.False(t.beaconConfig.ForkSchemaMatchesSlot(sidecar.SignedBlockHeader.Header.Slot, sidecar.Version()))

	t.assertProcessMessageIgnoredBeforeStorage(sidecar)
}

func (t *dataColumnSidecarTestSuite) TestProcessMessage_IgnoresExactForkVersionMismatchBeforeStorage() {
	t.beaconConfig.ElectraForkEpoch = 0
	t.beaconConfig.FuluForkEpoch = 0
	t.beaconConfig.GloasForkEpoch = testEpoch + 1
	sidecar := createMockDataColumnSidecar(testSlot, 0, clparams.ElectraVersion)
	t.Equal(clparams.FuluVersion, t.beaconConfig.GetCurrentStateVersion(testEpoch))
	t.Equal(clparams.ElectraVersion, sidecar.Version())
	t.True(t.beaconConfig.ForkSchemaMatchesSlot(sidecar.SignedBlockHeader.Header.Slot, sidecar.Version()))

	t.assertProcessMessageIgnoredBeforeStorage(sidecar)
}

func (t *dataColumnSidecarTestSuite) assertProcessMessageIgnoredBeforeStorage(sidecar *cltypes.DataColumnSidecar) {
	t.T().Helper()
	verifyDataColumnSidecar = t.mockFuncs.VerifyDataColumnSidecar
	verifyDataColumnSidecarInclusionProof = t.mockFuncs.VerifyDataColumnSidecarInclusionProof
	verifyDataColumnSidecarKZGProofs = t.mockFuncs.VerifyDataColumnSidecarKZGProofs
	blsVerify = t.mockFuncs.BlsVerify
	t.mockSyncedData.EXPECT().Syncing().Return(false)
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecar", gomock.Any()).Return(true).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarInclusionProof", gomock.Any()).Return(true).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarKZGProofs", gomock.Any()).Return(true).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "BlsVerify", gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()
	t.mockSyncedData.EXPECT().ViewHeadState(gomock.Any()).Return(nil).AnyTimes()
	t.mockForkChoice.Headers[testParentRoot] = &cltypes.BeaconBlockHeader{}
	t.mockColumnSidecarStorage.EXPECT().WriteColumnSidecars(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	t.mockPeerDas.EXPECT().TryScheduleRecover(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	err := t.dataColumnSidecarService.ProcessMessage(t.T().Context(), nil, sidecar)

	t.ErrorIs(err, ErrIgnore)
	service := t.dataColumnSidecarService.(*dataColumnSidecarService)
	t.Zero(service.pendingGloasSidecars.count.Load())
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

	t.mockPeerDas.EXPECT().TryScheduleRecover(gomock.Any(), gomock.Any()).Return(nil).Times(1)

	// First call should succeed
	sidecar := createMockDataColumnSidecar(testSlot, 0, clparams.FuluVersion)
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
	sidecar := createMockDataColumnSidecar(testSlot, 0, clparams.FuluVersion)
	err := t.dataColumnSidecarService.ProcessMessage(context.Background(), nil, sidecar)

	// Assert
	t.Error(err)
	t.Contains(err.Error(), "invalid data column sidecar")
}

func (t *dataColumnSidecarTestSuite) TestProcessMessage_WhenIncorrectSubnet_ReturnsError() {
	incorrectSubnet := uint64(987654321)
	computeSubnetForDataColumnSidecar = func(index uint64) uint64 { return 1234 }
	verifyDataColumnSidecar = t.mockFuncs.VerifyDataColumnSidecar

	t.mockSyncedData.EXPECT().Syncing().Return(false)
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecar", gomock.Any()).Return(true)

	sidecar := createMockDataColumnSidecar(testSlot, 0, clparams.FuluVersion)
	err := t.dataColumnSidecarService.ProcessMessage(context.Background(), &incorrectSubnet, sidecar)

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
	sidecar := createMockDataColumnSidecar(testSlot+100, 0, clparams.FuluVersion)
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

	// Mock GetHeader to return a valid parent header
	t.mockForkChoice.Headers[testParentRoot] = &cltypes.BeaconBlockHeader{
		// Slot: testSlot - 1,
	}

	// The finalized checkpoint starts after the sidecar's slot, so the sidecar is too old
	t.mockForkChoice.FinalizedCheckpointVal = solid.Checkpoint{Epoch: (testSlot + 100) / testSlotsPerEpoch}

	// Execute
	sidecar := createMockDataColumnSidecar(testSlot, 0, clparams.FuluVersion)
	err := t.dataColumnSidecarService.ProcessMessage(context.Background(), nil, sidecar)

	// Assert
	t.Equal(ErrIgnore, err)
}

// TestProcessMessage_WhenSlotIsFinalizedEpochStart_ReturnsErrIgnore checks the boundary: a sidecar at
// compute_start_slot_at_epoch(finalized_checkpoint.epoch) is not greater than the finalized slot.
func (t *dataColumnSidecarTestSuite) TestProcessMessage_WhenSlotIsFinalizedEpochStart_ReturnsErrIgnore() {
	verifyDataColumnSidecar = t.mockFuncs.VerifyDataColumnSidecar
	t.mockSyncedData.EXPECT().Syncing().Return(false)
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecar", gomock.Any()).Return(true).AnyTimes()
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()
	finalizedEpoch := testSlot / testSlotsPerEpoch
	t.mockForkChoice.FinalizedCheckpointVal = solid.Checkpoint{Epoch: finalizedEpoch}

	sidecar := createMockDataColumnSidecar(finalizedEpoch*testSlotsPerEpoch, 0, clparams.FuluVersion)
	err := t.dataColumnSidecarService.ProcessMessage(context.Background(), nil, sidecar)

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
		// Slot: testSlot - 1,
	}

	// Mock ViewHeadState to avoid panic
	t.mockSyncedData.EXPECT().ViewHeadState(gomock.Any()).DoAndReturn(func(fn synced_data.ViewHeadStateFn) error {
		return nil
	}).Return(nil)

	// Execute
	sidecar := createMockDataColumnSidecar(testSlot, 0, clparams.FuluVersion)
	err := t.dataColumnSidecarService.ProcessMessage(context.Background(), nil, sidecar)

	// Assert
	t.Error(err)
	t.Contains(err.Error(), "invalid inclusion proof")
}

// TestProcessMessage_WhenSlotAfterFinalizedEpochStart_PassesFinalityCheck checks that only sidecars at or
// before compute_start_slot_at_epoch(finalized_checkpoint.epoch) are ignored, not the whole finalized epoch.
func (t *dataColumnSidecarTestSuite) TestProcessMessage_WhenSlotAfterFinalizedEpochStart_PassesFinalityCheck() {
	t.setupFinalizedAncestorTest(testFinalizedRoot)
	finalizedEpoch := testSlot / testSlotsPerEpoch
	t.Require().Greater(testSlot, finalizedEpoch*testSlotsPerEpoch)
	t.mockForkChoice.Ancestors[finalizedEpoch*testSlotsPerEpoch] = forkchoice.ForkChoiceNode{Root: testFinalizedRoot}
	t.mockForkChoice.FinalizedSlotVal = finalizedEpoch*testSlotsPerEpoch + testSlotsPerEpoch - 1

	sidecar := createMockDataColumnSidecar(testSlot, 0, clparams.FuluVersion)
	err := t.dataColumnSidecarService.ProcessMessage(context.Background(), nil, sidecar)

	// The sidecar gets past the finality check and fails on the forced inclusion proof error.
	t.Error(err)
	t.Contains(err.Error(), "invalid inclusion proof")
}

// TestProcessMessage_WhenAnchorAfterFinalizedEpochStart_ChecksAncestorAtAnchor checks that, as in block gossip,
// the finalized-ancestor lookup starts at the anchor slot when the node's history begins after the start slot
// of the finalized checkpoint epoch.
func (t *dataColumnSidecarTestSuite) TestProcessMessage_WhenAnchorAfterFinalizedEpochStart_ChecksAncestorAtAnchor() {
	finalizedEpoch := testSlot/testSlotsPerEpoch - 1
	checkpointRoot := common.Hash{0x0f}
	t.setupFinalizedAncestorTest(checkpointRoot)
	t.mockForkChoice.FinalizedCheckpointVal = solid.Checkpoint{Epoch: finalizedEpoch, Root: checkpointRoot}
	// Fork choice starts mid-epoch, so the epoch start slot is not in its history.
	anchorSlot := finalizedEpoch*testSlotsPerEpoch + 5
	t.mockForkChoice.AnchorSlotVal = anchorSlot
	t.mockForkChoice.Ancestors[anchorSlot] = forkchoice.ForkChoiceNode{Root: checkpointRoot}

	sidecar := createMockDataColumnSidecar(testSlot, 0, clparams.FuluVersion)
	err := t.dataColumnSidecarService.ProcessMessage(context.Background(), nil, sidecar)

	// The sidecar gets past the ancestor check and fails on the forced inclusion proof error.
	t.Error(err)
	t.Contains(err.Error(), "invalid inclusion proof")
}

// setupFinalizedAncestorTest prepares a Fulu sidecar that reaches the finalized-ancestor check.
func (t *dataColumnSidecarTestSuite) setupFinalizedAncestorTest(checkpointRoot common.Hash) {
	verifyDataColumnSidecar = t.mockFuncs.VerifyDataColumnSidecar
	verifyDataColumnSidecarInclusionProof = t.mockFuncs.VerifyDataColumnSidecarInclusionProof
	blsVerify = t.mockFuncs.BlsVerify
	t.mockSyncedData.EXPECT().Syncing().Return(false)
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecar", gomock.Any()).Return(true).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarInclusionProof", gomock.Any()).Return(false).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "BlsVerify", gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()
	t.mockForkChoice.Headers[testParentRoot] = &cltypes.BeaconBlockHeader{}
	t.mockForkChoice.FinalizedCheckpointVal = solid.Checkpoint{Epoch: testSlot / testSlotsPerEpoch, Root: checkpointRoot}
	t.mockSyncedData.EXPECT().ViewHeadState(gomock.Any()).DoAndReturn(func(fn synced_data.ViewHeadStateFn) error {
		return nil
	}).Return(nil).AnyTimes()
}

// TestProcessMessage_WhenFinalizedCheckpointIsNotAncestor_ReturnsError checks that a known ancestor other than
// the finalized checkpoint still rejects the sidecar.
func (t *dataColumnSidecarTestSuite) TestProcessMessage_WhenFinalizedCheckpointIsNotAncestor_ReturnsError() {
	t.setupFinalizedAncestorTest(common.Hash{0x0f})
	t.mockForkChoice.Ancestors[(testSlot/testSlotsPerEpoch)*testSlotsPerEpoch] = forkchoice.ForkChoiceNode{Root: common.Hash{0x1f}}

	err := t.dataColumnSidecarService.ProcessMessage(context.Background(), nil, createMockDataColumnSidecar(testSlot, 0, clparams.FuluVersion))

	t.Error(err)
	t.NotErrorIs(err, ErrIgnore)
	t.Contains(err.Error(), "finalized checkpoint is not an ancestor")
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
		// Slot: testSlot - 1,
	}

	// Mock ViewHeadState to avoid panic
	t.mockSyncedData.EXPECT().ViewHeadState(gomock.Any()).DoAndReturn(func(fn synced_data.ViewHeadStateFn) error {
		return nil
	}).Return(nil)

	// Execute
	sidecar := createMockDataColumnSidecar(testSlot, 0, clparams.FuluVersion)
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

	t.mockPeerDas.EXPECT().TryScheduleRecover(gomock.Any(), gomock.Any()).Return(nil).Times(1)
	// Execute
	events := make(chan *beaconevents.EventStream, 1)
	subscription := t.emitters.Operation().Subscribe(events)
	defer subscription.Unsubscribe()
	sidecar := createMockDataColumnSidecar(testSlot, 0, clparams.FuluVersion)
	err := t.dataColumnSidecarService.ProcessMessage(context.Background(), nil, sidecar)

	// Assert
	t.NoError(err)
	// Storage sends the event for a stored column, the service does not send another.
	t.Empty(events)
}

// A sidecar for the same (slot, proposer, index) claimed while this one is validated, for example by
// columns built from EL blobs, makes this one a duplicate: it is ignored, not stored or forwarded.
func (t *dataColumnSidecarTestSuite) TestProcessMessage_WhenClaimedDuringValidation_ReturnsErrIgnore() {
	verifyDataColumnSidecar = t.mockFuncs.VerifyDataColumnSidecar
	verifyDataColumnSidecarInclusionProof = t.mockFuncs.VerifyDataColumnSidecarInclusionProof
	verifyDataColumnSidecarKZGProofs = t.mockFuncs.VerifyDataColumnSidecarKZGProofs
	blsVerify = t.mockFuncs.BlsVerify

	t.mockSyncedData.EXPECT().Syncing().Return(false)
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()
	t.mockEthClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(false).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecar", gomock.Any()).Return(true).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarInclusionProof", gomock.Any()).Return(true).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarKZGProofs", gomock.Any()).Return(true).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "BlsVerify", gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()
	t.mockForkChoice.Headers[testParentRoot] = &cltypes.BeaconBlockHeader{}
	sidecar := createMockDataColumnSidecar(testSlot, 0, clparams.FuluVersion)
	header := sidecar.SignedBlockHeader.Header
	key := seenSidecarKey{slot: header.Slot, proposerIndex: header.ProposerIndex, index: sidecar.Index}
	t.mockSyncedData.EXPECT().ViewHeadState(gomock.Any()).DoAndReturn(func(fn synced_data.ViewHeadStateFn) error {
		t.dataColumnSidecarService.(*dataColumnSidecarService).seenSidecar.Add(key, struct{}{})
		return nil
	}).Times(1)
	t.mockColumnSidecarStorage.EXPECT().WriteColumnSidecars(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	t.mockPeerDas.EXPECT().TryScheduleRecover(gomock.Any(), gomock.Any()).Times(0)

	err := t.dataColumnSidecarService.ProcessMessage(context.Background(), nil, sidecar)

	t.ErrorIs(err, ErrIgnore)
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
	sidecar := createMockDataColumnSidecar(testSlot, 0, clparams.FuluVersion)
	err := t.dataColumnSidecarService.ProcessMessage(context.Background(), nil, sidecar)

	// Assert
	t.Error(err)
	t.ErrorIs(err, ErrIgnore)
	t.Contains(err.Error(), "failed to write data column sidecar")
	// The sidecar was not stored, so a later valid copy must not be ignored as a duplicate.
	header := sidecar.SignedBlockHeader.Header
	_, seen := t.dataColumnSidecarService.(*dataColumnSidecarService).seenSidecar.Get(seenSidecarKey{slot: header.Slot, proposerIndex: header.ProposerIndex, index: sidecar.Index})
	t.False(seen)
}

func (t *dataColumnSidecarTestSuite) TestProcessMessage_WhenCustodyLookupFails_ReturnsErrIgnore() {
	peerDas := das_mock.NewMockPeerDas(t.gomockCtrl)
	stateReader := das_state_mock.NewMockPeerDasStateReader(t.gomockCtrl)
	t.mockForkChoice.MockPeerDas = peerDas
	t.mockSyncedData.EXPECT().Syncing().Return(false)
	peerDas.EXPECT().StateReader().Return(stateReader)
	stateReader.EXPECT().GetMyCustodyColumns().Return(nil, errors.New("custody unavailable"))

	err := t.dataColumnSidecarService.ProcessMessage(t.T().Context(), nil, createMockDataColumnSidecar(testSlot, 0, clparams.FuluVersion))

	t.ErrorIs(err, ErrIgnore)
}

// expectFuluSidecarOutsideCustody makes every gossip check before the KZG check pass for a node whose only custody
// column is 0, and the KZG check return kzgProofsValid.
func (t *dataColumnSidecarTestSuite) expectFuluSidecarOutsideCustody(kzgProofsValid bool) {
	peerDas := das_mock.NewMockPeerDas(t.gomockCtrl)
	stateReader := das_state_mock.NewMockPeerDasStateReader(t.gomockCtrl)
	t.mockForkChoice.MockPeerDas = peerDas
	peerDas.EXPECT().IsArchivedMode().Return(false).AnyTimes()
	peerDas.EXPECT().StateReader().Return(stateReader).AnyTimes()
	stateReader.EXPECT().GetMyCustodyColumns().Return(map[uint64]bool{0: true}, nil).AnyTimes()

	verifyDataColumnSidecar = t.mockFuncs.VerifyDataColumnSidecar
	verifyDataColumnSidecarInclusionProof = t.mockFuncs.VerifyDataColumnSidecarInclusionProof
	verifyDataColumnSidecarKZGProofs = t.mockFuncs.VerifyDataColumnSidecarKZGProofs
	blsVerify = t.mockFuncs.BlsVerify
	t.mockSyncedData.EXPECT().Syncing().Return(false)
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()
	t.mockEthClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(false).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecar", gomock.Any()).Return(true).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarInclusionProof", gomock.Any()).Return(true).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarKZGProofs", gomock.Any()).Return(kzgProofsValid).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "BlsVerify", gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()
	t.mockForkChoice.Headers[testParentRoot] = &cltypes.BeaconBlockHeader{}
	t.mockSyncedData.EXPECT().ViewHeadState(gomock.Any()).Return(nil).AnyTimes()
}

// A node subscribed to a column's subnet relays a valid sidecar for it even when it does not keep the column.
func (t *dataColumnSidecarTestSuite) TestProcessMessage_WhenValidSidecarOutsideCustody_AcceptsWithoutStoring() {
	t.expectFuluSidecarOutsideCustody(true)
	sidecar := createMockDataColumnSidecar(testSlot, 1, clparams.FuluVersion)

	err := t.dataColumnSidecarService.ProcessMessage(t.T().Context(), nil, sidecar)

	t.NoError(err)
	header := sidecar.SignedBlockHeader.Header
	_, seen := t.dataColumnSidecarService.(*dataColumnSidecarService).seenSidecar.Get(seenSidecarKey{slot: header.Slot, proposerIndex: header.ProposerIndex, index: 1})
	t.True(seen)
}

// A sidecar outside custody passes gossip validation without being stored, so the data_column_sidecar event, which
// storage sends for stored columns, is sent by the service.
func (t *dataColumnSidecarTestSuite) TestProcessMessage_WhenValidSidecarOutsideCustody_EmitsEvent() {
	events := make(chan *beaconevents.EventStream, 2)
	subscription := t.emitters.Operation().Subscribe(events)
	defer subscription.Unsubscribe()
	t.expectFuluSidecarOutsideCustody(true)
	sidecar := createMockDataColumnSidecar(testSlot, 1, clparams.FuluVersion)

	t.Require().NoError(t.dataColumnSidecarService.ProcessMessage(t.T().Context(), nil, sidecar))

	t.Require().Len(events, 1)
	event := <-events
	t.Equal(beaconevents.OpDataColumnSidecar, event.Event)
	data, ok := event.Data.(*beaconevents.DataColumnSidecarData)
	t.Require().True(ok)
	blockRoot, err := sidecar.SignedBlockHeader.Header.HashSSZ()
	t.Require().NoError(err)
	t.Equal(common.Hash(blockRoot), data.BlockRoot)
	t.Equal(testSlot, data.Slot)
	t.Equal(uint64(1), data.Index)
}

func (t *dataColumnSidecarTestSuite) TestProcessMessage_WhenInvalidSidecarOutsideCustody_ReturnsError() {
	events := make(chan *beaconevents.EventStream, 1)
	subscription := t.emitters.Operation().Subscribe(events)
	defer subscription.Unsubscribe()
	t.expectFuluSidecarOutsideCustody(false)
	sidecar := createMockDataColumnSidecar(testSlot, 1, clparams.FuluVersion)

	err := t.dataColumnSidecarService.ProcessMessage(t.T().Context(), nil, sidecar)

	t.ErrorContains(err, "invalid kzg proofs")
	header := sidecar.SignedBlockHeader.Header
	_, seen := t.dataColumnSidecarService.(*dataColumnSidecarService).seenSidecar.Get(seenSidecarKey{slot: header.Slot, proposerIndex: header.ProposerIndex, index: 1})
	t.False(seen)
	t.Empty(events)
}

// archivePeerDas is an archive node with custody column 0 whose block has its blobs recovered, or else over half of
// its columns stored. Such a node stores only its custody columns of the block.
func (t *dataColumnSidecarTestSuite) archivePeerDas(blobsRecovered bool) *das_mock.MockPeerDas {
	peerDas := t.archivePeerDasWithCustodyColumn0()
	peerDas.EXPECT().IsColumnOverHalf(gomock.Any(), gomock.Any()).Return(!blobsRecovered).AnyTimes()
	peerDas.EXPECT().IsBlobAlreadyRecovered(gomock.Any()).Return(blobsRecovered).AnyTimes()
	return peerDas
}

func (t *dataColumnSidecarTestSuite) archivePeerDasWithCustodyColumn0() *das_mock.MockPeerDas {
	peerDas := das_mock.NewMockPeerDas(t.gomockCtrl)
	stateReader := das_state_mock.NewMockPeerDasStateReader(t.gomockCtrl)
	peerDas.EXPECT().IsArchivedMode().Return(true).AnyTimes()
	peerDas.EXPECT().StateReader().Return(stateReader).AnyTimes()
	stateReader.EXPECT().GetMyCustodyColumns().Return(map[uint64]bool{0: true}, nil).AnyTimes()
	return peerDas
}

func (t *dataColumnSidecarTestSuite) assertFuluArchiveSidecar(blobsRecovered, kzgProofsValid bool) {
	t.expectFuluSidecarOutsideCustody(kzgProofsValid)
	t.mockForkChoice.MockPeerDas = t.archivePeerDas(blobsRecovered)
	sidecar := createMockDataColumnSidecar(testSlot, 1, clparams.FuluVersion)

	err := t.dataColumnSidecarService.ProcessMessage(t.T().Context(), nil, sidecar)

	header := sidecar.SignedBlockHeader.Header
	_, seen := t.dataColumnSidecarService.(*dataColumnSidecarService).seenSidecar.Get(seenSidecarKey{slot: header.Slot, proposerIndex: header.ProposerIndex, index: 1})
	if kzgProofsValid {
		t.NoError(err)
		t.True(seen)
		return
	}
	t.ErrorContains(err, "invalid kzg proofs")
	t.NotErrorIs(err, ErrIgnore)
	t.False(seen)
}

func (t *dataColumnSidecarTestSuite) TestProcessMessage_WhenArchiveAndColumnsOverHalf_AcceptsValidSidecar() {
	t.assertFuluArchiveSidecar(false, true)
}

func (t *dataColumnSidecarTestSuite) TestProcessMessage_WhenArchiveAndColumnsOverHalf_RejectsInvalidSidecar() {
	t.assertFuluArchiveSidecar(false, false)
}

func (t *dataColumnSidecarTestSuite) TestProcessMessage_WhenArchiveAndBlobsRecovered_AcceptsValidSidecar() {
	t.assertFuluArchiveSidecar(true, true)
}

func (t *dataColumnSidecarTestSuite) TestProcessMessage_WhenArchiveAndBlobsRecovered_RejectsInvalidSidecar() {
	t.assertFuluArchiveSidecar(true, false)
}

// A custody column is stored even when the archive node can recover the blobs, so a column that recovery failed to
// write is still stored from gossip. Storage sends its event, so the service does not send another.
func (t *dataColumnSidecarTestSuite) TestProcessMessage_WhenArchiveAndColumnsOverHalf_StoresCustodySidecar() {
	events := make(chan *beaconevents.EventStream, 1)
	subscription := t.emitters.Operation().Subscribe(events)
	defer subscription.Unsubscribe()
	t.expectFuluSidecarOutsideCustody(true)
	peerDas := t.archivePeerDas(false)
	t.mockForkChoice.MockPeerDas = peerDas
	sidecar := createMockDataColumnSidecar(testSlot, 0, clparams.FuluVersion)
	blockRoot, err := sidecar.SignedBlockHeader.Header.HashSSZ()
	t.Require().NoError(err)
	t.mockColumnSidecarStorage.EXPECT().WriteColumnSidecars(gomock.Any(), common.Hash(blockRoot), int64(0), sidecar).Return(nil).Times(1)
	peerDas.EXPECT().TryScheduleRecover(testSlot, common.Hash(blockRoot)).Return(nil).Times(1)

	t.NoError(t.dataColumnSidecarService.ProcessMessage(t.T().Context(), nil, sidecar))

	t.Empty(events)
}

func (t *dataColumnSidecarTestSuite) TestProcessMessage_WhenNotFromExpectedProposer_ReturnsError() {
	t.expectFuluSidecarOutsideCustody(true)
	t.setExpectedProposer(2, nil)
	sidecar := createMockDataColumnSidecar(testSlot, 1, clparams.FuluVersion)

	err := t.dataColumnSidecarService.ProcessMessage(t.T().Context(), nil, sidecar)

	t.ErrorContains(err, "does not match expected proposer")
	t.NotErrorIs(err, ErrIgnore)
	header := sidecar.SignedBlockHeader.Header
	_, seen := t.dataColumnSidecarService.(*dataColumnSidecarService).seenSidecar.Get(seenSidecarKey{slot: header.Slot, proposerIndex: header.ProposerIndex, index: 1})
	t.False(seen)
}

// The proposer check comes after the proof checks, as in the spec: a sidecar with invalid proofs is rejected even when
// the proposer read would ignore it.
func (t *dataColumnSidecarTestSuite) TestProcessMessage_WhenInvalidProofsAndExpectedProposerUnknown_ReturnsError() {
	t.expectFuluSidecarOutsideCustody(false)
	t.setExpectedProposer(0, fmt.Errorf("%w: expected proposer is not in the parent state", ErrIgnore))

	err := t.dataColumnSidecarService.ProcessMessage(t.T().Context(), nil, createMockDataColumnSidecar(testSlot, 1, clparams.FuluVersion))

	t.ErrorContains(err, "invalid kzg proofs")
	t.NotErrorIs(err, ErrIgnore)
}

// While the proposers of the sidecar's branch are not known, the sidecar is ignored instead of rejected.
func (t *dataColumnSidecarTestSuite) TestProcessMessage_WhenExpectedProposerUnknown_ReturnsErrIgnore() {
	t.expectFuluSidecarOutsideCustody(true)
	t.setExpectedProposer(0, fmt.Errorf("%w: expected proposer is not in the parent state", ErrIgnore))

	err := t.dataColumnSidecarService.ProcessMessage(t.T().Context(), nil, createMockDataColumnSidecar(testSlot, 1, clparams.FuluVersion))

	t.ErrorIs(err, ErrIgnore)
}

// A block in fork choice had its proposer checked by the state transition, so its sidecars need no shuffling read.
func (t *dataColumnSidecarTestSuite) TestProcessMessage_WhenBlockInForkChoice_SkipsExpectedProposerRead() {
	t.expectFuluSidecarOutsideCustody(true)
	t.dataColumnSidecarService.(*dataColumnSidecarService).expectedProposer = func(context.Context, common.Hash, uint64, uint64) (uint64, error) {
		t.Fail("expected proposer read for a block in fork choice")
		return 0, nil
	}
	sidecar := createMockDataColumnSidecar(testSlot, 1, clparams.FuluVersion)
	blockRoot, err := sidecar.SignedBlockHeader.Header.HashSSZ()
	t.Require().NoError(err)
	t.mockForkChoice.Headers[blockRoot] = sidecar.SignedBlockHeader.Header

	t.NoError(t.dataColumnSidecarService.ProcessMessage(t.T().Context(), nil, sidecar))
}

// ============================================
// GLOAS (EIP-7732) Tests
// ============================================

var testBlockRoot = common.Hash{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00}

// createMockGloasDataColumnSidecar creates a GLOAS-style sidecar with Slot and BeaconBlockRoot
func createMockGloasDataColumnSidecar(slot uint64, index uint64, blockRoot common.Hash) *cltypes.DataColumnSidecar {
	sidecar := cltypes.NewDataColumnSidecarWithVersion(clparams.GloasVersion)
	sidecar.Index = index
	sidecar.Slot = slot
	sidecar.BeaconBlockRoot = blockRoot

	// Add mock data
	proof := &cltypes.KZGProof{}
	copy(proof[:], []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48})
	sidecar.KzgProofs.Append(proof)

	cell := &cltypes.Cell{}
	copy(cell[:], []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32})
	sidecar.Column.Append(cell)

	return sidecar
}

// createMockGloasBlock creates a mock GLOAS block with SignedExecutionPayloadBid
func createMockGloasBlock(slot uint64) *cltypes.SignedBeaconBlock {
	block := &cltypes.SignedBeaconBlock{
		Block: &cltypes.BeaconBlock{
			Slot: slot,
			Body: &cltypes.BeaconBody{
				Version: clparams.GloasVersion, // Required for GetBlobKzgCommitments() to work correctly
				SignedExecutionPayloadBid: &cltypes.SignedExecutionPayloadBid{
					Message: &cltypes.ExecutionPayloadBid{
						BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](4, 48),
					},
				},
			},
		},
	}
	// Add a mock KZG commitment matching the sidecar
	commitment := &cltypes.KZGCommitment{}
	copy(commitment[:], []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48})
	block.Block.Body.SignedExecutionPayloadBid.Message.BlobKzgCommitments.Append(commitment)
	return block
}

// TestGloasProcessMessage_WhenSyncing_ReturnsErrIgnore tests GLOAS returns ErrIgnore when syncing
func (t *dataColumnSidecarTestSuite) TestGloasProcessMessage_WhenSyncing_ReturnsErrIgnore() {
	t.mockSyncedData.EXPECT().Syncing().Return(true)

	sidecar := createMockGloasDataColumnSidecar(testSlot, 0, testBlockRoot)
	err := t.dataColumnSidecarService.ProcessMessage(context.Background(), nil, sidecar)

	t.Equal(ErrIgnore, err)
}

func (t *dataColumnSidecarTestSuite) TestGloasProcessMessage_IgnoresGloasSchemaAtPreGloasSlotBeforeQueueing() {
	t.beaconConfig.ElectraForkEpoch = 0
	t.beaconConfig.FuluForkEpoch = 0
	t.beaconConfig.GloasForkEpoch = testEpoch + 1
	t.mockSyncedData.EXPECT().Syncing().Return(false)
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()
	t.mockColumnSidecarStorage.EXPECT().WriteColumnSidecars(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	sidecar := createMockGloasDataColumnSidecar(testSlot, 0, testBlockRoot)
	t.False(t.beaconConfig.ForkSchemaMatchesSlot(sidecar.Slot, sidecar.Version()))

	err := t.dataColumnSidecarService.ProcessMessage(t.T().Context(), nil, sidecar)

	t.ErrorIs(err, ErrIgnore)
	service := t.dataColumnSidecarService.(*dataColumnSidecarService)
	t.Zero(service.pendingGloasSidecars.count.Load())
}

// TestGloasProcessMessage_WhenAlreadySeen_ReturnsErrIgnore tests GLOAS returns ErrIgnore for duplicate sidecars
func (t *dataColumnSidecarTestSuite) TestGloasProcessMessage_WhenAlreadySeen_ReturnsErrIgnore() {
	t.activateGloas()
	verifyDataColumnSidecarWithCommitments = t.mockFuncs.VerifyDataColumnSidecarWithCommitments
	verifyDataColumnSidecarKZGProofsWithCommitments = t.mockFuncs.VerifyDataColumnSidecarKZGProofsWithCommitments

	t.mockSyncedData.EXPECT().Syncing().Return(false).Times(2)
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarWithCommitments", gomock.Any(), gomock.Any()).Return(true).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarKZGProofsWithCommitments", gomock.Any(), gomock.Any()).Return(true).AnyTimes()

	// Setup block in forkChoice
	block := createMockGloasBlock(testSlot)
	t.importBlock(testBlockRoot, block)

	t.mockColumnSidecarStorage.EXPECT().WriteColumnSidecars(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(1)
	t.mockPeerDas.EXPECT().TryScheduleRecover(gomock.Any(), gomock.Any()).Return(nil).Times(1)

	// First call should succeed
	sidecar := createMockGloasDataColumnSidecar(testSlot, 0, testBlockRoot)
	err := t.dataColumnSidecarService.ProcessMessage(context.Background(), nil, sidecar)
	t.NoError(err)

	// Second call with same sidecar should return ErrIgnore
	err = t.dataColumnSidecarService.ProcessMessage(context.Background(), nil, sidecar)
	t.Equal(ErrIgnore, err)
}

// TestGloasProcessMessage_WhenFutureSlot_ReturnsErrIgnore tests GLOAS future slot handling
func (t *dataColumnSidecarTestSuite) TestGloasProcessMessage_WhenFutureSlot_ReturnsErrIgnore() {
	t.activateGloas()
	t.mockSyncedData.EXPECT().Syncing().Return(false)
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot)
	t.mockEthClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(testSlot + 100).Return(false)

	sidecar := createMockGloasDataColumnSidecar(testSlot+100, 0, testBlockRoot)
	err := t.dataColumnSidecarService.ProcessMessage(context.Background(), nil, sidecar)

	t.Equal(ErrIgnore, err)
}

func (t *dataColumnSidecarTestSuite) TestGloasProcessMessage_WhenIncorrectSubnetAndFutureSlot_ReturnsError() {
	t.activateGloas()
	computeSubnetForDataColumnSidecar = func(index uint64) uint64 { return 1234 }
	incorrectSubnet := uint64(9999)

	t.mockSyncedData.EXPECT().Syncing().Return(false)
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()
	t.mockEthClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(testSlot + 100).Return(false).AnyTimes()

	sidecar := createMockGloasDataColumnSidecar(testSlot+100, 0, testBlockRoot)
	err := t.dataColumnSidecarService.ProcessMessage(context.Background(), &incorrectSubnet, sidecar)

	t.Error(err)
	t.Contains(err.Error(), "incorrect subnet")
}

// finalizeGloasEpochAfterTestSlot finalizes the epoch after testSlot's epoch, so testSlot is below the finalized
// checkpoint start slot and FinalizedSlot().
func (t *dataColumnSidecarTestSuite) finalizeGloasEpochAfterTestSlot() {
	t.activateGloas()
	t.mockForkChoice.FinalizedCheckpointVal = solid.Checkpoint{Epoch: testEpoch + 1}
	t.mockForkChoice.FinalizedSlotVal = (testEpoch+2)*testSlotsPerEpoch - 1
}

// The Gloas gossip rules have no finality condition: a sidecar of a known block is validated at any slot.
func (t *dataColumnSidecarTestSuite) TestGloasProcessMessage_WhenSlotFinalizedAndBlockKnown_StoresSidecar() {
	t.finalizeGloasEpochAfterTestSlot()
	t.expectGloasSidecarStored(testSlot, 0)
	t.importBlock(testBlockRoot, createMockGloasBlock(testSlot))

	err := t.dataColumnSidecarService.ProcessMessage(t.T().Context(), nil, createMockGloasDataColumnSidecar(testSlot, 0, testBlockRoot))

	t.NoError(err)
}

// Fork choice imports no block at or below the finalized checkpoint start slot, so a sidecar of an unknown block
// there is ignored without being queued.
func (t *dataColumnSidecarTestSuite) TestGloasProcessMessage_WhenBlockUnknownAtFinalizedEpochStart_DoesNotQueue() {
	t.activateGloas()
	t.mockForkChoice.FinalizedCheckpointVal = solid.Checkpoint{Epoch: testEpoch}
	t.mockSyncedData.EXPECT().Syncing().Return(false)
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()

	err := t.dataColumnSidecarService.ProcessMessage(t.T().Context(), nil, createMockGloasDataColumnSidecar(testEpoch*testSlotsPerEpoch, 0, testBlockRoot))

	t.ErrorIs(err, ErrIgnore)
	t.Zero(t.dataColumnSidecarService.(*dataColumnSidecarService).pendingGloasSidecars.count.Load())
}

// The block of a slot after the finalized checkpoint start slot can still be imported, even when FinalizedSlot() is
// the last slot of the finalized epoch.
func (t *dataColumnSidecarTestSuite) TestGloasProcessMessage_WhenBlockUnknownAfterFinalizedEpochStart_Queues() {
	t.activateGloas()
	t.mockForkChoice.FinalizedCheckpointVal = solid.Checkpoint{Epoch: testEpoch}
	t.mockForkChoice.FinalizedSlotVal = (testEpoch+1)*testSlotsPerEpoch - 1
	t.mockSyncedData.EXPECT().Syncing().Return(false)
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()

	err := t.dataColumnSidecarService.ProcessMessage(t.T().Context(), nil, createMockGloasDataColumnSidecar(testEpoch*testSlotsPerEpoch+1, 0, testBlockRoot))

	t.ErrorIs(err, ErrIgnore)
	t.Equal(int32(1), t.dataColumnSidecarService.(*dataColumnSidecarService).pendingGloasSidecars.count.Load())
}

// The anchor block bounds fork choice like the finalized checkpoint does.
func (t *dataColumnSidecarTestSuite) TestGloasProcessMessage_WhenBlockUnknownAtAnchorSlot_DoesNotQueue() {
	t.activateGloas()
	t.mockForkChoice.AnchorSlotVal = testSlot
	t.mockSyncedData.EXPECT().Syncing().Return(false)
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()

	err := t.dataColumnSidecarService.ProcessMessage(t.T().Context(), nil, createMockGloasDataColumnSidecar(testSlot, 0, testBlockRoot))

	t.ErrorIs(err, ErrIgnore)
	t.Zero(t.dataColumnSidecarService.(*dataColumnSidecarService).pendingGloasSidecars.count.Load())
}

// importBlock makes fork choice hold block with its header, as after a successful import.
func (t *dataColumnSidecarTestSuite) importBlock(root common.Hash, block *cltypes.SignedBeaconBlock) {
	t.mockForkChoice.Blocks[root] = block
	t.mockForkChoice.Headers[root] = &cltypes.BeaconBlockHeader{Slot: block.Block.Slot}
}

// Fork choice holds a block while its state transition runs and publishes its header only after the transition
// succeeds. Until then the block is not seen, so the sidecar is queued instead of validated against it.
func (t *dataColumnSidecarTestSuite) TestGloasProcessMessage_WhenBlockIsStillImporting_Queues() {
	t.activateGloas()
	t.mockSyncedData.EXPECT().Syncing().Return(false)
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()
	t.mockColumnSidecarStorage.EXPECT().WriteColumnSidecars(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	t.mockForkChoice.Blocks[testBlockRoot] = createMockGloasBlock(testSlot)

	err := t.dataColumnSidecarService.ProcessMessage(t.T().Context(), nil, createMockGloasDataColumnSidecar(testSlot, 0, testBlockRoot))

	t.ErrorIs(err, ErrIgnore)
	service := t.dataColumnSidecarService.(*dataColumnSidecarService)
	t.Equal(int32(1), service.pendingGloasSidecars.count.Load())
	service.pendingGloasSidecars.processPending(t.T().Context())
	t.Equal(int32(1), service.pendingGloasSidecars.count.Load(), "the queue keeps the sidecar until the block is imported")
}

// importingWhileFinalizing imports a block when finality is read, as when fork choice imports the block and finality
// moves past its slot between the block lookup and the importability check.
type importingWhileFinalizing struct {
	*forkchoice_mock.ForkChoiceStorageMock
	importBlock func()
}

func (f *importingWhileFinalizing) FinalizedCheckpoint() solid.Checkpoint {
	f.importBlock()
	return f.ForkChoiceStorageMock.FinalizedCheckpoint()
}

func (t *dataColumnSidecarTestSuite) TestGloasPendingQueueProcessesSidecarWhenBlockIsImportedAsFinalityMoves() {
	service := t.dataColumnSidecarService.(*dataColumnSidecarService)
	sidecar := createMockGloasDataColumnSidecar(testSlot, 0, testBlockRoot)
	service.scheduleSidecarForLaterProcessing(sidecar)
	t.Require().Equal(int32(1), service.pendingGloasSidecars.count.Load())
	t.finalizeGloasEpochAfterTestSlot()
	t.expectGloasSidecarStored(testSlot, 0)
	service.forkChoice = &importingWhileFinalizing{
		ForkChoiceStorageMock: t.mockForkChoice,
		importBlock:           func() { t.importBlock(testBlockRoot, createMockGloasBlock(testSlot)) },
	}

	service.pendingGloasSidecars.processPending(t.T().Context())

	t.Zero(service.pendingGloasSidecars.count.Load())
}

// importingWhileReadingAnchor imports a block when the anchor slot is read, as when the importability check waits
// for fork choice to finish importing the block.
type importingWhileReadingAnchor struct {
	*forkchoice_mock.ForkChoiceStorageMock
	importBlock func()
}

func (f *importingWhileReadingAnchor) AnchorSlot() uint64 {
	f.importBlock()
	return f.ForkChoiceStorageMock.AnchorSlot()
}

// A sidecar whose first lookup misses a block being imported is validated against it once the import finishes, so a
// sidecar outside custody is accepted and relayed instead of ignored.
func (t *dataColumnSidecarTestSuite) TestGloasProcessMessage_WhenBlockIsImportedDuringLookup_AcceptsSidecarOutsideCustody() {
	t.expectGloasSidecarOutsideCustody(true)
	service := t.dataColumnSidecarService.(*dataColumnSidecarService)
	service.forkChoice = &importingWhileReadingAnchor{
		ForkChoiceStorageMock: t.mockForkChoice,
		importBlock:           func() { t.importBlock(testBlockRoot, createMockGloasBlock(testSlot)) },
	}

	err := t.dataColumnSidecarService.ProcessMessage(t.T().Context(), nil, createMockGloasDataColumnSidecar(testSlot, 1, testBlockRoot))

	t.NoError(err)
	t.Zero(service.pendingGloasSidecars.count.Load())
}

func (t *dataColumnSidecarTestSuite) expectGloasSidecarStored(slot, index uint64) {
	verifyDataColumnSidecarWithCommitments = t.mockFuncs.VerifyDataColumnSidecarWithCommitments
	verifyDataColumnSidecarKZGProofsWithCommitments = t.mockFuncs.VerifyDataColumnSidecarKZGProofsWithCommitments
	t.mockSyncedData.EXPECT().Syncing().Return(false).AnyTimes()
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(slot).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarWithCommitments", gomock.Any(), gomock.Any()).Return(true).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarKZGProofsWithCommitments", gomock.Any(), gomock.Any()).Return(true).AnyTimes()
	t.mockColumnSidecarStorage.EXPECT().WriteColumnSidecars(gomock.Any(), testBlockRoot, int64(index), gomock.Any()).Return(nil).Times(1)
	t.mockPeerDas.EXPECT().TryScheduleRecover(slot, testBlockRoot).Return(nil).Times(1)
}

// TestGloasProcessMessage_WhenBlockNotFound_SchedulesForLater tests deferred processing
func (t *dataColumnSidecarTestSuite) TestGloasProcessMessage_WhenBlockNotFound_SchedulesForLater() {
	t.activateGloas()
	t.mockSyncedData.EXPECT().Syncing().Return(false)
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()
	// Block not in forkChoice (Blocks map is empty)

	sidecar := createMockGloasDataColumnSidecar(testSlot, 0, testBlockRoot)
	err := t.dataColumnSidecarService.ProcessMessage(context.Background(), nil, sidecar)

	t.ErrorIs(err, ErrIgnore)
	service := t.dataColumnSidecarService.(*dataColumnSidecarService)
	t.Equal(int32(1), service.pendingGloasSidecars.count.Load())
	root, keyErr := sidecar.HashSSZ()
	t.NoError(keyErr)
	_, exists := service.pendingGloasSidecars.jobs.Load(common.Hash(root))
	t.True(exists)
}

func (t *dataColumnSidecarTestSuite) TestGloasPendingQueueCap() {
	service := t.dataColumnSidecarService.(*dataColumnSidecarService)
	t.Equal(int32(maxPendingGloasSidecars), service.pendingGloasSidecars.capacity)
	service.pendingGloasSidecars.capacity = 1
	service.scheduleSidecarForLaterProcessing(createMockGloasDataColumnSidecar(testSlot, 0, testBlockRoot))
	t.Require().Equal(int32(1), service.pendingGloasSidecars.count.Load())

	sidecar := createMockGloasDataColumnSidecar(testSlot, 1, testBlockRoot)
	service.scheduleSidecarForLaterProcessing(sidecar)

	t.Equal(int32(1), service.pendingGloasSidecars.count.Load())
	root, err := sidecar.HashSSZ()
	t.Require().NoError(err)
	_, exists := service.pendingGloasSidecars.jobs.Load(common.Hash(root))
	t.False(exists)
}

func (t *dataColumnSidecarTestSuite) TestGloasProcessMessage_WhenOversizedAndBlockMissing_DoesNotQueue() {
	t.activateGloas()
	t.mockSyncedData.EXPECT().Syncing().Return(false)
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()
	t.beaconConfig.BlobSchedule = []clparams.BlobParameters{{Epoch: testEpoch, MaxBlobsPerBlock: 2}}
	sidecar := createMockGloasDataColumnSidecar(testSlot, 0, testBlockRoot)
	for sidecar.Column.Len() <= 2 {
		sidecar.Column.Append(&cltypes.Cell{})
		sidecar.KzgProofs.Append(&cltypes.KZGProof{})
	}

	err := t.dataColumnSidecarService.ProcessMessage(t.T().Context(), nil, sidecar)

	t.ErrorIs(err, ErrIgnore)
	service := t.dataColumnSidecarService.(*dataColumnSidecarService)
	t.Zero(service.pendingGloasSidecars.count.Load())
}

func (t *dataColumnSidecarTestSuite) TestGloasPendingQueueDeduplicates() {
	service := t.dataColumnSidecarService.(*dataColumnSidecarService)
	sidecar := createMockGloasDataColumnSidecar(testSlot, 0, testBlockRoot)

	service.scheduleSidecarForLaterProcessing(sidecar)
	service.scheduleSidecarForLaterProcessing(sidecar)

	t.Equal(int32(1), service.pendingGloasSidecars.count.Load())
}

func (t *dataColumnSidecarTestSuite) TestGloasProcessMessage_WhenProofCountMismatchAndBlockMissing_DoesNotQueue() {
	t.activateGloas()
	t.mockSyncedData.EXPECT().Syncing().Return(false)
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()
	sidecar := createMockGloasDataColumnSidecar(testSlot, 0, testBlockRoot)
	sidecar.KzgProofs.Append(&cltypes.KZGProof{})

	err := t.dataColumnSidecarService.ProcessMessage(t.T().Context(), nil, sidecar)

	t.ErrorIs(err, ErrIgnore)
	service := t.dataColumnSidecarService.(*dataColumnSidecarService)
	t.Zero(service.pendingGloasSidecars.count.Load())
}

func (t *dataColumnSidecarTestSuite) TestGloasPendingQueueKeepsDistinctContentForSameColumn() {
	service := t.dataColumnSidecarService.(*dataColumnSidecarService)
	first := createMockGloasDataColumnSidecar(testSlot, 0, testBlockRoot)
	second := createMockGloasDataColumnSidecar(testSlot, 0, testBlockRoot)
	differentProof := &cltypes.KZGProof{}
	differentProof[0] = 0xff
	second.KzgProofs.Set(0, differentProof)

	service.scheduleSidecarForLaterProcessing(first)
	service.scheduleSidecarForLaterProcessing(second)

	t.Equal(int32(2), service.pendingGloasSidecars.count.Load())
}

// blockGoneAfterFirstRead finds a block on the first read only, as when the block disappears from fork choice
// between the queue's lookup and validation.
type blockGoneAfterFirstRead struct {
	forkchoice.ForkChoiceStorage
	reads int
}

func (s *blockGoneAfterFirstRead) GetBlock(root common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	s.reads++
	if s.reads > 1 {
		return nil, false
	}
	return s.ForkChoiceStorage.GetBlock(root)
}

func (t *dataColumnSidecarTestSuite) TestGloasPendingQueueKeepsOriginalAgeWhenBlockDisappears() {
	t.activateGloas()
	service := t.dataColumnSidecarService.(*dataColumnSidecarService)
	sidecar := createMockGloasDataColumnSidecar(testSlot, 0, testBlockRoot)
	root, err := sidecar.HashSSZ()
	t.Require().NoError(err)
	service.pendingGloasSidecars.capacity = 1
	createdAt := time.Now().Add(-pendingGloasSidecarExpiry / 2)
	original := storePendingJob(t.T(), service.pendingGloasSidecars, common.Hash(root), sidecar, createdAt)
	rejectionsBeforeRetry := service.pendingGloasSidecars.fullCounter.GetValueUint64()

	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()
	t.importBlock(testBlockRoot, createMockGloasBlock(testSlot))
	service.forkChoice = &blockGoneAfterFirstRead{ForkChoiceStorage: t.mockForkChoice}
	service.pendingGloasSidecars.processPending(t.T().Context())

	stored, exists := service.pendingGloasSidecars.jobs.Load(common.Hash(root))
	t.Require().True(exists)
	t.Same(original, stored)
	t.Equal(createdAt, stored.(*pendingJob[*cltypes.DataColumnSidecar]).creationTime)
	t.Equal(int32(1), service.pendingGloasSidecars.count.Load())
	t.Equal(rejectionsBeforeRetry, service.pendingGloasSidecars.fullCounter.GetValueUint64())
}

func (t *dataColumnSidecarTestSuite) TestGloasPendingQueueDropsSidecarWhoseBlockCannotBeImported() {
	service := t.dataColumnSidecarService.(*dataColumnSidecarService)
	sidecar := createMockGloasDataColumnSidecar(testSlot, 0, testBlockRoot)
	service.scheduleSidecarForLaterProcessing(sidecar)
	t.Require().Equal(int32(1), service.pendingGloasSidecars.count.Load())
	t.mockForkChoice.FinalizedCheckpointVal = solid.Checkpoint{Epoch: testEpoch + 1}

	service.pendingGloasSidecars.processPending(t.T().Context())

	t.Zero(service.pendingGloasSidecars.count.Load())
}

func (t *dataColumnSidecarTestSuite) TestGloasPendingQueueProcessesFinalizedSidecarWhenBlockArrives() {
	service := t.dataColumnSidecarService.(*dataColumnSidecarService)
	sidecar := createMockGloasDataColumnSidecar(testSlot, 0, testBlockRoot)
	service.scheduleSidecarForLaterProcessing(sidecar)
	t.Require().Equal(int32(1), service.pendingGloasSidecars.count.Load())
	t.finalizeGloasEpochAfterTestSlot()
	t.expectGloasSidecarStored(testSlot, 0)
	t.importBlock(testBlockRoot, createMockGloasBlock(testSlot))

	service.pendingGloasSidecars.processPending(t.T().Context())

	t.Zero(service.pendingGloasSidecars.count.Load())
}

func (t *dataColumnSidecarTestSuite) TestGloasPendingQueueDropsSeenSidecar() {
	t.activateGloas()
	service := t.dataColumnSidecarService.(*dataColumnSidecarService)
	sidecar := createMockGloasDataColumnSidecar(testSlot, 0, testBlockRoot)
	service.scheduleSidecarForLaterProcessing(sidecar)
	t.Require().Equal(int32(1), service.pendingGloasSidecars.count.Load())
	service.seenGloasSidecar.Add(seenGloasSidecarKey{testBlockRoot, sidecar.Index}, struct{}{})
	t.importBlock(testBlockRoot, createMockGloasBlock(testSlot))

	service.pendingGloasSidecars.processPending(t.T().Context())

	t.Zero(service.pendingGloasSidecars.count.Load())
}

func (t *dataColumnSidecarTestSuite) TestGloasPendingQueueExpiryLogsSlot() {
	service := t.dataColumnSidecarService.(*dataColumnSidecarService)
	sidecar := createMockGloasDataColumnSidecar(testSlot, 0, testBlockRoot)
	root, err := sidecar.HashSSZ()
	t.Require().NoError(err)
	storePendingJob(t.T(), service.pendingGloasSidecars, common.Hash(root), sidecar, time.Now().Add(-(pendingGloasSidecarExpiry + time.Second)))
	output := captureServiceLogs(t.T())

	service.pendingGloasSidecars.processPending(t.T().Context())

	t.Zero(service.pendingGloasSidecars.count.Load())
	t.Contains(output.String(), "slot=321")
}

func (t *dataColumnSidecarTestSuite) TestGloasPendingQueueProcessesSidecarAtBlobLimit() {
	t.activateGloas()
	verifyDataColumnSidecarKZGProofsWithCommitments = t.mockFuncs.VerifyDataColumnSidecarKZGProofsWithCommitments
	t.mockSyncedData.EXPECT().Syncing().Return(false)
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarKZGProofsWithCommitments", gomock.Any(), gomock.Any()).Return(true)
	t.mockColumnSidecarStorage.EXPECT().WriteColumnSidecars(gomock.Any(), testBlockRoot, int64(0), gomock.Any()).Return(nil)
	t.mockPeerDas.EXPECT().TryScheduleRecover(testSlot, testBlockRoot).Return(nil)
	t.beaconConfig.BlobSchedule = []clparams.BlobParameters{{Epoch: testEpoch, MaxBlobsPerBlock: 2}}

	service := t.dataColumnSidecarService.(*dataColumnSidecarService)
	sidecar := createMockGloasDataColumnSidecar(testSlot, 0, testBlockRoot)
	sidecar.Column.Append(&cltypes.Cell{})
	sidecar.KzgProofs.Append(&cltypes.KZGProof{})
	t.ErrorIs(service.ProcessMessage(t.T().Context(), nil, sidecar), ErrIgnore)
	t.Require().Equal(int32(1), service.pendingGloasSidecars.count.Load())

	block := createMockGloasBlock(testSlot)
	block.Block.Body.SignedExecutionPayloadBid.Message.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
	t.importBlock(testBlockRoot, block)
	service.pendingGloasSidecars.processPending(t.T().Context())

	t.Zero(service.pendingGloasSidecars.count.Load())
	_, seen := service.seenGloasSidecar.Get(seenGloasSidecarKey{
		beaconBlockRoot: testBlockRoot,
		index:           sidecar.Index,
	})
	t.True(seen)
}

// TestGloasProcessMessage_WhenSlotMismatch_ReturnsError tests slot mismatch validation
func (t *dataColumnSidecarTestSuite) TestGloasProcessMessage_WhenSlotMismatch_ReturnsError() {
	t.activateGloas()
	t.mockSyncedData.EXPECT().Syncing().Return(false)
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()

	// Block has different slot than sidecar
	block := createMockGloasBlock(testSlot + 10)
	t.importBlock(testBlockRoot, block)

	sidecar := createMockGloasDataColumnSidecar(testSlot, 0, testBlockRoot)
	err := t.dataColumnSidecarService.ProcessMessage(context.Background(), nil, sidecar)

	t.Error(err)
	t.Contains(err.Error(), "does not match block slot")
}

// TestGloasProcessMessage_WhenInvalidSidecar_ReturnsError tests GLOAS sidecar validation
func (t *dataColumnSidecarTestSuite) TestGloasProcessMessage_WhenInvalidSidecar_ReturnsError() {
	t.activateGloas()
	verifyDataColumnSidecarWithCommitments = t.mockFuncs.VerifyDataColumnSidecarWithCommitments

	t.mockSyncedData.EXPECT().Syncing().Return(false)
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarWithCommitments", gomock.Any(), gomock.Any()).Return(false).AnyTimes()

	block := createMockGloasBlock(testSlot)
	t.importBlock(testBlockRoot, block)

	sidecar := createMockGloasDataColumnSidecar(testSlot, 0, testBlockRoot)
	err := t.dataColumnSidecarService.ProcessMessage(context.Background(), nil, sidecar)

	t.Error(err)
	t.Contains(err.Error(), "invalid data column sidecar")
}

func (t *dataColumnSidecarTestSuite) TestGloasProcessMessage_WhenIncorrectSubnet_RejectsBeforeQueueing() {
	t.activateGloas()
	computeSubnetForDataColumnSidecar = func(index uint64) uint64 { return 1234 }

	incorrectSubnet := uint64(9999)

	t.mockSyncedData.EXPECT().Syncing().Return(false)
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()

	sidecar := createMockGloasDataColumnSidecar(testSlot, 0, testBlockRoot)
	err := t.dataColumnSidecarService.ProcessMessage(context.Background(), &incorrectSubnet, sidecar)

	t.Error(err)
	t.Contains(err.Error(), "incorrect subnet")
	service := t.dataColumnSidecarService.(*dataColumnSidecarService)
	t.Zero(service.pendingGloasSidecars.count.Load())
}

// TestGloasProcessMessage_WhenInvalidKZGProofs_ReturnsError tests GLOAS KZG proof validation
func (t *dataColumnSidecarTestSuite) TestGloasProcessMessage_WhenInvalidKZGProofs_ReturnsError() {
	t.activateGloas()
	verifyDataColumnSidecarWithCommitments = t.mockFuncs.VerifyDataColumnSidecarWithCommitments
	verifyDataColumnSidecarKZGProofsWithCommitments = t.mockFuncs.VerifyDataColumnSidecarKZGProofsWithCommitments

	t.mockSyncedData.EXPECT().Syncing().Return(false)
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarWithCommitments", gomock.Any(), gomock.Any()).Return(true).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarKZGProofsWithCommitments", gomock.Any(), gomock.Any()).Return(false).AnyTimes()

	block := createMockGloasBlock(testSlot)
	t.importBlock(testBlockRoot, block)

	sidecar := createMockGloasDataColumnSidecar(testSlot, 0, testBlockRoot)
	err := t.dataColumnSidecarService.ProcessMessage(context.Background(), nil, sidecar)

	t.Error(err)
	t.Contains(err.Error(), "invalid kzg proofs")
}

// TestGloasProcessMessage_WhenValid_StoresSuccessfully tests successful GLOAS sidecar processing
func (t *dataColumnSidecarTestSuite) TestGloasProcessMessage_WhenValid_StoresSuccessfully() {
	t.activateGloas()
	verifyDataColumnSidecarWithCommitments = t.mockFuncs.VerifyDataColumnSidecarWithCommitments
	verifyDataColumnSidecarKZGProofsWithCommitments = t.mockFuncs.VerifyDataColumnSidecarKZGProofsWithCommitments

	t.mockSyncedData.EXPECT().Syncing().Return(false)
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarWithCommitments", gomock.Any(), gomock.Any()).Return(true).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarKZGProofsWithCommitments", gomock.Any(), gomock.Any()).Return(true).AnyTimes()

	block := createMockGloasBlock(testSlot)
	t.importBlock(testBlockRoot, block)

	t.mockColumnSidecarStorage.EXPECT().WriteColumnSidecars(gomock.Any(), testBlockRoot, int64(0), gomock.Any()).Return(nil).Times(1)
	t.mockPeerDas.EXPECT().TryScheduleRecover(testSlot, testBlockRoot).Return(nil).Times(1)

	sidecar := createMockGloasDataColumnSidecar(testSlot, 0, testBlockRoot)
	err := t.dataColumnSidecarService.ProcessMessage(context.Background(), nil, sidecar)

	t.NoError(err)
}

// TestGloasProcessMessage_WhenStorageFails_ReturnsError tests GLOAS storage failure
func (t *dataColumnSidecarTestSuite) TestGloasProcessMessage_WhenStorageFails_ReturnsError() {
	t.activateGloas()
	verifyDataColumnSidecarWithCommitments = t.mockFuncs.VerifyDataColumnSidecarWithCommitments
	verifyDataColumnSidecarKZGProofsWithCommitments = t.mockFuncs.VerifyDataColumnSidecarKZGProofsWithCommitments

	t.mockSyncedData.EXPECT().Syncing().Return(false)
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarWithCommitments", gomock.Any(), gomock.Any()).Return(true).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarKZGProofsWithCommitments", gomock.Any(), gomock.Any()).Return(true).AnyTimes()

	block := createMockGloasBlock(testSlot)
	t.importBlock(testBlockRoot, block)

	t.mockColumnSidecarStorage.EXPECT().WriteColumnSidecars(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(errors.New("storage error"))

	sidecar := createMockGloasDataColumnSidecar(testSlot, 0, testBlockRoot)
	err := t.dataColumnSidecarService.ProcessMessage(context.Background(), nil, sidecar)

	t.Error(err)
	t.ErrorIs(err, ErrIgnore)
	t.Contains(err.Error(), "failed to write data column sidecar")
	_, seen := t.dataColumnSidecarService.(*dataColumnSidecarService).seenGloasSidecar.Get(seenGloasSidecarKey{beaconBlockRoot: testBlockRoot, index: 0})
	t.False(seen)
}

func (t *dataColumnSidecarTestSuite) TestGloasProcessMessage_WhenCustodyLookupFails_ReturnsErrIgnore() {
	t.activateGloas()
	peerDas := das_mock.NewMockPeerDas(t.gomockCtrl)
	stateReader := das_state_mock.NewMockPeerDasStateReader(t.gomockCtrl)
	t.mockForkChoice.MockPeerDas = peerDas
	t.mockSyncedData.EXPECT().Syncing().Return(false)
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot)
	peerDas.EXPECT().StateReader().Return(stateReader)
	stateReader.EXPECT().GetMyCustodyColumns().Return(nil, errors.New("custody unavailable"))
	t.importBlock(testBlockRoot, createMockGloasBlock(testSlot))

	err := t.dataColumnSidecarService.ProcessMessage(t.T().Context(), nil, createMockGloasDataColumnSidecar(testSlot, 0, testBlockRoot))

	t.ErrorIs(err, ErrIgnore)
}

// expectGloasSidecarOutsideCustody makes every Gloas gossip check before the KZG check pass for a node whose only
// custody column is 0, and the KZG check return kzgProofsValid. The node must not store or recover anything.
func (t *dataColumnSidecarTestSuite) expectGloasSidecarOutsideCustody(kzgProofsValid bool) {
	t.activateGloas()
	peerDas := das_mock.NewMockPeerDas(t.gomockCtrl)
	stateReader := das_state_mock.NewMockPeerDasStateReader(t.gomockCtrl)
	t.mockForkChoice.MockPeerDas = peerDas
	peerDas.EXPECT().IsArchivedMode().Return(false).AnyTimes()
	peerDas.EXPECT().StateReader().Return(stateReader).AnyTimes()
	stateReader.EXPECT().GetMyCustodyColumns().Return(map[uint64]bool{0: true}, nil).AnyTimes()

	verifyDataColumnSidecarWithCommitments = t.mockFuncs.VerifyDataColumnSidecarWithCommitments
	verifyDataColumnSidecarKZGProofsWithCommitments = t.mockFuncs.VerifyDataColumnSidecarKZGProofsWithCommitments
	t.mockSyncedData.EXPECT().Syncing().Return(false).AnyTimes()
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarWithCommitments", gomock.Any(), gomock.Any()).Return(true).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarKZGProofsWithCommitments", gomock.Any(), gomock.Any()).Return(kzgProofsValid).AnyTimes()
	t.mockColumnSidecarStorage.EXPECT().WriteColumnSidecars(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
}

func (t *dataColumnSidecarTestSuite) TestGloasProcessMessage_WhenValidSidecarOutsideCustody_AcceptsWithoutStoring() {
	t.expectGloasSidecarOutsideCustody(true)
	t.importBlock(testBlockRoot, createMockGloasBlock(testSlot))

	err := t.dataColumnSidecarService.ProcessMessage(t.T().Context(), nil, createMockGloasDataColumnSidecar(testSlot, 1, testBlockRoot))

	t.NoError(err)
	_, seen := t.dataColumnSidecarService.(*dataColumnSidecarService).seenGloasSidecar.Get(seenGloasSidecarKey{beaconBlockRoot: testBlockRoot, index: 1})
	t.True(seen)
}

func (t *dataColumnSidecarTestSuite) TestGloasProcessMessage_WhenValidSidecarOutsideCustody_EmitsEvent() {
	events := make(chan *beaconevents.EventStream, 2)
	subscription := t.emitters.Operation().Subscribe(events)
	defer subscription.Unsubscribe()
	t.expectGloasSidecarOutsideCustody(true)
	t.importBlock(testBlockRoot, createMockGloasBlock(testSlot))

	t.Require().NoError(t.dataColumnSidecarService.ProcessMessage(t.T().Context(), nil, createMockGloasDataColumnSidecar(testSlot, 1, testBlockRoot)))

	t.Require().Len(events, 1)
	event := <-events
	t.Equal(beaconevents.OpDataColumnSidecar, event.Event)
	data, ok := event.Data.(*beaconevents.DataColumnSidecarData)
	t.Require().True(ok)
	t.Equal(testBlockRoot, data.BlockRoot)
	t.Equal(testSlot, data.Slot)
	t.Equal(uint64(1), data.Index)
}

func (t *dataColumnSidecarTestSuite) TestGloasProcessMessage_WhenInvalidSidecarOutsideCustody_ReturnsError() {
	events := make(chan *beaconevents.EventStream, 1)
	subscription := t.emitters.Operation().Subscribe(events)
	defer subscription.Unsubscribe()
	t.expectGloasSidecarOutsideCustody(false)
	t.importBlock(testBlockRoot, createMockGloasBlock(testSlot))

	err := t.dataColumnSidecarService.ProcessMessage(t.T().Context(), nil, createMockGloasDataColumnSidecar(testSlot, 1, testBlockRoot))

	t.EqualError(err, "invalid kzg proofs for data column sidecar")
	_, seen := t.dataColumnSidecarService.(*dataColumnSidecarService).seenGloasSidecar.Get(seenGloasSidecarKey{beaconBlockRoot: testBlockRoot, index: 1})
	t.False(seen)
	t.Empty(events)
}

func (t *dataColumnSidecarTestSuite) TestGloasProcessMessage_WhenSidecarOutsideCustodyAndBlockMissing_DoesNotQueue() {
	t.expectGloasSidecarOutsideCustody(true)

	err := t.dataColumnSidecarService.ProcessMessage(t.T().Context(), nil, createMockGloasDataColumnSidecar(testSlot, 1, testBlockRoot))

	t.ErrorIs(err, ErrIgnore)
	t.Zero(t.dataColumnSidecarService.(*dataColumnSidecarService).pendingGloasSidecars.count.Load())
}

func (t *dataColumnSidecarTestSuite) TestGloasProcessMessage_WhenSidecarOutsideCustodyAlreadyAccepted_ReturnsErrIgnore() {
	events := make(chan *beaconevents.EventStream, 2)
	subscription := t.emitters.Operation().Subscribe(events)
	defer subscription.Unsubscribe()
	t.expectGloasSidecarOutsideCustody(true)
	t.importBlock(testBlockRoot, createMockGloasBlock(testSlot))
	sidecar := createMockGloasDataColumnSidecar(testSlot, 1, testBlockRoot)
	t.Require().NoError(t.dataColumnSidecarService.ProcessMessage(t.T().Context(), nil, sidecar))

	err := t.dataColumnSidecarService.ProcessMessage(t.T().Context(), nil, sidecar)

	t.Equal(ErrIgnore, err)
	t.Len(events, 1)
}

func (t *dataColumnSidecarTestSuite) assertGloasArchiveSidecar(blobsRecovered, kzgProofsValid bool) {
	t.expectGloasSidecarOutsideCustody(kzgProofsValid)
	t.mockForkChoice.MockPeerDas = t.archivePeerDas(blobsRecovered)
	t.importBlock(testBlockRoot, createMockGloasBlock(testSlot))

	err := t.dataColumnSidecarService.ProcessMessage(t.T().Context(), nil, createMockGloasDataColumnSidecar(testSlot, 1, testBlockRoot))

	_, seen := t.dataColumnSidecarService.(*dataColumnSidecarService).seenGloasSidecar.Get(seenGloasSidecarKey{beaconBlockRoot: testBlockRoot, index: 1})
	if kzgProofsValid {
		t.NoError(err)
		t.True(seen)
		return
	}
	t.EqualError(err, "invalid kzg proofs for data column sidecar")
	t.False(seen)
}

func (t *dataColumnSidecarTestSuite) TestGloasProcessMessage_WhenArchiveAndColumnsOverHalf_AcceptsValidSidecar() {
	t.assertGloasArchiveSidecar(false, true)
}

func (t *dataColumnSidecarTestSuite) TestGloasProcessMessage_WhenArchiveAndColumnsOverHalf_RejectsInvalidSidecar() {
	t.assertGloasArchiveSidecar(false, false)
}

func (t *dataColumnSidecarTestSuite) TestGloasProcessMessage_WhenArchiveAndBlobsRecovered_AcceptsValidSidecar() {
	t.assertGloasArchiveSidecar(true, true)
}

func (t *dataColumnSidecarTestSuite) TestGloasProcessMessage_WhenArchiveAndBlobsRecovered_RejectsInvalidSidecar() {
	t.assertGloasArchiveSidecar(true, false)
}

// A published Gloas column, such as a recovered one, is marked seen, so a later gossip copy is a duplicate that is
// ignored before the KZG check.
func (t *dataColumnSidecarTestSuite) TestGloasPublishColumns_MarksColumnSeenSoGossipCopyIsIgnored() {
	t.activateGloas()
	publisher := gossipmock.NewMockGossip(t.gomockCtrl)
	t.dataColumnSidecarService.(*dataColumnSidecarService).gossipManager = publisher
	computeSubnetForDataColumnSidecar = func(index uint64) uint64 { return index }
	sidecar := createMockGloasDataColumnSidecar(testSlot, 1, testBlockRoot)
	encoded, err := sidecar.EncodeSSZ(nil)
	t.Require().NoError(err)
	digest := common.Bytes4{1, 2, 3, 4}
	t.mockEthClock.EXPECT().ComputeForkDigest(testSlot/testSlotsPerEpoch).Return(digest, nil)
	publisher.EXPECT().PublishToForkDigest(gomock.Any(), digest, gossip.TopicNameDataColumnSidecar(1), encoded).Return(nil).Times(1)

	t.dataColumnSidecarService.PublishColumns(t.T().Context(), []*cltypes.DataColumnSidecar{sidecar})

	verifyDataColumnSidecarKZGProofsWithCommitments = t.mockFuncs.VerifyDataColumnSidecarKZGProofsWithCommitments
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarKZGProofsWithCommitments", gomock.Any(), gomock.Any()).Times(0)
	t.mockSyncedData.EXPECT().Syncing().Return(false)
	t.importBlock(testBlockRoot, createMockGloasBlock(testSlot))
	t.Equal(ErrIgnore, t.dataColumnSidecarService.ProcessMessage(t.T().Context(), nil, sidecar))
}

func (t *dataColumnSidecarTestSuite) TestGloasPublishColumns_WhenSlotFinalizedAndBlockImported_PublishesColumns() {
	t.finalizeGloasEpochAfterTestSlot()
	t.importBlock(testBlockRoot, createMockGloasBlock(testSlot))
	publisher := gossipmock.NewMockGossip(t.gomockCtrl)
	t.dataColumnSidecarService.(*dataColumnSidecarService).gossipManager = publisher
	computeSubnetForDataColumnSidecar = func(index uint64) uint64 { return index }
	t.mockEthClock.EXPECT().ComputeForkDigest(testSlot/testSlotsPerEpoch).Return(common.Bytes4{1, 2, 3, 4}, nil)
	publisher.EXPECT().PublishToForkDigest(gomock.Any(), gomock.Any(), gossip.TopicNameDataColumnSidecar(1), gomock.Any()).Return(nil).Times(1)

	t.dataColumnSidecarService.PublishColumns(t.T().Context(), []*cltypes.DataColumnSidecar{createMockGloasDataColumnSidecar(testSlot, 1, testBlockRoot)})
}

// Recovery also runs for blocks backfilled below finality, which fork choice never imports; peers no longer need their
// columns.
func (t *dataColumnSidecarTestSuite) TestGloasPublishColumns_WhenBlockCannotBeImported_DoesNotPublish() {
	t.finalizeGloasEpochAfterTestSlot()
	publisher := gossipmock.NewMockGossip(t.gomockCtrl)
	t.dataColumnSidecarService.(*dataColumnSidecarService).gossipManager = publisher
	publisher.EXPECT().PublishToForkDigest(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	t.dataColumnSidecarService.PublishColumns(t.T().Context(), []*cltypes.DataColumnSidecar{createMockGloasDataColumnSidecar(testSlot, 1, testBlockRoot)})

	_, seen := t.dataColumnSidecarService.(*dataColumnSidecarService).seenGloasSidecar.Get(seenGloasSidecarKey{beaconBlockRoot: testBlockRoot, index: 1})
	t.False(seen)
}

func (t *dataColumnSidecarTestSuite) TestGloasProcessMessage_WhenArchiveMode_StoresSidecar() {
	peerDas := t.archivePeerDasWithCustodyColumn0()
	peerDas.EXPECT().IsColumnOverHalf(testSlot, testBlockRoot).Return(false).AnyTimes()
	peerDas.EXPECT().IsBlobAlreadyRecovered(testBlockRoot).Return(false).AnyTimes()
	t.assertGloasArchiveStoresSidecar(peerDas, 1)
}

// A custody column is stored even when the archive node can recover the blobs, so a column that recovery failed to
// write is still stored from gossip.
func (t *dataColumnSidecarTestSuite) TestGloasProcessMessage_WhenArchiveAndColumnsOverHalf_StoresCustodySidecar() {
	t.assertGloasArchiveStoresSidecar(t.archivePeerDas(false), 0)
}

func (t *dataColumnSidecarTestSuite) assertGloasArchiveStoresSidecar(peerDas *das_mock.MockPeerDas, index uint64) {
	t.activateGloas()
	t.mockForkChoice.MockPeerDas = peerDas
	peerDas.EXPECT().TryScheduleRecover(testSlot, testBlockRoot).Return(nil).Times(1)
	verifyDataColumnSidecarWithCommitments = t.mockFuncs.VerifyDataColumnSidecarWithCommitments
	verifyDataColumnSidecarKZGProofsWithCommitments = t.mockFuncs.VerifyDataColumnSidecarKZGProofsWithCommitments
	t.mockSyncedData.EXPECT().Syncing().Return(false)
	t.mockEthClock.EXPECT().GetCurrentSlot().Return(testSlot).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarWithCommitments", gomock.Any(), gomock.Any()).Return(true).AnyTimes()
	t.mockFuncs.ctrl.RecordCall(t.mockFuncs, "VerifyDataColumnSidecarKZGProofsWithCommitments", gomock.Any(), gomock.Any()).Return(true).AnyTimes()
	t.importBlock(testBlockRoot, createMockGloasBlock(testSlot))
	t.mockColumnSidecarStorage.EXPECT().WriteColumnSidecars(gomock.Any(), testBlockRoot, int64(index), gomock.Any()).Return(nil).Times(1)

	err := t.dataColumnSidecarService.ProcessMessage(t.T().Context(), nil, createMockGloasDataColumnSidecar(testSlot, index, testBlockRoot))

	t.NoError(err)
}
