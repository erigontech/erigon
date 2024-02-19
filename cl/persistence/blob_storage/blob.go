package blob_storage

import (
	"fmt"
	"os"
	"strings"
	"sync"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"go.uber.org/zap/buffer"
)

var bPool = sync.Pool{
	New: func() interface{} {
		return &buffer.Buffer{}
	},
}

const subDivisionFolderSize = 10_000

// BlobStorage is saving and retrieving BlobSidecars.
type aferoBlobStorage struct {
	fs  afero.Fs
	cfg *clparams.BeaconChainConfig

	blockRootToBlobs          sync.Map
	blockRootToKzgCommitments sync.Map
	kzgCommitmentToBlob       sync.Map
}

func NewBlobStorage(fs afero.Fs, cfg *clparams.BeaconChainConfig) *aferoBlobStorage {
	return &aferoBlobStorage{
		fs:  fs,
		cfg: cfg,

		blockRootToBlobs:          sync.Map{},
		blockRootToKzgCommitments: sync.Map{},
		kzgCommitmentToBlob:       sync.Map{},
	}
}

func rootToPaths(slot uint64, root libcommon.Hash, config *clparams.BeaconChainConfig) (folderPath, path string) {
	// bufio
	buffer := bPool.Get().(*buffer.Buffer)
	defer bPool.Put(buffer)
	buffer.Reset()

	// slot/root
	fmt.Fprintf(buffer, "%d/%x.sz", slot/subDivisionFolderSize, root)
	split := strings.Split(buffer.String(), "/")
	return split[0], buffer.String()
}

func (a *aferoBlobStorage) WriteBlob(blobSidecar *cltypes.BlobSidecar) error {
	slot := blobSidecar.SignedBlockHeader.Header.Slot
	blockRoot, _ := blobSidecar.SignedBlockHeader.Header.HashSSZ()

	folderPath, path := rootToPaths(slot, blockRoot, a.cfg)
	_ = a.fs.MkdirAll(folderPath, 0o755)

	w, err := a.fs.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o755)
	if err != nil {
		return err
	}
	defer w.Close()

	err = ssz_snappy.EncodeAndWrite(w, blobSidecar)
	if err != nil {
		return err
	}
	notify(a, blobSidecar, blockRoot)
	return nil
}

func notify(a *aferoBlobStorage, blobSidecar *cltypes.BlobSidecar, blockRoot [32]byte) {
	//blockRoot=>[]blobs
	blobs, ok := a.blockRootToBlobs.Load(blockRoot)
	if !ok {
		blobs = []cltypes.Blob{}
	}
	blobs = append(blobs.([]cltypes.Blob), blobSidecar.Blob)
	a.blockRootToBlobs.Store(blockRoot, blobs)

	//blockRoot=>[]kzgCommitments
	kzgCommitments, ok := a.blockRootToKzgCommitments.Load(blockRoot)
	if !ok {
		kzgCommitments = []libcommon.Bytes48{}
	}
	kzgCommitments = append(kzgCommitments.([]libcommon.Bytes48), blobSidecar.KzgCommitment)
	a.blockRootToKzgCommitments.Store(blockRoot, kzgCommitments)

	//kzgCommitments=>blob
	a.kzgCommitmentToBlob.Store(blobSidecar.KzgCommitment, blobSidecar.Blob)
}

func (a *aferoBlobStorage) ReadBlobsByBlockRoot(blockRoot [32]byte) ([]cltypes.Blob, error) {
	blobs, ok := a.blockRootToBlobs.Load(blockRoot)
	if !ok {
		return nil, errors.New("block root not found")
	}
	return blobs.([]cltypes.Blob), nil
}

func (a *aferoBlobStorage) ReadKzgCommitmentsForBlockRoot(blockRoot [32]byte) ([]libcommon.Bytes48, error) {
	kzgCommitments, ok := a.blockRootToKzgCommitments.Load(blockRoot)
	if !ok {
		return nil, errors.New("block root not found")
	}
	return kzgCommitments.([]libcommon.Bytes48), nil
}

func (a *aferoBlobStorage) ReadBlobByKzgCommitment(kzgCommitment libcommon.Bytes48) (cltypes.Blob, error) {
	blob, ok := a.kzgCommitmentToBlob.Load(kzgCommitment)
	if !ok {
		return cltypes.Blob{}, errors.New("kzg commitment not found")
	}
	return blob.(cltypes.Blob), nil
}

func (a *aferoBlobStorage) PruneBlobs(currentSlot uint64) error {
	a.blockRootToBlobs.Range(func(key, value interface{}) bool {
		blobSidecar := value.(*cltypes.BlobSidecar)
		if blobSidecar.SignedBlockHeader.Header.Slot < currentSlot-a.cfg.MinEpochsForBlobsSidecarsRequest {
			// Clean blockRootToBlobs
			a.blockRootToBlobs.Delete(key)

			// Clean kzgCommitmentToBlob
			kzgCommitments, ok := a.blockRootToKzgCommitments.Load(key)
			if !ok {
				kzgCommitments = []libcommon.Bytes48{}
			}
			for _, kzgCommitment := range kzgCommitments.([]libcommon.Bytes48) {
				a.kzgCommitmentToBlob.Delete(kzgCommitment)
			}

			// Clean blockRootToKzgCommitments
			a.blockRootToKzgCommitments.Delete(key)

			// Delete the file
			blockRoot, _ := blobSidecar.SignedBlockHeader.Header.HashSSZ()
			_, path := rootToPaths(blobSidecar.SignedBlockHeader.Header.Slot, blockRoot, a.cfg)
			if err := a.fs.Remove(path); err != nil {
				return false
			}
		}
		return true
	})
	return nil
}

func (a *aferoBlobStorage) retrieveBlobsAndProofs(beaconBlockRoot [32]byte) ([]cltypes.Blob, []libcommon.Bytes48, error) {
	blobs, err := a.ReadBlobsByBlockRoot(beaconBlockRoot)
	if err != nil {
		return nil, nil, err
	}
	kzgCommitments, err := a.ReadKzgCommitmentsForBlockRoot(beaconBlockRoot)
	if err != nil {
		return nil, nil, err
	}
	return blobs, kzgCommitments, nil
}
