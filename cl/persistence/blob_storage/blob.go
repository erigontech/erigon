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

// BlobStorage is the concrete implementation of the filesystem backend for saving and retrieving BlobSidecars.
type aferoBlobStorage struct {
	fs  afero.Fs
	cfg *clparams.BeaconChainConfig

	blockRootToSidecar sync.Map
}

func NewBlobStorage(fs afero.Fs, cfg *clparams.BeaconChainConfig) *aferoBlobStorage {
	return &aferoBlobStorage{
		fs:  fs,
		cfg: cfg,

		blockRootToSidecar: sync.Map{},
	}
}

func rootToPaths(slot uint64, root libcommon.Hash, config *clparams.BeaconChainConfig) (folderPath, path string) {
	// bufio
	buffer := bPool.Get().(*buffer.Buffer)
	defer bPool.Put(buffer)
	buffer.Reset()

	fmt.Fprintf(buffer, "%d/%x.sz", slot/subDivisionFolderSize, root)
	split := strings.Split(buffer.String(), "/")
	return split[0], buffer.String()
}

func (a *aferoBlobStorage) BlobWriter(blobSidecar *cltypes.BlobSidecar) error {
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
	processBlock(a, blobSidecar, blockRoot)
	return nil
}

// BlobSidecarReader retrieves a single BlobSidecar by its root
func (a *aferoBlobStorage) BlobReader(blockRoot [32]byte) (*cltypes.BlobSidecar, error) {
	blobSidecar, err := a.getBlobSidecarsForBlockRoot(blockRoot)
	if err != nil {
		return nil, err
	}

	slot := blobSidecar.SignedBlockHeader.Header.Slot

	_, path := rootToPaths(slot, blockRoot, a.cfg)
	file, err := a.fs.OpenFile(path, os.O_RDONLY, 0o755)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return blobSidecar, nil
}

func (a *aferoBlobStorage) PruneBlobs(currentSlot uint64) error {
	a.blockRootToSidecar.Range(func(key, value interface{}) bool {
		blobSidecar := value.(*cltypes.BlobSidecar)
		if blobSidecar.SignedBlockHeader.Header.Slot < currentSlot-a.cfg.MinEpochsForBlobsSidecarsRequest {
			// Remove the sidecar from the map
			a.blockRootToSidecar.Delete(key)

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

func processBlock(a *aferoBlobStorage, blobSidecar *cltypes.BlobSidecar, blockRoot [32]byte) {
	a.blockRootToSidecar.Store(blockRoot, blobSidecar)
}

// getBlobsForBlockRoot retrieves blob for a given block root.
func (a *aferoBlobStorage) getBlobSidecarsForBlockRoot(blockRoot [32]byte) (*cltypes.BlobSidecar, error) {
	blobSidecars, ok := a.blockRootToSidecar.Load(blockRoot)
	if !ok {
		return nil, errors.New("block root not found")
	}
	return blobSidecars.(*cltypes.BlobSidecar), nil
}

func (a *aferoBlobStorage) retrieveBlobsAndProofs(beaconBlockRoot [32]byte) ([]cltypes.Blob, []libcommon.Bytes48, error) {
	sidecars, ok := a.blockRootToSidecar.Load(beaconBlockRoot)

	if !ok {
		return nil, nil, errors.New("block root not found")
	}
	var blobs []cltypes.Blob
	var proofs []libcommon.Bytes48

	//FIXME: Load list of sidecars
	sidecar := sidecars.(*cltypes.BlobSidecar)
	blobs = append(blobs, sidecar.Blob)
	proofs = append(proofs, sidecar.KzgProof)

	// sidecarList, ok := sidecars.([]*cltypes.BlobSidecar)
	// if !ok {
	// 	return nil, nil, errors.New("invalid sidecar type")
	// }

	// for _, sidecar := range sidecarList {
	// 	blobs = append(blobs, sidecar.Blob)
	// 	proofs = append(proofs, sidecar.KzgProof)
	// }
	return blobs, proofs, nil
}
