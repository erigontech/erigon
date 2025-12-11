package peerdasutils

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/p2p/enode"
	ckzg "github.com/ethereum/c-kzg-4844/v2/bindings/go"
	"github.com/holiman/uint256"
)

// CustodyIndex represents the index of a custody group
type CustodyIndex = cltypes.CustodyIndex

// ColumnIndex represents the index of a column in the matrix
type ColumnIndex = cltypes.ColumnIndex

// RowIndex represents the index of a row in the matrix
type RowIndex = cltypes.RowIndex

var (
	maxUint256 = new(uint256.Int).Sub(new(uint256.Int).Lsh(uint256.NewInt(1), 256), uint256.NewInt(1))
)

// GetCustodyGroups generates custody groups for a given node ID.
// This function is re-entrant and thread-safe.
func GetCustodyGroups(nodeID enode.ID, custodyGroupCount uint64) ([]CustodyIndex, error) {
	cfg := clparams.GetBeaconConfig()
	if custodyGroupCount > cfg.NumberOfCustodyGroups {
		return nil, fmt.Errorf("custody group count %d exceeds maximum allowed %d", custodyGroupCount, cfg.NumberOfCustodyGroups)
	}
	currentID := uint256.NewInt(0).SetBytes(nodeID.Bytes())
	custodyGroups := make([]CustodyIndex, 0)
	custodyGroupLookup := make(map[CustodyIndex]bool)
	for uint64(len(custodyGroups)) < custodyGroupCount {
		// Hash current ID and take first 8 bytes
		idBytes := currentID.Bytes32()
		// Reverse the bytes to convert from big-endian to little-endian.
		// This ensures compatibility with the hashing process that follows.
		for i := 0; i < len(idBytes)/2; i++ {
			idBytes[i], idBytes[len(idBytes)-i-1] = idBytes[len(idBytes)-i-1], idBytes[i]
		}
		hash := sha256.Sum256(idBytes[:])
		custodyGroup := binary.LittleEndian.Uint64(hash[:8]) % cfg.NumberOfCustodyGroups

		// Check if custody group already exists
		if _, ok := custodyGroupLookup[custodyGroup]; !ok {
			custodyGroups = append(custodyGroups, custodyGroup)
			custodyGroupLookup[custodyGroup] = true
		}

		// Increment currentID with overflow protection
		if currentID.Cmp(maxUint256) == 0 {
			currentID = uint256.NewInt(0)
		} else {
			currentID.Add(currentID, uint256.NewInt(1))
		}
	}

	// Sort custody groups
	slices.Sort(custodyGroups)

	return custodyGroups, nil
}

// ComputeColumnsForCustodyGroup returns the column indices that belong to a given custody group.
// This function is re-entrant and thread-safe.
func ComputeColumnsForCustodyGroup(custodyGroup CustodyIndex) ([]ColumnIndex, error) {
	numberOfCustodyGroups := clparams.GetBeaconConfig().NumberOfCustodyGroups
	numberOfColumns := clparams.GetBeaconConfig().NumberOfColumns

	if custodyGroup >= numberOfCustodyGroups {
		return nil, fmt.Errorf("custody group %d is greater than or equal to the number of custody groups (%d)", custodyGroup, numberOfCustodyGroups)
	}

	columnsPerGroup := numberOfColumns / numberOfCustodyGroups
	columns := make([]ColumnIndex, columnsPerGroup)

	for i := ColumnIndex(0); i < columnsPerGroup; i++ {
		columns[i] = numberOfCustodyGroups*i + custodyGroup
	}

	return columns, nil
}

// ComputeMatrix takes a slice of blobs and returns a flattened sequence of matrix entries.
// This function is re-entrant and thread-safe.
func ComputeMatrix(blobs [][]byte) ([]cltypes.MatrixEntry, error) {
	numberOfColumns := clparams.GetBeaconConfig().NumberOfColumns
	matrix := make([]cltypes.MatrixEntry, 0, len(blobs)*int(numberOfColumns))

	for blobIndex, blob := range blobs {
		cells, proofs, err := ComputeCellsAndKZGProofs(blob) // in kzg pkg
		if err != nil {
			return nil, fmt.Errorf("failed to compute cells and proofs for blob %d: %w", blobIndex, err)
		}

		for cellIndex := range cells {
			matrix = append(matrix, cltypes.MatrixEntry{
				Cell:        cells[cellIndex],
				KzgProof:    proofs[cellIndex],
				RowIndex:    uint64(blobIndex),
				ColumnIndex: ColumnIndex(cellIndex),
			})
		}
	}

	return matrix, nil
}

// RecoverMatrix takes a partial matrix and the total blob count and returns a complete matrix.
func RecoverMatrix(partialMatrix []cltypes.MatrixEntry, blobCount uint64) ([][]cltypes.MatrixEntry, error) {
	matrix := make([][]cltypes.MatrixEntry, 0, blobCount)

	// Process each blob row
	for blobIndex := uint64(0); blobIndex < blobCount; blobIndex++ {
		// Get cells and column indices for this blob row
		var cellIndices []ColumnIndex
		var cells []cltypes.Cell
		for _, entry := range partialMatrix {
			if entry.RowIndex == blobIndex {
				cellIndices = append(cellIndices, entry.ColumnIndex)
				cells = append(cells, entry.Cell)
			}
		}

		// Recover the full row of cells and proofs
		recoveredCells, recoveredProofs, err := RecoverCellsAndKZGProofs(cellIndices, cells) // in kzg pkg
		if err != nil {
			return nil, fmt.Errorf("failed to recover cells and proofs for blob %d: %w", blobIndex, err)
		}

		// Add recovered entries to matrix
		blobEntries := make([]cltypes.MatrixEntry, 0, len(recoveredCells))
		for index := range recoveredCells {
			blobEntries = append(blobEntries, cltypes.MatrixEntry{
				Cell:        recoveredCells[index],
				KzgProof:    recoveredProofs[index],
				RowIndex:    blobIndex,
				ColumnIndex: ColumnIndex(index),
			})
		}
		matrix = append(matrix, blobEntries)
	}

	return matrix, nil
}

// RecoverCellsAndKZGProofs uses the c-kzg-4844 library to recover the cells and proofs.
func RecoverCellsAndKZGProofs(cellIndices []ColumnIndex, cells []cltypes.Cell) ([]cltypes.Cell, []cltypes.KZGProof, error) {
	ckzgCells := make([]ckzg.Cell, len(cells))
	for i, cell := range cells {
		ckzgCells[i] = ckzg.Cell(cell)
	}

	// recover cells and proofs
	recoveredCells, recoveredProofs, err := ckzg.RecoverCellsAndKZGProofs(cellIndices, ckzgCells)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to recover cells and proofs: %w", err)
	}

	convertCells := make([]cltypes.Cell, len(recoveredCells))
	for i := range recoveredCells {
		convertCells[i] = cltypes.Cell(recoveredCells[i])
	}

	convertProofs := make([]cltypes.KZGProof, len(recoveredProofs))
	for i := range recoveredProofs {
		convertProofs[i] = cltypes.KZGProof(recoveredProofs[i])
	}
	return convertCells, convertProofs, nil
}

// ComputeCellsAndKZGProofs uses the c-kzg-4844 library to compute the cells and proofs.
func ComputeCellsAndKZGProofs(blob []byte) ([]cltypes.Cell, []cltypes.KZGProof, error) {
	ckzgBlob := ckzg.Blob{}
	if len(blob) != len(ckzgBlob) {
		return nil, nil, fmt.Errorf("blob length mismatch: %d != %d", len(blob), len(ckzgBlob))
	}
	copy(ckzgBlob[:], blob)
	cells, proofs, err := ckzg.ComputeCellsAndKZGProofs(&ckzgBlob)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to compute cells and proofs: %w", err)
	}

	convertCells := make([]cltypes.Cell, len(cells))
	for i := range cells {
		convertCells[i] = cltypes.Cell(cells[i])
	}

	convertProofs := make([]cltypes.KZGProof, len(proofs))
	for i := range proofs {
		convertProofs[i] = cltypes.KZGProof(proofs[i])
	}

	return convertCells, convertProofs, nil
}

func GetCustodyColumns(nodeID enode.ID, cgc uint64) (map[cltypes.CustodyIndex]bool, error) {
	// TODO: cache the following computations in terms of custody columns
	sampleSize := max(clparams.GetBeaconConfig().SamplesPerSlot, cgc)
	groups, err := GetCustodyGroups(nodeID, sampleSize)
	if err != nil {
		return nil, err
	}
	// compute all required custody columns
	custodyColumns := map[cltypes.CustodyIndex]bool{}
	for _, group := range groups {
		columns, err := ComputeColumnsForCustodyGroup(group)
		if err != nil {
			return nil, err
		}
		for _, column := range columns {
			custodyColumns[column] = true
		}
	}
	return custodyColumns, nil
}
