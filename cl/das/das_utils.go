package das

import (
	"encoding/binary"
	"fmt"
	"math/big"
	"sort"

	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-p2p/enode"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	ckzg "github.com/ethereum/c-kzg-4844/v2/bindings/go"
)

type (
	CustodyIndex = cltypes.CustodyIndex
	ColumnIndex  = cltypes.ColumnIndex
	RowIndex     = cltypes.RowIndex
)

var (
	maxUint256 = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
)

// GetCustodyGroups generates custody groups for a given node ID.
// This function is re-entrant and thread-safe.
func GetCustodyGroups(nodeId enode.ID, custodyGroupCount uint64) ([]CustodyIndex, error) {
	cfg := clparams.GetBeaconConfig()
	if custodyGroupCount > cfg.NumberOfCustodyGroups {
		return nil, fmt.Errorf("custody group count %d exceeds maximum allowed %d", custodyGroupCount, cfg.NumberOfCustodyGroups)
	}
	currentId, ok := new(big.Int).SetString(nodeId.String(), 16)
	if !ok {
		return nil, fmt.Errorf("failed to convert nodeId %s to big int", nodeId.String())
	}
	custodyGroups := make([]CustodyIndex, 0)
	for uint64(len(custodyGroups)) < custodyGroupCount {
		// Hash current ID and take first 8 bytes
		hash := crypto.Keccak256(currentId.Bytes())
		custodyGroup := binary.LittleEndian.Uint64(hash[:8]) % cfg.NumberOfCustodyGroups

		// Check if custody group already exists
		exists := false
		for _, g := range custodyGroups {
			if g == CustodyIndex(custodyGroup) {
				exists = true
				break
			}
		}

		if !exists {
			custodyGroups = append(custodyGroups, CustodyIndex(custodyGroup))
		}

		// Increment currentId with overflow protection
		if currentId.Cmp(maxUint256) == 0 {
			currentId.SetInt64(0)
		} else {
			currentId.Add(currentId, big.NewInt(1))
		}
	}

	// Sort custody groups
	sort.Slice(custodyGroups, func(i, j int) bool {
		return custodyGroups[i] < custodyGroups[j]
	})

	return custodyGroups, nil
}

// ComputeColumnsForCustodyGroup returns the column indices that belong to a given custody group.
// This function is re-entrant and thread-safe.
func ComputeColumnsForCustodyGroup(custodyGroup CustodyIndex) ([]ColumnIndex, error) {
	numberOfCustodyGroups := clparams.GetBeaconConfig().NumberOfCustodyGroups
	numberOfColumns := clparams.GetBeaconConfig().NumberOfColumns

	if custodyGroup >= CustodyIndex(numberOfCustodyGroups) {
		return nil, fmt.Errorf("custody group %d is greater than or equal to the number of custody groups (%d)", custodyGroup, numberOfCustodyGroups)
	}

	columnsPerGroup := numberOfColumns / numberOfCustodyGroups
	columns := make([]ColumnIndex, columnsPerGroup)

	for i := ColumnIndex(0); i < ColumnIndex(columnsPerGroup); i++ {
		columns[i] = ColumnIndex(numberOfCustodyGroups*uint64(i) + uint64(custodyGroup))
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
				RowIndex:    RowIndex(blobIndex),
				ColumnIndex: ColumnIndex(cellIndex),
			})
		}
	}

	return matrix, nil
}

// RecoverMatrix takes a partial matrix and the total blob count and returns a complete matrix.
func RecoverMatrix(partialMatrix []cltypes.MatrixEntry, blobCount uint64) ([]cltypes.MatrixEntry, error) {
	numberOfColumns := clparams.GetBeaconConfig().NumberOfColumns
	matrix := make([]cltypes.MatrixEntry, 0, blobCount*numberOfColumns)

	// Process each blob row
	for blobIndex := uint64(0); blobIndex < blobCount; blobIndex++ {
		// Get cells and column indices for this blob row
		var cellIndices []ColumnIndex
		var cells []cltypes.Cell
		for _, entry := range partialMatrix {
			if entry.RowIndex == RowIndex(blobIndex) {
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
		for cellIndex := range recoveredCells {
			matrix = append(matrix, cltypes.MatrixEntry{
				Cell:        recoveredCells[cellIndex],
				KzgProof:    recoveredProofs[cellIndex],
				RowIndex:    RowIndex(blobIndex),
				ColumnIndex: ColumnIndex(cellIndex),
			})
		}
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
