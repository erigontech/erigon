// Copyright 2015 The draw2d Authors. All rights reserved.
// created: 26/06/2015 by Stani Michiels

package draw2dpdf

import "github.com/jung-kurt/gofpdf"

// SaveToPdfFile creates and saves a pdf document to a file
func SaveToPdfFile(filePath string, pdf *gofpdf.Fpdf) error {
	return pdf.OutputFileAndClose(filePath)
}
