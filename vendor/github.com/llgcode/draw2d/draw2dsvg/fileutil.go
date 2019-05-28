package draw2dsvg

import (
	"encoding/xml"
	_ "errors"
	"os"
)

func SaveToSvgFile(filePath string, svg *Svg) error {
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	f.Write([]byte(xml.Header))
	encoder := xml.NewEncoder(f)
	encoder.Indent("", "\t")
	err = encoder.Encode(svg)

	return err
}
