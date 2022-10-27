package main

import (
	"bytes"
	"fmt"
	"io"

	"github.com/golang/snappy"
	"github.com/ledgerwatch/erigon/cmd/lightclient/cltypes"
)

func main() {
	// Convert packet to slice of bytes.
	fullPacket := make([]byte, len(testFinalityUpdatePacket))
	for i, v := range testFinalityUpdatePacket {
		fullPacket[i] = byte(v)
	}
	fmt.Printf("Hex Packet: %x\n", fullPacket[:])

	// We are reading a test finality update object.
	result := &cltypes.LightClientFinalityUpdate{}
	ln := result.SizeSSZ() // size = 24896

	// Read first six bytes.
	r := bytes.NewReader(fullPacket)
	r.Read(make([]byte, 6))

	// Now we can construct the snappy reader.
	sr := snappy.NewReader(r)
	decompressed := make([]byte, ln)

	// Do the snappy decompression.
	_, err := io.ReadFull(sr, decompressed)
	if err != nil {
		fmt.Printf("unable to decompress data: %v\n", err)
		return
	}

	// Unmarshal into our result object.
	err = result.UnmarshalSSZ(decompressed)
	if err != nil {
		fmt.Printf("unable to unmarshall data: %v\n", err)
		return
	}

	fmt.Printf("decoded object: %+v\n", result)
}
