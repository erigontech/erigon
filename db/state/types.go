package state

import "github.com/erigontech/erigon/db/kv"

/** custom types **/
type Num = kv.Num
type RootNum = kv.RootNum

// sequence number of entity - might contain non-canonical values
type Id uint64

type Bytes = []byte

// file extensions

type AccessorExtension string

const (
	AccessorExtensionIdx AccessorExtension = ".idx"
	AccessorExtensionKvi AccessorExtension = ".kvi"
	AccessorExtensionVi  AccessorExtension = ".vi"
	AccessorExtensionEfi AccessorExtension = ".efi"
)

func (a AccessorExtension) IsSet() bool {
	switch a {
	case AccessorExtensionIdx, AccessorExtensionKvi, AccessorExtensionVi, AccessorExtensionEfi:
		return true
	}
	return false
}

func (a AccessorExtension) String() string {
	return string(a)
}

func (a AccessorExtension) Equals(target string) bool {
	return string(a) == target
}

type DataExtension string

const (
	DataExtensionSeg DataExtension = ".seg"
	DataExtensionKv  DataExtension = ".kv"
	DataExtensionV   DataExtension = ".v"
	DataExtensionEf  DataExtension = ".ef"
)

func (d DataExtension) IsSet() bool {
	switch d {
	case DataExtensionSeg, DataExtensionKv, DataExtensionV, DataExtensionEf:
		return true
	}
	return false
}

func (d DataExtension) String() string {
	return string(d)
}

func (d DataExtension) Equals(target string) bool {
	return string(d) == target
}
