package vm

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidEOFInitcode = errors.New("invalid eof initcode")
)

const (
	initcode byte = 0
	runtime  byte = 1

	kindTypes     byte = 1
	kindCode      byte = 2
	kindContainer byte = 3
	kindData      byte = 4

	maxInitCodeSize = 49152

	nonReturningFunction = 0x80
	maxStackHeight       = 1023

	maxInputItems  = 127
	maxOutputItems = 127

	stackSizeLimit = 1024
)

const ( // EOFv1 Opcodes
	RJUMP  OpCode = 0xe0
	RJUMPI OpCode = 0xe1
	RJUMPV OpCode = 0xe2

	CALLF OpCode = 0xe3
	RETF  OpCode = 0xe4
	JUMPF OpCode = 0xe5

	DUPN     OpCode = 0xe6
	SWAPN    OpCode = 0xe7
	EXCHANGE OpCode = 0xe8

	EOFCREATE  OpCode = 0xec
	RETURNCODE OpCode = 0xee

	DATALOAD  OpCode = 0xd0
	DATALOADN OpCode = 0xd1
	DATASIZE  OpCode = 0xd2
	DATACOPY  OpCode = 0xd3

	RETURNDATALOAD  OpCode = 0xf7
	EXTCALL         OpCode = 0xf8
	EXTDELEGATECALL OpCode = 0xf9
	EXTSTATICCALL   OpCode = 0xfb
)

func isEOFcode(code []byte) bool {
	return len(code) >= 2 && code[0] == 0xEF && code[1] == 0x00
}

func isSupportedVersion(version byte) bool {
	return version == 1 // || version == 2 etc.
}

type eofHeader struct {
	version          byte
	typesOffset      uint16
	codeSizes        []uint16
	codeOffsets      []uint16
	containerSizes   []uint16
	containerOffsets []uint16
	dataSize         uint16
	dataSizePos      uint16
	dataOffset       uint16
}

func ParseEOFHeader(eofCode []byte, jt *JumpTable, containerKind byte, validate bool, depth int) (*eofHeader, error) {

	header := &eofHeader{}
	if len(eofCode) < 14 {
		return nil, fmt.Errorf("EOFException.CODE_TOO_SHORT")
	}
	if len(eofCode) > maxInitCodeSize {
		return nil, fmt.Errorf("EOFException.CONTAINER_SIZE_ABOVE_LIMIT")
	}
	if !isEOFcode(eofCode) { // additional check for recursive container validations
		return nil, fmt.Errorf("EOFException.INVALID_MAGIC")
	}

	var offset uint16 = 2
	header.version = eofCode[offset]
	if !isSupportedVersion(header.version) {
		return nil, fmt.Errorf("EOFException.INVALID_VERSION")
	}
	offset++ // version

	if eofCode[offset] != kindTypes {
		return nil, fmt.Errorf("EOFException.MISSING_TYPE_HEADER")
	}
	offset++ // kind types

	typesSizes := uint16(eofCode[offset])<<8 | uint16(eofCode[offset+1])
	if typesSizes < 4 || typesSizes%4 != 0 {
		return nil, fmt.Errorf("EOFException.INVALID_TYPE_SECTION_SIZE")
	}
	if typesSizes/4 > 1024 {
		return nil, fmt.Errorf("EOFException.TOO_MANY_CODE_SECTIONS")
	}
	offset += 2 // type sizes

	if eofCode[offset] != kindCode {
		return nil, fmt.Errorf("EOFException.MISSING_CODE_HEADER|EOFException.UNEXPECTED_HEADER_KIND")
	}
	offset++ // kind code

	numCodeSections := uint16(eofCode[offset])<<8 | uint16(eofCode[offset+1])
	offset += 2 // num code sections

	if numCodeSections == 0 {
		return nil, fmt.Errorf("EOFException.ZERO_SECTION_SIZE|EOFException.INCOMPLETE_SECTION_NUMBER")
	}
	if numCodeSections != typesSizes/4 {
		return nil, fmt.Errorf("EOFException.INVALID_TYPE_SECTION_SIZE|EOFException.INVALID_SECTION_BODIES_SIZE")
	}
	// if numCodeSections > 1024 { // no need to check this
	// 	return nil, fmt.Errorf("EOF code sections == 0")
	// }

	for i := uint16(0); i < numCodeSections; i++ {
		if offset+1 >= uint16(len(eofCode)) {
			return nil, fmt.Errorf("EOFException.INCOMPLETE_SECTION_SIZE")
		}
		size := uint16(eofCode[offset])<<8 | uint16(eofCode[offset+1])
		if size == 0 {
			return nil, fmt.Errorf("EOFException.ZERO_SECTION_SIZE")
		}
		header.codeSizes = append(header.codeSizes, size)
		offset += 2
	} // code sizes
	if offset >= uint16(len(eofCode)) {
		return nil, fmt.Errorf("EOFException.INCOMPLETE_SECTION_SIZE")
	}

	if eofCode[offset] != kindContainer && eofCode[offset] != kindData {
		return nil, fmt.Errorf("EOFException.MISSING_DATA_SECTION|EOFException.UNEXPECTED_HEADER_KIND")
	}

	if eofCode[offset] == kindContainer {
		offset++
		numContainerSections := uint16(eofCode[offset])<<8 | uint16(eofCode[offset+1])
		offset += 2
		if numContainerSections > 256 {
			return nil, fmt.Errorf("EOFException.TOO_MANY_CONTAINERS")
		}
		for i := uint16(0); i < numContainerSections; i++ {
			if offset+1 >= uint16(len(eofCode)) {
				return nil, fmt.Errorf("EOFException.INCOMPLETE_SECTION_SIZE")
			}
			size := uint16(eofCode[offset])<<8 | uint16(eofCode[offset+1])
			if size == 0 {
				return nil, fmt.Errorf("EOFException.ZERO_SECTION_SIZE")
			}
			header.containerSizes = append(header.containerSizes, size)
			offset += 2
		} // container sizes

		if len(header.containerSizes) == 0 {
			return nil, fmt.Errorf("EOFException.ZERO_SECTION_SIZE")
		}
	}
	if offset >= uint16(len(eofCode)) {
		return nil, fmt.Errorf("EOFException.INCOMPLETE_SECTION_SIZE")
	}
	// skip check for data section, already did above
	if eofCode[offset] != kindData {
		return nil, fmt.Errorf("EOFException.MISSING_DATA_SECTION|EOFException.UNEXPECTED_HEADER_KIND")
	}

	offset++
	if offset+1 >= uint16(len(eofCode)) {
		return nil, fmt.Errorf("EOFException.INCOMPLETE_SECTION_SIZE")
	}

	header.dataSizePos = offset
	header.dataSize = uint16(eofCode[offset])<<8 | uint16(eofCode[offset+1])
	offset += 2

	if offset >= uint16(len(eofCode)) {
		return nil, fmt.Errorf("EOFException.INCOMPLETE_SECTION_SIZE")
	}
	if eofCode[offset] != 0 {
		return nil, fmt.Errorf("EOFException.MISSING_TERMINATOR")
	}
	offset++ // terminator

	header.typesOffset = offset

	offset += typesSizes

	for i := uint16(0); i < numCodeSections; i++ {
		header.codeOffsets = append(header.codeOffsets, offset)
		offset += header.codeSizes[i]
	}

	if len(header.containerSizes) > 0 { // do we need this check?
		for i := range header.containerSizes {
			header.containerOffsets = append(header.containerOffsets, offset)
			offset += header.containerSizes[i]
		}
	}

	if validate {
		var err error
		referencedByEofCreate := make([]bool, len(header.containerSizes))
		referencedByReturnCode := make([]bool, len(header.containerSizes))

		// validate body
		// check if remaining bytes are enough for the body
		if err := validateBody(eofCode, header, containerKind, depth); err != nil {
			return nil, err
		}

		// validate types
		if offset, err = validateTypes(eofCode, header); err != nil {
			return nil, err
		}
		// validate code
		if err = validateCode(eofCode, header, offset, containerKind, jt, &referencedByEofCreate, &referencedByReturnCode); err != nil {
			return nil, err
		}

		for i := range header.codeSizes {
			offset += header.codeSizes[i]
		}

		// validate containers
		for i := range header.containerSizes {
			if _, err = validateContainer(eofCode, header, offset, i, referencedByEofCreate[i], referencedByReturnCode[i], jt, depth); err != nil {
				return nil, err
			}
			offset += header.containerSizes[i]
		}
	}

	header.dataOffset = offset

	return header, nil
}

func validateBody(code []byte, header *eofHeader, containerKind byte, depth int) error {

	offset := int(header.typesOffset) + 4*len(header.codeSizes)
	for i := range header.codeSizes {
		offset += int(header.codeSizes[i])
	}
	for i := range header.containerSizes {
		offset += int(header.containerSizes[i])
	}
	if offset > len(code) {
		return fmt.Errorf("EOFException.INVALID_SECTION_BODIES_SIZE")
	}
	// offset here is data offset
	if offset == len(code) &&
		header.dataSize > 0 && depth == 0 /* top level container */ {
		return fmt.Errorf("EOFException.TOPLEVEL_CONTAINER_TRUNCATED")
	}
	if offset+int(header.dataSize) > len(code) {
		if depth == 0 {
			return fmt.Errorf("EOFException.TOPLEVEL_CONTAINER_TRUNCATED")
		}
		if containerKind == initcode {
			return fmt.Errorf("EOFException.EOFCREATE_WITH_TRUNCATED_CONTAINER")
		}
	}

	if offset+int(header.dataSize) < len(code) {
		return fmt.Errorf("EOFException.INVALID_SECTION_BODIES_SIZE")
	}

	return nil
}

func validateTypes(code []byte, header *eofHeader) (uint16, error) {

	offset := header.typesOffset

	inputs := code[offset] // TODO: check if inputs okay?
	if inputs != 0 {
		return 0, fmt.Errorf("EOFException.INVALID_FIRST_SECTION_TYPE")
	}
	offset++

	outputs := code[offset]
	if outputs != nonReturningFunction {
		return 0, fmt.Errorf("EOFException.INVALID_FIRST_SECTION_TYPE")
	}
	offset++

	stackHeight := uint16(code[offset])<<8 | uint16(code[offset+1])
	if stackHeight > maxStackHeight {
		return 0, fmt.Errorf("EOFException.MAX_STACK_HEIGHT_ABOVE_LIMIT")
	}
	offset += 2

	for offset < header.codeOffsets[0] {
		inputs = code[offset]
		if inputs > maxInputItems {
			return 0, fmt.Errorf("EOFException.INPUTS_OUTPUTS_NUM_ABOVE_LIMIT")
		}
		offset++

		outputs = code[offset]
		if outputs > maxOutputItems && outputs != nonReturningFunction {
			return 0, fmt.Errorf("EOFException.INPUTS_OUTPUTS_NUM_ABOVE_LIMIT")
		}
		offset++

		stackHeight = uint16(code[offset])<<8 | uint16(code[offset+1])
		if stackHeight > maxStackHeight {
			return 0, fmt.Errorf("EOF type section stack height > maxStackHeight")
		}
		offset += 2
	}

	return offset, nil
}

func validateCode(eofCode []byte, header *eofHeader, offset uint16, containerKind byte, jt *JumpTable, referencedByEofCreate, referencedByReturnCode *[]bool) error {

	// at least one code section is guaranteed by spec

	visitedCodeSections := make([]bool, len(header.codeSizes))

	codeSectionsQueue := make([]uint16, 0)
	codeSectionsQueue = append(codeSectionsQueue, 0)

	var err error
	var subcontainerRefs [][2]uint16
	var accessCodeSections map[uint16]bool

	for len(codeSectionsQueue) > 0 {
		codeIdx := codeSectionsQueue[0]
		codeSectionsQueue = codeSectionsQueue[1:]

		if visitedCodeSections[codeIdx] {
			continue
		}
		visitedCodeSections[codeIdx] = true

		start := offset
		for i := uint16(0); i < codeIdx; i++ {
			start += header.codeSizes[i]
		}
		// codeSection := code[start : start+header.codeSizes[codeIdx]]
		// offset += header.codeSizes[codeIdx]
		if subcontainerRefs, accessCodeSections, err = validateInstructions(eofCode, codeIdx, header, start, containerKind, jt); err != nil {
			return err
		} else {
			for _, ref := range subcontainerRefs {
				if OpCode(ref[1]) == EOFCREATE {
					(*referencedByEofCreate)[ref[0]] = true
				} else if OpCode(ref[1]) == RETURNCODE {
					(*referencedByReturnCode)[ref[0]] = true
				}
			}
		}
		for idx := range accessCodeSections { // TODO: do we need a map here?
			codeSectionsQueue = append(codeSectionsQueue, idx)
		}
		if err := validateRjumpDestinations(eofCode, header, jt, codeIdx, start); err != nil {
			return err
		}
		if maxStackHeight, err := validateMaxStackHeight(eofCode, codeIdx, header, start, jt); err != nil {
			return err
		} else {
			typesOffset := getTypeSectionOffset(codeIdx, header)
			sectionMaxStack := int(eofCode[typesOffset+2])<<8 | int(eofCode[typesOffset+3])
			if maxStackHeight != sectionMaxStack {
				return fmt.Errorf("EOFException.INVALID_MAX_STACK_HEIGHT")
			}
		}
	}
	for _, visited := range visitedCodeSections {
		if !visited {
			return fmt.Errorf("EOFException.UNREACHABLE_CODE_SECTIONS")
		}
	}

	return nil
}

func validateContainer(eofCode []byte, header *eofHeader, offset uint16, containerIdx int, eofcreate, returnCode bool, jt *JumpTable, depth int) (*eofHeader, error) {

	container := eofCode[offset : offset+header.containerSizes[containerIdx]]

	if eofcreate && returnCode {
		return nil, fmt.Errorf("EOF container referenced by both eofcreate and returncode")
	}
	if !eofcreate && !returnCode {
		return nil, fmt.Errorf("EOFException.ORPHAN_SUBCONTAINER")
	}
	var err error
	var h *eofHeader
	if eofcreate {
		_, err = ParseEOFHeader(container, jt, initcode, true, depth+1)
	} else {
		_, err = ParseEOFHeader(container, jt, runtime, true, depth+1)
	}
	if err != nil {
		return nil, err
	}
	return h, nil
}

// given code 0xef00010100100200040008000a00040006040000000080000200000001008000000000000260006000e3000100600035e10001e4e50002e30003006001600055e4
// 0x
// ef00 - magic
// 01 - version
// 01 - kind type
// 0010 - type sizes (16/4=4 type sections)
// 02 - kind code
// 0004 - num code sections (4)
// 0008 - 1st code section size
// 000a - 2nd
// 0004 - 3d
// 0006 - 4th
// 04 - kind data
// 0000 - data size
// 00 - terminator
// 00 - inputs 1st type section
// 80 - outputs 1st type section (non returning function)
// 0002 - max stack height 1st section
// 00 - inputs 2nd
// 00 - outputs 2nd
// 0001 - max stack height 2nd
// 00 - inputs 3d
// 80 - outputs 3d (non returning function)
// 0000 - max stack height 3d
// 00 - inputs 4th
// 00 - outputs 4th
// 0002 - max stack heitgh 4th
// 60006000e3000100 - 1st code
// 600035e10001e4e50002 - 2nd code
// e3000300 - 3d code
// 6001600055e4 - 4th code

// 0x
// ef00
// 01 2
// 01 3
// 0004 5
// 02 6
// 0001 8
// 0801 10
// 04
// 0000
// 00
// 00
// 80
// 0102
// 610d00610d01610d02610d03610d04610d05610d06610d07610d08610d09610d0a610d0b610d0c610d0d610d0e610d0f610d10610d11610d12610d13610d14610d15610d16610d17610d18610d19610d1a610d1b610d1c610d1d610d1e610d1f610d20610d21610d22610d23610d24610d25610d26610d27610d28610d29610d2a610d2b610d2c610d2d610d2e610d2f610d30610d31610d32610d33610d34610d35610d36610d37610d38610d39610d3a610d3b610d3c610d3d610d3e610d3f610d40610d41610d42610d43610d44610d45610d46610d47610d48610d49610d4a610d4b610d4c610d4d610d4e610d4f610d50610d51610d52610d53610d54610d55610d56610d57610d58610d59610d5a610d5b610d5c610d5d610d5e610d5f610d60610d61610d62610d63610d64610d65610d66610d67610d68610d69610d6a610d6b610d6c610d6d610d6e610d6f610d70610d71610d72610d73610d74610d75610d76610d77610d78610d79610d7a610d7b610d7c610d7d610d7e610d7f610d80610d81610d82610d83610d84610d85610d86610d87610d88610d89610d8a610d8b610d8c610d8d610d8e610d8f610d90610d91610d92610d93610d94610d95610d96610d97610d98610d99610d9a610d9b610d9c610d9d610d9e610d9f610da0610da1610da2610da3610da4610da5610da6610da7610da8610da9610daa610dab610dac610dad610dae610daf610db0610db1610db2610db3610db4610db5610db6610db7610db8610db9610dba610dbb610dbc610dbd610dbe610dbf610dc0610dc1610dc2610dc3610dc4610dc5610dc6610dc7610dc8610dc9610dca610dcb610dcc610dcd610dce610dcf610dd0610dd1610dd2610dd3610dd4610dd5610dd6610dd7610dd8610dd9610dda610ddb610ddc610ddd610dde610ddf610de0610de1610de2610de3610de4610de5610de6610de7610de8610de9610dea610deb610dec610ded610dee610def610df0610df1610df2610df3610df4610df5610df6610df7610df8610df9610dfa610dfb610dfc610dfd610dfe610dffe600600055e601600155e602600255e603600355e604600455e605600555e606600655e607600755e608600855e609600955e60a600a55e60b600b55e60c600c55e60d600d55e60e600e55e60f600f55e610601055e611601155e612601255e613601355e614601455e615601555e616601655e617601755e618601855e619601955e61a601a55e61b601b55e61c601c55e61d601d55e61e601e55e61f601f55e620602055e621602155e622602255e623602355e624602455e625602555e626602655e627602755e628602855e629602955e62a602a55e62b602b55e62c602c55e62d602d55e62e602e55e62f602f55e630603055e631603155e632603255e633603355e634603455e635603555e636603655e637603755e638603855e639603955e63a603a55e63b603b55e63c603c55e63d603d55e63e603e55e63f603f55e640604055e641604155e642604255e643604355e644604455e645604555e646604655e647604755e648604855e649604955e64a604a55e64b604b55e64c604c55e64d604d55e64e604e55e64f604f55e650605055e651605155e652605255e653605355e654605455e655605555e656605655e657605755e658605855e659605955e65a605a55e65b605b55e65c605c55e65d605d55e65e605e55e65f605f55e660606055e661606155e662606255e663606355e664606455e665606555e666606655e667606755e668606855e669606955e66a606a55e66b606b55e66c606c55e66d606d55e66e606e55e66f606f55e670607055e671607155e672607255e673607355e674607455e675607555e676607655e677607755e678607855e679607955e67a607a55e67b607b55e67c607c55e67d607d55e67e607e55e67f607f55e680608055e681608155e682608255e683608355e684608455e685608555e686608655e687608755e688608855e689608955e68a608a55e68b608b55e68c608c55e68d608d55e68e608e55e68f608f55e690609055e691609155e692609255e693609355e694609455e695609555e696609655e697609755e698609855e699609955e69a609a55e69b609b55e69c609c55e69d609d55e69e609e55e69f609f55e6a060a055e6a160a155e6a260a255e6a360a355e6a460a455e6a560a555e6a660a655e6a760a755e6a860a855e6a960a955e6aa60aa55e6ab60ab55e6ac60ac55e6ad60ad55e6ae60ae55e6af60af55e6b060b055e6b160b155e6b260b255e6b360b355e6b460b455e6b560b555e6b660b655e6b760b755e6b860b855e6b960b955e6ba60ba55e6bb60bb55e6bc60bc55e6bd60bd55e6be60be55e6bf60bf55e6c060c055e6c160c155e6c260c255e6c360c355e6c460c455e6c560c555e6c660c655e6c760c755e6c860c855e6c960c955e6ca60ca55e6cb60cb55e6cc60cc55e6cd60cd55e6ce60ce55e6cf60cf55e6d060d055e6d160d155e6d260d255e6d360d355e6d460d455e6d560d555e6d660d655e6d760d755e6d860d855e6d960d955e6da60da55e6db60db55e6dc60dc55e6dd60dd55e6de60de55e6df60df55e6e060e055e6e160e155e6e260e255e6e360e355e6e460e455e6e560e555e6e660e655e6e760e755e6e860e855e6e960e955e6ea60ea55e6eb60eb55e6ec60ec55e6ed60ed55e6ee60ee55e6ef60ef55e6f060f055e6f160f155e6f260f255e6f360f355e6f460f455e6f560f555e6f660f655e6f760f755e6f860f855e6f960f955e6fa60fa55e6fb60fb55e6fc60fc55e6fd60fd55e6fe60fe55e6ff60ff5500
