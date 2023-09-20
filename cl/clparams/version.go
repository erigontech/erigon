package clparams

type StateVersion uint8

const (
	Phase0Version    StateVersion = 0
	AltairVersion    StateVersion = 1
	BellatrixVersion StateVersion = 2
	CapellaVersion   StateVersion = 3
	DenebVersion     StateVersion = 4
)

// stringToClVersion converts the string to the current state version.
func StringToClVersion(s string) StateVersion {
	switch s {
	case "phase0":
		return Phase0Version
	case "altair":
		return AltairVersion
	case "bellatrix":
		return BellatrixVersion
	case "capella":
		return CapellaVersion
	case "deneb":
		return DenebVersion
	default:
		panic("unsupported fork version: " + s)
	}
}

func ClVersionToString(s StateVersion) string {
	switch s {
	case Phase0Version:
		return "phase0"
	case AltairVersion:
		return "altair"
	case BellatrixVersion:
		return "bellatrix"
	case CapellaVersion:
		return "capella"
	case DenebVersion:
		return "deneb"
	default:
		panic("unsupported fork version")
	}
}
