package clparams

import "fmt"

type StateVersion uint8

const (
	Phase0Version    StateVersion = 0
	AltairVersion    StateVersion = 1
	BellatrixVersion StateVersion = 2
	CapellaVersion   StateVersion = 3
	DenebVersion     StateVersion = 4
	ElectraVersion   StateVersion = 5
)

// stringToClVersion converts the string to the current state version.
func StringToClVersion(s string) (StateVersion, error) {
	switch s {
	case "phase0":
		return Phase0Version, nil
	case "altair":
		return AltairVersion, nil
	case "bellatrix":
		return BellatrixVersion, nil
	case "capella":
		return CapellaVersion, nil
	case "deneb":
		return DenebVersion, nil
	case "electra":
		return ElectraVersion, nil
	default:
		return 0, fmt.Errorf("unsupported fork version %s", s)
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
	case ElectraVersion:
		return "electra"
	default:
		panic("unsupported fork version")
	}
}
