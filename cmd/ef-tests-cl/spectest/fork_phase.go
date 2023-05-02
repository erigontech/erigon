package spectest

import "github.com/ledgerwatch/erigon/cl/clparams"

var supportedVersions = []string{"phase0", "altair", "bellatrix", "capella"}

// stringToClVersion converts the string to the current state version.
func StringToClVersion(s string) clparams.StateVersion {
	switch s {
	case "phase0":
		return clparams.Phase0Version
	case "altair":
		return clparams.AltairVersion
	case "bellatrix":
		return clparams.BellatrixVersion
	case "capella":
		return clparams.CapellaVersion
	default:
		panic("unsupported fork version: " + s)
	}
}
