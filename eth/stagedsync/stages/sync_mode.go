package stages

type Mode int8 // in which staged sync can run

const (
	ModeUnknown Mode = iota
	ModeBlockProduction
	ModeForkValidation
	ModeApplyingBlocks
)

func (m Mode) String() string {
	switch m {
	case ModeBlockProduction:
		return "ModeBlockProduction"
	case ModeForkValidation:
		return "ModeForkValidation"
	case ModeApplyingBlocks:
		return "ModeApplyingBlocks"
	default:
		return "UnknownMode"
	}
}

func ModeFromString(s string) Mode { //nolint
	switch s {
	case "ModeBlockProduction":
		return ModeBlockProduction
	case "ModeForkValidation":
		return ModeForkValidation
	case "ModeApplyingBlocks":
		return ModeApplyingBlocks
	case "UnknownMode":
		return ModeUnknown
	default:
		panic("unexpected mode string: " + s)
	}
}
