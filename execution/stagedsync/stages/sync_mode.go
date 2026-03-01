package stages

type Mode int8 // in which staged sync can run

const (
	ModeUnknown Mode = iota
	ModeBlockProduction
	ModeForkValidation
	ModeApplyingBlocks
	ModeApplyingBlocksOffline // offline integration tools (always use high workers)
)

func (m Mode) String() string {
	switch m {
	case ModeBlockProduction:
		return "ModeBlockProduction"
	case ModeForkValidation:
		return "ModeForkValidation"
	case ModeApplyingBlocks:
		return "ModeApplyingBlocks"
	case ModeApplyingBlocksOffline:
		return "ModeApplyingBlocksOffline"
	default:
		return "UnknownMode"
	}
}

func (m Mode) IsApplyingBlocks() bool {
	return m == ModeApplyingBlocks || m == ModeApplyingBlocksOffline
}

func (m Mode) IsOffline() bool {
	return m == ModeApplyingBlocksOffline
}

func ModeFromString(s string) Mode { //nolint
	switch s {
	case "ModeBlockProduction":
		return ModeBlockProduction
	case "ModeForkValidation":
		return ModeForkValidation
	case "ModeApplyingBlocks":
		return ModeApplyingBlocks
	case "ModeApplyingBlocksOffline":
		return ModeApplyingBlocksOffline
	case "UnknownMode":
		return ModeUnknown
	default:
		panic("unexpected mode string: " + s)
	}
}
